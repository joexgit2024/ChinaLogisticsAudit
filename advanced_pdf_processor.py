#!/usr/bin/env python3
"""
Advanced PDF Invoice Processor with Machine Learning
===================================================

This processor extracts charge details from DHL invoice PDFs using:
1. Pattern recognition for known charges
2. Machine learning for unknown charge classification
3. Natural language processing for charge descriptions
"""

import os
import re
import json
import sqlite3
import warnings
import pickle
import numpy as np
from typing import Dict, List, Optional, Tuple
import PyPDF2
import pdfplumber
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import PorterStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier

# Suppress scikit-learn version warnings
warnings.filterwarnings('ignore', category=UserWarning, module='sklearn')

# Download required NLTK data
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')


class AdvancedPDFProcessor:
    """
    Advanced PDF processor with machine learning capabilities
    """
    
    def __init__(self, db_path: str = 'dhl_audit.db',
                 upload_folder: str = 'uploads'):
        self.db_path = db_path
        self.upload_folder = upload_folder
        self.stemmer = PorterStemmer()
        self.stop_words = set(stopwords.words('english'))
        self.init_database()
        self.load_or_create_ml_models()
    
    def init_database(self):
        """Initialize database tables for advanced PDF processing"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Enhanced PDF details table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS invoice_pdf_details_enhanced (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_no VARCHAR(50) NOT NULL,
                pdf_file_path VARCHAR(500),
                pdf_filename VARCHAR(255),
                extracted_charges TEXT,  -- JSON string
                charge_descriptions TEXT,  -- JSON with detailed descriptions
                shipment_references TEXT,  -- JSON string
                service_type VARCHAR(100),
                classification_confidence FLOAT,
                total_amount DECIMAL(10,2),
                currency VARCHAR(10),
                extraction_timestamp DATETIME,
                extraction_confidence FLOAT DEFAULT 0.0,
                ml_predictions TEXT,  -- JSON with ML predictions
                manual_corrections TEXT,  -- JSON with human corrections
                UNIQUE(invoice_no)
            )
        ''')
        
        # Charge classification training data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS charge_training_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                charge_description TEXT NOT NULL,
                charge_type VARCHAR(100) NOT NULL,
                charge_category VARCHAR(50),  -- FREIGHT, SERVICE, DUTY_TAX, SURCHARGE
                confidence FLOAT DEFAULT 1.0,
                source VARCHAR(50),  -- MANUAL, AUTO, VERIFIED
                created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Unknown charges for review
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS unknown_charges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_no VARCHAR(50),
                charge_description TEXT,
                extracted_amount DECIMAL(10,2),
                suggested_type VARCHAR(100),
                confidence FLOAT,
                status VARCHAR(20) DEFAULT 'PENDING',  -- PENDING, CLASSIFIED, IGNORED
                created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Charge type definitions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS charge_type_definitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                charge_type VARCHAR(100) UNIQUE,
                category VARCHAR(50),
                description TEXT,
                keywords TEXT,  -- JSON array of keywords
                is_auditable BOOLEAN DEFAULT 1,
                is_passthrough BOOLEAN DEFAULT 0,
                created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        
        # Initialize default charge types
        self.initialize_default_charge_types()
    
    def initialize_default_charge_types(self):
        """Initialize default charge type definitions"""
        default_charges = [
            {
                'charge_type': 'freight_charges',
                'category': 'FREIGHT',
                'description': 'Basic transportation charges',
                'keywords': ['freight', 'transport', 'shipping', 'carriage'],
                'is_auditable': True,
                'is_passthrough': False
            },
            {
                'charge_type': 'fuel_surcharge',
                'category': 'SURCHARGE',
                'description': 'Fuel adjustment charges',
                'keywords': ['fuel', 'BAF', 'bunker', 'surcharge'],
                'is_auditable': False,
                'is_passthrough': True
            },
            {
                'charge_type': 'fumigation',
                'category': 'SERVICE',
                'description': 'Fumigation and treatment services',
                'keywords': ['fumigation', 'treatment', 'pest', 'quarantine'],
                'is_auditable': False,
                'is_passthrough': True
            },
            {
                'charge_type': 'emergency_handling',
                'category': 'SERVICE',
                'description': 'Emergency processing and handling',
                'keywords': ['emergency', 'urgent', 'rush', 'priority'],
                'is_auditable': False,
                'is_passthrough': True
            },
            {
                'charge_type': 'documentation',
                'category': 'SERVICE',
                'description': 'Documentation and paperwork fees',
                'keywords': ['documentation', 'paperwork', 'certificate', 'permit'],
                'is_auditable': False,
                'is_passthrough': True
            },
            {
                'charge_type': 'customs_clearance',
                'category': 'DUTY_TAX',
                'description': 'Customs clearance services',
                'keywords': ['customs', 'clearance', 'broker', 'entry'],
                'is_auditable': False,
                'is_passthrough': True
            },
            {
                'charge_type': 'storage_demurrage',
                'category': 'SERVICE',
                'description': 'Storage and demurrage charges',
                'keywords': ['storage', 'demurrage', 'warehouse', 'detention'],
                'is_auditable': False,
                'is_passthrough': True
            },
            {
                'charge_type': 'inspection_fees',
                'category': 'SERVICE',
                'description': 'Inspection and examination fees',
                'keywords': ['inspection', 'examination', 'survey', 'check'],
                'is_auditable': False,
                'is_passthrough': True
            }
        ]
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        for charge in default_charges:
            cursor.execute('''
                INSERT OR IGNORE INTO charge_type_definitions 
                (charge_type, category, description, keywords, is_auditable, is_passthrough)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                charge['charge_type'],
                charge['category'],
                charge['description'],
                json.dumps(charge['keywords']),
                charge['is_auditable'],
                charge['is_passthrough']
            ))
        
        conn.commit()
        conn.close()
    
    def load_or_create_ml_models(self):
        """Load existing ML models or create new ones"""
        self.models_path = 'ml_models'
        os.makedirs(self.models_path, exist_ok=True)
        
        try:
            # Load vectorizer
            with open(os.path.join(self.models_path, 'tfidf_vectorizer.pkl'), 'rb') as f:
                self.vectorizer = pickle.load(f)
            
            # Load classifier
            with open(os.path.join(self.models_path, 'charge_classifier.pkl'), 'rb') as f:
                self.classifier = pickle.load(f)
                
            print("Loaded existing ML models")
            
        except FileNotFoundError:
            # Create new models
            self.vectorizer = TfidfVectorizer(
                max_features=1000,
                stop_words='english',
                ngram_range=(1, 2)
            )
            self.classifier = RandomForestClassifier(
                n_estimators=100,
                random_state=42
            )
            
            # Train with initial data
            self.train_initial_models()
            print("Created new ML models")
    
    def train_initial_models(self):
        """Train models with initial training data"""
        # Get training data from database
        training_data = self.get_training_data()
        
        if len(training_data) < 10:
            # Create synthetic training data if not enough real data
            training_data.extend(self.generate_synthetic_training_data())
        
        if training_data:
            descriptions = [item['description'] for item in training_data]
            labels = [item['charge_type'] for item in training_data]
            
            # Preprocess descriptions
            processed_descriptions = [self.preprocess_text(desc) for desc in descriptions]
            
            # Train vectorizer and classifier
            X = self.vectorizer.fit_transform(processed_descriptions)
            self.classifier.fit(X, labels)
            
            # Save models
            self.save_models()
            print(f"Trained models with {len(training_data)} samples")
    
    def generate_synthetic_training_data(self) -> List[Dict]:
        """Generate synthetic training data for initial model training"""
        synthetic_data = [
            {'description': 'Freight charges for shipment', 'charge_type': 'freight_charges'},
            {'description': 'Transportation costs', 'charge_type': 'freight_charges'},
            {'description': 'Fuel adjustment surcharge', 'charge_type': 'fuel_surcharge'},
            {'description': 'BAF bunker adjustment factor', 'charge_type': 'fuel_surcharge'},
            {'description': 'Fumigation treatment service', 'charge_type': 'fumigation'},
            {'description': 'Pest control treatment', 'charge_type': 'fumigation'},
            {'description': 'Emergency handling fee', 'charge_type': 'emergency_handling'},
            {'description': 'Urgent processing charge', 'charge_type': 'emergency_handling'},
            {'description': 'Documentation preparation', 'charge_type': 'documentation'},
            {'description': 'Certificate processing fee', 'charge_type': 'documentation'},
            {'description': 'Customs clearance service', 'charge_type': 'customs_clearance'},
            {'description': 'Customs broker fee', 'charge_type': 'customs_clearance'},
            {'description': 'Storage charges', 'charge_type': 'storage_demurrage'},
            {'description': 'Warehouse handling', 'charge_type': 'storage_demurrage'},
            {'description': 'Inspection service fee', 'charge_type': 'inspection_fees'},
            {'description': 'Examination charges', 'charge_type': 'inspection_fees'},
        ]
        return synthetic_data
    
    def get_training_data(self) -> List[Dict]:
        """Get training data from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT charge_description, charge_type 
            FROM charge_training_data 
            WHERE confidence >= 0.8
        ''')
        
        rows = cursor.fetchall()
        conn.close()
        
        return [{'description': row[0], 'charge_type': row[1]} for row in rows]
    
    def preprocess_text(self, text: str) -> str:
        """Preprocess text for ML training"""
        # Convert to lowercase
        text = text.lower()
        
        # Remove special characters
        text = re.sub(r'[^a-zA-Z\s]', '', text)
        
        # Tokenize
        tokens = word_tokenize(text)
        
        # Remove stopwords and stem
        tokens = [self.stemmer.stem(token) for token in tokens if token not in self.stop_words]
        
        return ' '.join(tokens)
    
    def save_models(self):
        """Save trained ML models"""
        with open(os.path.join(self.models_path, 'tfidf_vectorizer.pkl'), 'wb') as f:
            pickle.dump(self.vectorizer, f)
        
        with open(os.path.join(self.models_path, 'charge_classifier.pkl'), 'wb') as f:
            pickle.dump(self.classifier, f)
    
    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """Extract text from PDF using multiple methods"""
        text_content = ""
        
        try:
            # Try pdfplumber first (better for tables)
            with pdfplumber.open(pdf_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_content += page_text + "\n"
        except:
            # Fallback to PyPDF2
            try:
                with open(pdf_path, 'rb') as file:
                    pdf_reader = PyPDF2.PdfReader(file)
                    for page in pdf_reader.pages:
                        text_content += page.extract_text() + "\n"
            except Exception as e:
                print(f"Error extracting text from PDF: {e}")
                return ""
        
        return text_content
    
    def extract_charges_with_descriptions(self, text: str) -> List[Dict]:
        """Extract charges with detailed descriptions using advanced patterns"""
        charges = []
        
        # Enhanced patterns for charge extraction
        charge_patterns = [
            # Pattern 1: Description followed by amount
            r'([A-Z][^$\n]*?)[\s\-:]+\$?([\d,]+\.?\d*)',
            # Pattern 2: Amount followed by description
            r'\$?([\d,]+\.?\d*)[\s\-:]+([A-Z][^$\n]*?)(?=\n|\$|$)',
            # Pattern 3: Line item format
            r'(\d+)\.\s+([^$\n]+?)[\s\-:]+\$?([\d,]+\.?\d*)',
            # Pattern 4: Table format
            r'([A-Z][^|$\n]*?)\s*\|\s*\$?([\d,]+\.?\d*)',
        ]
        
        for pattern in charge_patterns:
            matches = re.findall(pattern, text, re.MULTILINE | re.IGNORECASE)
            for match in matches:
                if len(match) == 2:
                    description, amount_str = match
                elif len(match) == 3:
                    # Handle numbered items
                    if match[0].isdigit():
                        description, amount_str = match[1], match[2]
                    else:
                        description, amount_str = match[0], match[1]
                else:
                    continue
                
                # Clean and validate
                description = description.strip()
                amount_str = amount_str.replace(',', '')
                
                try:
                    amount = float(amount_str)
                    if amount > 0 and len(description) > 3:
                        charges.append({
                            'description': description,
                            'amount': amount,
                            'raw_text': f"{description} ${amount_str}"
                        })
                except ValueError:
                    continue
        
        # Remove duplicates and sort by amount
        unique_charges = []
        seen_descriptions = set()
        
        for charge in sorted(charges, key=lambda x: x['amount'], reverse=True):
            desc_key = charge['description'].lower().strip()
            if desc_key not in seen_descriptions and len(desc_key) > 5:
                seen_descriptions.add(desc_key)
                unique_charges.append(charge)
        
        return unique_charges[:20]  # Limit to top 20 charges
    
    def classify_charge_with_ml(self, description: str) -> Tuple[str, float]:
        """Classify charge using ML model"""
        try:
            # Preprocess description
            processed_desc = self.preprocess_text(description)
            
            # Vectorize
            X = self.vectorizer.transform([processed_desc])
            
            # Predict
            prediction = self.classifier.predict(X)[0]
            probabilities = self.classifier.predict_proba(X)[0]
            confidence = max(probabilities)
            
            return prediction, confidence
            
        except Exception as e:
            print(f"ML classification error: {e}")
            return 'unknown', 0.0
    
    def classify_charge_with_rules(self, description: str) -> Tuple[str, float]:
        """Classify charge using rule-based approach"""
        description_lower = description.lower()
        
        # Get charge type definitions
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT charge_type, keywords FROM charge_type_definitions')
        definitions = cursor.fetchall()
        conn.close()
        
        best_match = None
        best_score = 0
        
        for charge_type, keywords_json in definitions:
            keywords = json.loads(keywords_json)
            score = 0
            
            for keyword in keywords:
                if keyword.lower() in description_lower:
                    score += 1
            
            if score > best_score:
                best_score = score
                best_match = charge_type
        
        confidence = min(best_score / 3.0, 1.0)  # Normalize score
        
        return best_match or 'unknown', confidence
    
    def classify_charge(self, description: str) -> Dict:
        """Classify charge using both ML and rules"""
        # Try ML classification
        ml_type, ml_confidence = self.classify_charge_with_ml(description)
        
        # Try rule-based classification
        rule_type, rule_confidence = self.classify_charge_with_rules(description)
        
        # Combine results
        if ml_confidence > 0.7:
            final_type = ml_type
            final_confidence = ml_confidence
            method = 'ML'
        elif rule_confidence > 0.5:
            final_type = rule_type
            final_confidence = rule_confidence
            method = 'RULES'
        elif ml_confidence > rule_confidence:
            final_type = ml_type
            final_confidence = ml_confidence
            method = 'ML_LOW'
        else:
            final_type = rule_type if rule_type != 'unknown' else ml_type
            final_confidence = max(ml_confidence, rule_confidence)
            method = 'COMBINED'
        
        return {
            'charge_type': final_type,
            'confidence': final_confidence,
            'method': method,
            'ml_prediction': ml_type,
            'ml_confidence': ml_confidence,
            'rule_prediction': rule_type,
            'rule_confidence': rule_confidence
        }

    def extract_shipment_references(self, text: str) -> Dict:
        """Extract shipment references"""
        references = {}
        
        patterns = {
            'master_bill': [r'Master\s*Bill\s*[:\-]?\s*(\w+)', r'MAWB\s*[:\-]?\s*(\w+)'],
            'house_bill': [r'House\s*Bill\s*[:\-]?\s*(\w+)', r'HAWB\s*[:\-]?\s*(\w+)'],
            'shipment_date': [r'Shipment\s*Date\s*[:\-]?\s*(\d{4}-\d{2}-\d{2})', r'Ship\s*Date\s*[:\-]?\s*(\d{2}/\d{2}/\d{4})'],
            'origin': [r'Origin\s*[:\-]?\s*([A-Z]{3})', r'From\s*[:\-]?\s*([A-Z]{3})'],
            'destination': [r'Destination\s*[:\-]?\s*([A-Z]{3})', r'To\s*[:\-]?\s*([A-Z]{3})']
        }
        
        for ref_type, ref_patterns in patterns.items():
            for pattern in ref_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    references[ref_type] = match.group(1)
                    break
        
        return references

    def extract_total_amount(self, text: str) -> Tuple[float, str]:
        """Extract total amount and currency"""
        total_patterns = [
            r'Total\s*[:\-]?\s*([A-Z]{3})?\s*\$?([\d,]+\.?\d*)',
            r'Amount\s*Due\s*[:\-]?\s*([A-Z]{3})?\s*\$?([\d,]+\.?\d*)',
            r'Invoice\s*Total\s*[:\-]?\s*([A-Z]{3})?\s*\$?([\d,]+\.?\d*)'
        ]
        
        for pattern in total_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                currency = match.group(1) if match.group(1) else 'USD'
                amount_str = match.group(2).replace(',', '')
                try:
                    amount = float(amount_str)
                    return amount, currency
                except ValueError:
                    continue
        
        return 0.0, 'USD'

    def parse_invoice_number(self, text: str) -> Optional[str]:
        """Extract invoice number from PDF text"""
        patterns = [
            r'Invoice\s*(?:No\.?|Number)?\s*:?\s*([A-Z]\d{7,})',
            r'Invoice\s*([A-Z]\d{7,})',
            r'Bill\s*(?:No\.?|Number)?\s*:?\s*([A-Z]\d{7,})',
            r'Document\s*(?:No\.?|Number)?\s*:?\s*([A-Z]\d{7,})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None

    def get_pdf_details(self, invoice_no: str) -> Optional[Dict]:
        """Get enhanced PDF details for an invoice"""
        def safe_json_loads(json_str):
            """Safely parse JSON string with Unicode handling"""
            if not json_str:
                return {}
            try:
                return json.loads(json_str)
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                print(f"JSON parsing error: {e}")
                return {}
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM invoice_pdf_details_enhanced WHERE invoice_no = ?
        ''', (invoice_no,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'id': row[0],
                'invoice_no': row[1],
                'pdf_file_path': row[2],
                'pdf_filename': row[3],
                'extracted_charges': safe_json_loads(row[4]),
                'charge_descriptions': safe_json_loads(row[5]),
                'shipment_references': safe_json_loads(row[6]),
                'service_type': row[7],
                'classification_confidence': row[8],
                'total_amount': row[9],
                'currency': row[10],
                'extraction_timestamp': row[11],
                'extraction_confidence': row[12],
                'ml_predictions': safe_json_loads(row[13]),
                'manual_corrections': safe_json_loads(row[14]),
                'comprehensive_data': safe_json_loads(row[17]) if len(row) > 17 and row[17] else {}
            }
        
        return None

    def process_uploads_folder(self) -> List[Dict]:
        """Process all PDFs in uploads folder"""
        results = []
        
        if not os.path.exists(self.upload_folder):
            return results
        
        for filename in os.listdir(self.upload_folder):
            if filename.lower().endswith('.pdf'):
                pdf_path = os.path.join(self.upload_folder, filename)
                
                # Try to extract invoice number from filename
                invoice_match = re.search(r'([A-Z]\d{7,})', filename)
                invoice_no = invoice_match.group(1) if invoice_match else None
                
                result = self.process_pdf_advanced(pdf_path, invoice_no)
                result['filename'] = filename
                results.append(result)
        
        return results

    def process_pdf_advanced(self, pdf_path: str, invoice_no: str = None) -> Dict:
        """Process PDF with advanced charge extraction and classification"""
        try:
            # Extract text
            text = self.extract_text_from_pdf(pdf_path)
            if not text:
                return {'error': 'Could not extract text from PDF'}
            
            # Parse invoice number if not provided
            if not invoice_no:
                invoice_no = self.parse_invoice_number(text)
                if not invoice_no:
                    return {'error': 'Could not identify invoice number in PDF'}
            
            # Extract charges with descriptions
            raw_charges = self.extract_charges_with_descriptions(text)
            
            # Classify each charge
            classified_charges = {}
            charge_descriptions = {}
            ml_predictions = {}
            unknown_charges = []
            
            for charge in raw_charges:
                description = charge['description']
                amount = charge['amount']
                
                # Classify the charge
                classification = self.classify_charge(description)
                
                charge_type = classification['charge_type']
                confidence = classification['confidence']
                
                # Store charge
                if charge_type in classified_charges:
                    # If duplicate type, combine amounts
                    classified_charges[charge_type] += amount
                    charge_descriptions[charge_type].append(description)
                else:
                    classified_charges[charge_type] = amount
                    charge_descriptions[charge_type] = [description]
                
                # Store ML prediction details
                ml_predictions[description] = classification
                
                # Track unknown charges for review
                if confidence < 0.5 or charge_type == 'unknown':
                    unknown_charges.append({
                        'description': description,
                        'amount': amount,
                        'suggested_type': charge_type,
                        'confidence': confidence
                    })
            
            # Determine service type
            service_type, type_confidence = self.determine_service_type(classified_charges)
            
            # Extract other details
            references = self.extract_shipment_references(text)
            total_amount, currency = self.extract_total_amount(text)
            
            # Calculate overall confidence
            overall_confidence = self.calculate_overall_confidence(
                classified_charges, charge_descriptions, total_amount, type_confidence
            )
            
            return {
                'success': True,
                'invoice_no': invoice_no,
                'charges': classified_charges,
                'charge_descriptions': charge_descriptions,
                'references': references,
                'service_type': service_type,
                'classification_confidence': type_confidence,
                'total_amount': total_amount,
                'currency': currency,
                'confidence': overall_confidence,
                'unknown_charges': unknown_charges,
                'ml_predictions': ml_predictions
            }
            
        except Exception as e:
            return {'error': f'Error processing PDF: {str(e)}'}

    def determine_service_type(self, charges: Dict) -> Tuple[str, float]:
        """Determine if invoice is FREIGHT, SERVICE, or MIXED"""
        freight_charges = ['freight_charges', 'transportation']
        service_charges = ['fumigation', 'emergency_handling', 'documentation', 
                          'inspection_fees', 'storage_demurrage']
        
        freight_amount = sum(charges.get(charge, 0) for charge in freight_charges)
        service_amount = sum(charges.get(charge, 0) for charge in service_charges)
        total_amount = sum(charges.values())
        
        if total_amount == 0:
            return 'UNKNOWN', 0.0
        
        freight_ratio = freight_amount / total_amount
        service_ratio = service_amount / total_amount
        
        if freight_ratio > 0.7:
            return 'FREIGHT', freight_ratio
        elif service_ratio > 0.7:
            return 'SERVICE', service_ratio
        elif freight_ratio > service_ratio:
            return 'MIXED_FREIGHT', max(freight_ratio, service_ratio)
        else:
            return 'MIXED_SERVICE', max(freight_ratio, service_ratio)

    def calculate_overall_confidence(self, charges: Dict, descriptions: Dict, 
                                   total_amount: float, type_confidence: float) -> float:
        """Calculate overall extraction confidence"""
        score = 0.0
        
        # Points for extracted charges
        if charges:
            score += min(len(charges) * 0.1, 0.4)
        
        # Points for charge descriptions
        if descriptions:
            avg_desc_length = np.mean([len(' '.join(descs)) for descs in descriptions.values()])
            score += min(avg_desc_length / 100, 0.2)
        
        # Points for total amount
        if total_amount > 0:
            score += 0.2
        
        # Points for classification confidence
        score += type_confidence * 0.2
        
        return min(score, 1.0)
