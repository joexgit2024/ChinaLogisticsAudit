import os
import re
import json
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import PyPDF2
import pdfplumber
from werkzeug.utils import secure_filename

class PDFInvoiceProcessor:
    """
    PDF Invoice Processor for extracting charge details from DHL invoice PDFs
    """
    
    def __init__(self, db_path: str = 'dhl_audit.db', upload_folder: str = 'uploads'):
        self.db_path = db_path
        self.upload_folder = upload_folder
        self.init_database()
    
    def init_database(self):
        """Initialize database tables for PDF processing"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create PDF details table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS invoice_pdf_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_no VARCHAR(50) NOT NULL,
                pdf_file_path VARCHAR(500),
                pdf_filename VARCHAR(255),
                extracted_charges TEXT,  -- JSON string
                shipment_references TEXT,  -- JSON string
                service_type VARCHAR(100),
                total_amount DECIMAL(10,2),
                currency VARCHAR(10),
                extraction_timestamp DATETIME,
                extraction_confidence FLOAT DEFAULT 0.0,
                UNIQUE(invoice_no)
            )
        ''')
        
        # Create shipment groups table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS shipment_groups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                master_bill VARCHAR(50),
                house_bill VARCHAR(50),
                shipment_date DATE,
                related_invoices TEXT,  -- JSON array of invoice numbers
                total_invoices INTEGER DEFAULT 1,
                combined_variance DECIMAL(10,2) DEFAULT 0.0
            )
        ''')
        
        conn.commit()
        conn.close()
    
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
    
    def parse_charges(self, text: str) -> Dict:
        """Parse charge breakdown from PDF text with enhanced patterns"""
        charges = {}
        charge_descriptions = {}
        
        # Enhanced charge patterns with more comprehensive coverage
        charge_patterns = {
            'freight': [
                r'freight\s*charge[s]?\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'transportation\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'shipping\s*charge[s]?\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'carriage\s*[:\-]?\s*\$?(\d+\.?\d*)'
            ],
            'fuel_surcharge': [
                r'fuel\s*surcharge\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'BAF\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'bunker\s*adjustment\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'fuel\s*adjustment\s*[:\-]?\s*\$?(\d+\.?\d*)'
            ],
            'origin_handling': [
                r'origin\s*handling\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'pickup\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'collection\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'export\s*handling\s*[:\-]?\s*\$?(\d+\.?\d*)'
            ],
            'destination_handling': [
                r'destination\s*handling\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'delivery\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'import\s*handling\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'terminal\s*handling\s*[:\-]?\s*\$?(\d+\.?\d*)'
            ],
            'customs': [
                r'customs\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'clearance\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'brokerage\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'entry\s*fee\s*[:\-]?\s*\$?(\d+\.?\d*)'
            ],
            'duty_tax': [
                r'duty\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'tax\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'GST\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'VAT\s*[:\-]?\s*\$?(\d+\.?\d*)'
            ],
            'fumigation': [
                r'fumigation\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'treatment\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'quarantine\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'phytosanitary\s*[:\-]?\s*\$?(\d+\.?\d*)'
            ],
            'emergency': [
                r'emergency\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'urgent\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'express\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'rush\s*[:\-]?\s*\$?(\d+\.?\d*)'
            ],
            'documentation': [
                r'documentation\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'paperwork\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'admin\s*fee\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'processing\s*fee\s*[:\-]?\s*\$?(\d+\.?\d*)'
            ],
            'security': [
                r'security\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'screening\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'inspection\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'x[\-\s]?ray\s*[:\-]?\s*\$?(\d+\.?\d*)'
            ],
            'storage': [
                r'storage\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'warehouse\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'demurrage\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'detention\s*[:\-]?\s*\$?(\d+\.?\d*)'
            ],
            'insurance': [
                r'insurance\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'cargo\s*insurance\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'coverage\s*[:\-]?\s*\$?(\d+\.?\d*)'
            ],
            'additional_services': [
                r'special\s*handling\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'dangerous\s*goods\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'oversized\s*[:\-]?\s*\$?(\d+\.?\d*)',
                r'refrigeration\s*[:\-]?\s*\$?(\d+\.?\d*)'
            ]
        }
        
        # Extract detailed descriptions for variance explanation
        for charge_type, patterns in charge_patterns.items():
            for pattern in patterns:
                # Look for pattern with surrounding context for description
                context_pattern = rf'(.{{0,50}}){pattern}(.{{0,50}})'
                matches = re.finditer(context_pattern, text, re.IGNORECASE)
                
                for match in matches:
                    amount = float(match.group(2))  # The captured amount
                    
                    # Store the charge amount
                    if charge_type not in charges or amount > charges[charge_type]:
                        charges[charge_type] = amount
                        
                        # Extract description context
                        before_context = match.group(1).strip()
                        after_context = match.group(3).strip()
                        full_description = f"{before_context} {match.group(0)} {after_context}".strip()
                        charge_descriptions[charge_type] = full_description
                    
                    break
        
        # Store descriptions in charges for variance explanation
        if charge_descriptions:
            charges['_descriptions'] = charge_descriptions
        
        return charges
    
    def extract_shipment_references(self, text: str) -> Dict:
        """Extract shipment references (master bill, house bill, etc.)"""
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
    
    def identify_service_type(self, charges: Dict, text: str) -> str:
        """Identify if this is a freight invoice or service invoice"""
        
        # Service-only indicators
        service_indicators = ['fumigation', 'emergency', 'documentation', 'customs']
        freight_indicators = ['freight', 'transportation']
        
        has_service_charges = any(charge in charges for charge in service_indicators)
        has_freight_charges = any(charge in charges for charge in freight_indicators)
        
        # Check text for service keywords
        service_keywords = ['service', 'handling', 'processing', 'clearance', 'treatment']
        freight_keywords = ['transport', 'shipping', 'freight', 'delivery']
        
        text_lower = text.lower()
        service_count = sum(1 for keyword in service_keywords if keyword in text_lower)
        freight_count = sum(1 for keyword in freight_keywords if keyword in text_lower)
        
        if has_service_charges and not has_freight_charges:
            return 'SERVICE'
        elif has_freight_charges and not has_service_charges:
            return 'FREIGHT'
        elif service_count > freight_count:
            return 'SERVICE'
        elif freight_count > service_count:
            return 'FREIGHT'
        else:
            return 'MIXED'
    
    def extract_total_amount(self, text: str) -> Tuple[float, str]:
        """Extract total amount and currency"""
        # Pattern for total amount
        total_patterns = [
            r'Total\s*[:\-]?\s*([A-Z]{3})?\s*\$?(\d+\.?\d*)',
            r'Amount\s*Due\s*[:\-]?\s*([A-Z]{3})?\s*\$?(\d+\.?\d*)',
            r'Invoice\s*Total\s*[:\-]?\s*([A-Z]{3})?\s*\$?(\d+\.?\d*)'
        ]
        
        for pattern in total_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                currency = match.group(1) if match.group(1) else 'USD'
                amount = float(match.group(2))
                return amount, currency
        
        return 0.0, 'USD'
    
    def process_pdf(self, pdf_path: str, invoice_no: str = None) -> Dict:
        """Process a PDF invoice and extract all details"""
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
            
            # Extract charge details
            charges = self.parse_charges(text)
            references = self.extract_shipment_references(text)
            service_type = self.identify_service_type(charges, text)
            total_amount, currency = self.extract_total_amount(text)
            
            # Calculate confidence score
            confidence = self.calculate_confidence(charges, references, total_amount)
            
            # Store in database
            self.store_pdf_details(
                invoice_no=invoice_no,
                pdf_path=pdf_path,
                charges=charges,
                references=references,
                service_type=service_type,
                total_amount=total_amount,
                currency=currency,
                confidence=confidence
            )
            
            return {
                'success': True,
                'invoice_no': invoice_no,
                'charges': charges,
                'references': references,
                'service_type': service_type,
                'total_amount': total_amount,
                'currency': currency,
                'confidence': confidence
            }
            
        except Exception as e:
            return {'error': f'Error processing PDF: {str(e)}'}
    
    def calculate_confidence(self, charges: Dict, references: Dict, total_amount: float) -> float:
        """Calculate extraction confidence score"""
        score = 0.0
        
        # Points for extracted charges
        if charges:
            score += min(len(charges) * 0.1, 0.4)
        
        # Points for references
        if references:
            score += min(len(references) * 0.1, 0.3)
        
        # Points for total amount
        if total_amount > 0:
            score += 0.3
        
        return min(score, 1.0)
    
    def store_pdf_details(self, invoice_no: str, pdf_path: str, charges: Dict, 
                         references: Dict, service_type: str, total_amount: float, 
                         currency: str, confidence: float):
        """Store PDF extraction results in database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        filename = os.path.basename(pdf_path)
        
        cursor.execute('''
            INSERT OR REPLACE INTO invoice_pdf_details 
            (invoice_no, pdf_file_path, pdf_filename, extracted_charges, 
             shipment_references, service_type, total_amount, currency, 
             extraction_timestamp, extraction_confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            invoice_no, pdf_path, filename, json.dumps(charges),
            json.dumps(references), service_type, total_amount, currency,
            datetime.now(), confidence
        ))
        
        conn.commit()
        conn.close()
    
    def get_pdf_details(self, invoice_no: str) -> Optional[Dict]:
        """Get PDF details for an invoice"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM invoice_pdf_details WHERE invoice_no = ?
        ''', (invoice_no,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'id': row[0],
                'invoice_no': row[1],
                'pdf_file_path': row[2],
                'pdf_filename': row[3],
                'extracted_charges': json.loads(row[4]) if row[4] else {},
                'shipment_references': json.loads(row[5]) if row[5] else {},
                'service_type': row[6],
                'total_amount': row[7],
                'currency': row[8],
                'extraction_timestamp': row[9],
                'extraction_confidence': row[10]
            }
        
        return None
    
    def process_uploads_folder(self) -> List[Dict]:
        """Process all PDFs in the uploads folder"""
        results = []
        
        if not os.path.exists(self.upload_folder):
            return results
        
        for filename in os.listdir(self.upload_folder):
            if filename.lower().endswith('.pdf'):
                pdf_path = os.path.join(self.upload_folder, filename)
                
                # Try to extract invoice number from filename
                invoice_match = re.search(r'([A-Z]\d{7,})', filename)
                invoice_no = invoice_match.group(1) if invoice_match else None
                
                result = self.process_pdf(pdf_path, invoice_no)
                result['filename'] = filename
                results.append(result)
        
        return results