#!/usr/bin/env python3
"""
LLM-Enhanced PDF Invoice Processor using Ollama with DeepSeek-R1
"""

import os
import json
import sqlite3
import requests
from typing import Dict, List, Optional
import pdfplumber
from datetime import datetime


class LLMEnhancedPDFProcessor:
    """
    PDF processor enhanced with local LLM capabilities via Ollama (DeepSeek-R1)
    """
    
    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path
        # DeepSeek-R1 Configuration
        self.ollama_url = "http://localhost:11434"
        self.model_name = "deepseek-r1:latest"
        self.timeout = 120
        self.temperature = 0.1
        self.top_p = 0.9
        self.num_predict = 4096
        self.init_database()

    def init_database(self):
        """Initialize database tables for LLM-enhanced processing"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Enhanced LLM processing table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS llm_pdf_extractions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_no VARCHAR(50) NOT NULL,
                pdf_file_path VARCHAR(500),
                pdf_filename VARCHAR(255),
                extracted_data TEXT,
                raw_pdf_text TEXT,
                processing_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                llm_model_used VARCHAR(100),
                extraction_confidence FLOAT,
                manual_review_needed BOOLEAN DEFAULT 0,
                UNIQUE(invoice_no)
            )
        ''')
        
        # Detailed billing line items table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS llm_billing_line_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_no VARCHAR(50) NOT NULL,
                line_item_index INTEGER,
                description TEXT,
                amount DECIMAL(15,2),
                gst_amount DECIMAL(15,2),
                total_amount DECIMAL(15,2),
                currency VARCHAR(10),
                category VARCHAR(50),
                extraction_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (invoice_no) REFERENCES llm_pdf_extractions(invoice_no)
            )
        ''')
        
        # Invoice summary table for LLM extractions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS llm_invoice_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_no VARCHAR(50) NOT NULL,
                invoice_date DATE,
                due_date DATE,
                customer_name VARCHAR(255),
                currency VARCHAR(10),
                subtotal DECIMAL(15,2),
                gst_total DECIMAL(15,2),
                final_total DECIMAL(15,2),
                service_type VARCHAR(100),
                origin VARCHAR(100),
                destination VARCHAR(100),
                weight VARCHAR(50),
                shipment_ref VARCHAR(100),
                extraction_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(invoice_no),
                FOREIGN KEY (invoice_no) REFERENCES llm_pdf_extractions(invoice_no)
            )
        ''')
        
        conn.commit()
        conn.close()

    def check_ollama_connection(self) -> bool:
        """Check if Ollama is running and DeepSeek-R1 model is available"""
        try:
            # Check if Ollama is running
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=5)
            if response.status_code == 200:
                models = response.json().get('models', [])
                model_names = [model['name'] for model in models]
                return self.model_name in model_names
            return False
        except requests.RequestException:
            return False

    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """Extract text from PDF using pdfplumber"""
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text_content = ""
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_content += page_text + "\n"
                return text_content.strip()
        except Exception as e:
            print(f"Error extracting text from PDF: {e}")
            return ""

    def create_llm_prompt(self, pdf_text: str) -> str:
        """Create a detailed prompt for LLM invoice processing"""
        prompt = f"""You are an expert invoice processing assistant. Analyze the following DHL Express invoice and extract all billing information with high accuracy.

INVOICE TEXT:
{pdf_text}

Please extract and return a JSON response with the following structure:
{{
    "invoice_details": {{
        "invoice_number": "string",
        "invoice_date": "YYYY-MM-DD",
        "due_date": "YYYY-MM-DD",
        "customer_name": "string",
        "customer_address": "string",
        "service_type": "string (Express, Air Freight, etc.)",
        "origin": "string",
        "destination": "string",
        "shipment_reference": "string",
        "weight": "string",
        "currency": "string"
    }},
    "billing_breakdown": {{
        "subtotal": 0.00,
        "gst_amount": 0.00,
        "total_amount": 0.00,
        "line_items": [
            {{
                "description": "string",
                "amount": 0.00,
                "gst": 0.00,
                "total": 0.00,
                "category": "freight/surcharge/fuel/other"
            }}
        ]
    }},
    "confidence_score": 0.95,
    "extraction_notes": "Any important observations or potential issues"
}}

Focus on:
1. Accurate number extraction (amounts, dates, references)
2. Complete line item breakdown
3. Proper categorization of charges
4. GST/tax calculations
5. Service type identification

Return only the JSON response, no additional text."""
        return prompt

    def query_ollama_llm(self, prompt: str) -> Optional[Dict]:
        """Query DeepSeek-R1 via Ollama API"""
        try:
            payload = {
                "model": self.model_name,
                "prompt": prompt,
                "stream": False,
                "options": {
                    "temperature": self.temperature,
                    "top_p": self.top_p,
                    "num_predict": self.num_predict
                }
            }
            
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json=payload,
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                result = response.json()
                return result.get('response', '')
            else:
                print(f"Ollama API error: {response.status_code} - {response.text}")
                return None
                
        except requests.RequestException as e:
            print(f"Error querying Ollama: {e}")
            return None

    def parse_llm_response(self, llm_response: str) -> Optional[Dict]:
        """Parse and validate LLM JSON response"""
        try:
            # Find JSON in response (in case there's extra text)
            start_idx = llm_response.find('{')
            end_idx = llm_response.rfind('}') + 1
            
            if start_idx == -1 or end_idx == 0:
                print("No JSON found in LLM response")
                return None
                
            json_str = llm_response[start_idx:end_idx]
            parsed_data = json.loads(json_str)
            
            # Validate required fields
            required_fields = ['invoice_details', 'billing_breakdown']
            for field in required_fields:
                if field not in parsed_data:
                    print(f"Missing required field: {field}")
                    return None
            
            return parsed_data
            
        except json.JSONDecodeError as e:
            print(f"Error parsing LLM JSON response: {e}")
            print(f"Response content: {llm_response[:500]}...")
            return None

    def save_extraction_to_db(self, invoice_data: Dict, pdf_path: str, raw_text: str) -> bool:
        """Save LLM extraction results to database"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            invoice_details = invoice_data.get('invoice_details', {})
            billing_breakdown = invoice_data.get('billing_breakdown', {})
            
            invoice_no = invoice_details.get('invoice_number', 'UNKNOWN')
            
            # Insert main extraction record
            cursor.execute('''
                INSERT OR REPLACE INTO llm_pdf_extractions 
                (invoice_no, pdf_file_path, pdf_filename, extracted_data, raw_pdf_text, 
                 llm_model_used, extraction_confidence)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                invoice_no,
                pdf_path,
                os.path.basename(pdf_path),
                json.dumps(invoice_data),
                raw_text,
                self.model_name,
                invoice_data.get('confidence_score', 0.8)
            ))
            
            # Insert invoice summary
            cursor.execute('''
                INSERT OR REPLACE INTO llm_invoice_summary 
                (invoice_no, invoice_date, due_date, customer_name, currency, 
                 subtotal, gst_total, final_total, service_type, origin, destination, 
                 weight, shipment_ref)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                invoice_no,
                invoice_details.get('invoice_date'),
                invoice_details.get('due_date'),
                invoice_details.get('customer_name'),
                invoice_details.get('currency'),
                billing_breakdown.get('subtotal', 0),
                billing_breakdown.get('gst_amount', 0),
                billing_breakdown.get('total_amount', 0),
                invoice_details.get('service_type'),
                invoice_details.get('origin'),
                invoice_details.get('destination'),
                invoice_details.get('weight'),
                invoice_details.get('shipment_reference')
            ))
            
            # Insert line items
            cursor.execute('DELETE FROM llm_billing_line_items WHERE invoice_no = ?', (invoice_no,))
            
            line_items = billing_breakdown.get('line_items', [])
            for idx, item in enumerate(line_items):
                cursor.execute('''
                    INSERT INTO llm_billing_line_items 
                    (invoice_no, line_item_index, description, amount, gst_amount, 
                     total_amount, currency, category)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    invoice_no,
                    idx,
                    item.get('description'),
                    item.get('amount', 0),
                    item.get('gst', 0),
                    item.get('total', 0),
                    invoice_details.get('currency'),
                    item.get('category')
                ))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            print(f"Error saving to database: {e}")
            return False

    def extract_invoice_data_with_llm(self, pdf_path: str) -> Dict:
        """Main method to extract invoice data using LLM"""
        result = {
            'success': False,
            'error': None,
            'invoice_data': None,
            'processing_time': 0,
            'model_used': self.model_name
        }
        
        start_time = datetime.now()
        
        try:
            # Check Ollama connection
            if not self.check_ollama_connection():
                result['error'] = f"Ollama not running or {self.model_name} not available"
                return result
            
            # Extract text from PDF
            pdf_text = self.extract_text_from_pdf(pdf_path)
            if not pdf_text:
                result['error'] = "Could not extract text from PDF"
                return result
            
            # Create LLM prompt
            prompt = self.create_llm_prompt(pdf_text)
            
            # Query LLM
            llm_response = self.query_ollama_llm(prompt)
            if not llm_response:
                result['error'] = "Failed to get response from LLM"
                return result
            
            # Parse LLM response
            invoice_data = self.parse_llm_response(llm_response)
            if not invoice_data:
                result['error'] = "Failed to parse LLM response"
                return result
            
            # Save to database
            if self.save_extraction_to_db(invoice_data, pdf_path, pdf_text):
                result['success'] = True
                result['invoice_data'] = invoice_data
            else:
                result['error'] = "Failed to save to database"
            
        except Exception as e:
            result['error'] = f"Processing error: {str(e)}"
        
        finally:
            end_time = datetime.now()
            result['processing_time'] = (end_time - start_time).total_seconds()
        
        return result

    def process_pdf_with_llm(self, pdf_path: str) -> Dict:
        """Process a PDF file with LLM enhancement"""
        if not os.path.exists(pdf_path):
            return {
                'success': False,
                'error': f"PDF file not found: {pdf_path}"
            }
        
        return self.extract_invoice_data_with_llm(pdf_path)

    def get_extraction_history(self, limit: int = 50) -> List[Dict]:
        """Get recent LLM extraction history"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT e.invoice_no, e.pdf_filename, e.extraction_confidence, 
                       e.processing_timestamp, s.customer_name, s.final_total, s.currency
                FROM llm_pdf_extractions e
                LEFT JOIN llm_invoice_summary s ON e.invoice_no = s.invoice_no
                ORDER BY e.processing_timestamp DESC
                LIMIT ?
            ''', (limit,))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'invoice_no': row[0],
                    'pdf_filename': row[1],
                    'confidence': row[2],
                    'timestamp': row[3],
                    'customer_name': row[4],
                    'total_amount': row[5],
                    'currency': row[6]
                })
            
            conn.close()
            return results
            
        except Exception as e:
            print(f"Error getting extraction history: {e}")
            return []

    def get_extraction_details(self, invoice_no: str) -> Optional[Dict]:
        """Get detailed extraction data for a specific invoice"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT extracted_data, raw_pdf_text, extraction_confidence
                FROM llm_pdf_extractions 
                WHERE invoice_no = ?
            ''', (invoice_no,))
            
            row = cursor.fetchone()
            if row:
                extracted_data = json.loads(row[0]) if row[0] else {}
                return {
                    'extracted_data': extracted_data,
                    'raw_text': row[1],
                    'confidence': row[2]
                }
            
            conn.close()
            return None
            
        except Exception as e:
            print(f"Error getting extraction details: {e}")
            return None


if __name__ == "__main__":
    processor = LLMEnhancedPDFProcessor()
    
    # Test with a PDF file
    pdf_path = "uploads/sample_invoice.pdf"
    if os.path.exists(pdf_path):
        result = processor.process_pdf_with_llm(pdf_path)
        print(json.dumps(result, indent=2))
    else:
        print(f"PDF file not found: {pdf_path}")
        print(f"Current model: {processor.model_name}")
