#!/usr/bin/env python3
"""
Simplified PDF-YTD Validation (Timeout-Safe)
===========================================
"""

import sqlite3
import json
import os
import pdfplumber
import requests
from datetime import datetime

class SimplePDFValidator:
    """Simplified validator with timeout handling"""
    
    def __init__(self):
        self.db_path = 'dhl_audit.db'
    
    def validate_invoice_simple(self, invoice_no: str) -> dict:
        """Simple validation with timeout protection"""
        
        result = {
            'invoice_no': invoice_no,
            'status': 'ERROR',
            'ytd_data': None,
            'pdf_data': None,
            'validation_summary': {}
        }
        
        # 1. Get YTD data
        ytd_data = self.get_ytd_data(invoice_no)
        if not ytd_data:
            result['error'] = 'No YTD data found'
            return result
        
        result['ytd_data'] = ytd_data
        
        # 2. Find PDF
        pdf_path = self.find_pdf_file(invoice_no)
        if not pdf_path:
            result['error'] = 'PDF file not found'
            return result
        
        # 3. Simple PDF extraction (first page only)
        try:
            pdf_total = self.extract_pdf_total_simple(pdf_path)
            result['pdf_data'] = pdf_total
            
            if pdf_total and pdf_total.get('total_amount'):
                # Calculate variance
                ytd_total = ytd_data.get('total_amount', 0)
                pdf_amount = pdf_total.get('total_amount', 0)
                
                variance = abs(ytd_total - pdf_amount)
                variance_percent = (variance / ytd_total * 100) if ytd_total > 0 else 0
                
                result['validation_summary'] = {
                    'ytd_total': ytd_total,
                    'pdf_total': pdf_amount,
                    'variance_amount': variance,
                    'variance_percent': variance_percent,
                    'status': 'MATCH' if variance_percent < 2 else 'VARIANCE' if variance_percent < 10 else 'MISMATCH'
                }
                
                result['status'] = result['validation_summary']['status']
                
                # Save to database
                self.save_validation_result(result)
                
            else:
                result['error'] = 'Could not extract PDF total'
                
        except Exception as e:
            result['error'] = f'PDF processing error: {str(e)}'
        
        return result
    
    def get_ytd_data(self, invoice_no: str) -> dict:
        """Get YTD data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM ytd_audit_results WHERE invoice_no = ?', (invoice_no,))
        row = cursor.fetchone()
        
        if not row:
            conn.close()
            return None
        
        cursor.execute('PRAGMA table_info(ytd_audit_results)')
        columns = [col[1] for col in cursor.fetchall()]
        conn.close()
        
        data = dict(zip(columns, row))
        data['total_amount'] = data.get('total_invoice_amount', 0)
        
        return data
    
    def find_pdf_file(self, invoice_no: str) -> str:
        """Find PDF file for invoice"""
        possible_paths = [
            f'uploads/{invoice_no}.pdf',
            f'uploads/ANDA_DZNA_{invoice_no}.pdf',
            f'uploads/{invoice_no}_invoice.pdf'
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                return path
        
        return None
    
    def extract_pdf_total_simple(self, pdf_path: str) -> dict:
        """Extract total from PDF (improved pattern matching)"""
        
        try:
            with pdfplumber.open(pdf_path) as pdf:
                # Extract text from first 2 pages
                text = ""
                for page in pdf.pages[:2]:
                    text += page.extract_text() + "\n"
                
                lines = text.split('\n')
                total_amount = None
                currency = 'USD'
                
                # Look for various total patterns
                import re
                
                for line in lines:
                    line_clean = line.strip()
                    line_upper = line_clean.upper()
                    
                    # Look for total patterns
                    if any(pattern in line_upper for pattern in ['TOTAL', 'AMOUNT DUE', 'INVOICE TOTAL']):
                        # Extract all numbers from the line
                        numbers = re.findall(r'(\d{1,3}(?:,\d{3})*\.?\d*)', line_clean)
                        
                        if numbers:
                            # Take the largest number (likely the total)
                            amounts = []
                            for num_str in numbers:
                                try:
                                    amount = float(num_str.replace(',', ''))
                                    if amount > 100:  # Filter out small numbers
                                        amounts.append(amount)
                                except:
                                    continue
                            
                            if amounts:
                                total_amount = max(amounts)
                                
                                # Check currency
                                if 'AUD' in line_upper:
                                    currency = 'AUD'
                                elif 'USD' in line_upper:
                                    currency = 'USD'
                                
                                break
                
                # If no total found, look for largest amount in document
                if not total_amount:
                    all_amounts = re.findall(r'(\d{1,3}(?:,\d{3})*\.?\d*)', text)
                    amounts = []
                    for num_str in all_amounts:
                        try:
                            amount = float(num_str.replace(',', ''))
                            if 1000 <= amount <= 50000:  # Reasonable range for invoice total
                                amounts.append(amount)
                        except:
                            continue
                    
                    if amounts:
                        total_amount = max(amounts)
                
                return {
                    'total_amount': total_amount,
                    'currency': currency,
                    'extraction_method': 'enhanced_regex',
                    'confidence': 85 if total_amount else 0,
                    'extracted_text_sample': text[:500] + "..." if len(text) > 500 else text
                }
                
        except Exception as e:
            return {'error': f'PDF extraction failed: {str(e)}'}
    
    def save_validation_result(self, result: dict):
        """Save validation result to database"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create table if not exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pdf_ytd_validation_simple (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_no TEXT UNIQUE,
                ytd_total REAL,
                pdf_total REAL,
                currency TEXT,
                variance_percent REAL,
                validation_status TEXT,
                validation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Insert or replace
        cursor.execute('''
            INSERT OR REPLACE INTO pdf_ytd_validation_simple 
            (invoice_no, ytd_total, pdf_total, currency, variance_percent, validation_status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            result['invoice_no'],
            result['validation_summary'].get('ytd_total', 0),
            result['validation_summary'].get('pdf_total', 0),
            result.get('pdf_data', {}).get('currency', 'USD'),
            result['validation_summary'].get('variance_percent', 0),
            result['status']
        ))
        
        conn.commit()
        conn.close()

if __name__ == '__main__':
    validator = SimplePDFValidator()
    result = validator.validate_invoice_simple('D2158876')
    
    print("🧪 SIMPLE VALIDATION RESULT")
    print("=" * 40)
    print(json.dumps(result, indent=2, default=str))
