#!/usr/bin/env python3
"""
LLM-Enhanced PDF Validation System for YTD Invoices
==================================================

This creates a comprehensive validation system using LLM to extract detailed 
PDF data and compare it with aggregated Excel YTD data.
"""

import sqlite3
import json
import os
from llm_enhanced_pdf_processor import LLMEnhancedPDFProcessor
from datetime import datetime

class PDFYTDValidator:
    """Validate YTD Excel data against detailed PDF extractions using LLM"""
    
    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path
        self.llm_processor = LLMEnhancedPDFProcessor(db_path)
        self.init_validation_database()
    
    def init_validation_database(self):
        """Initialize database tables for PDF-YTD validation"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Enhanced LLM extraction table with detailed PDF data
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pdf_ytd_validation (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_no VARCHAR(50) NOT NULL,
                pdf_file_path VARCHAR(500),
                
                -- YTD Excel Data
                ytd_total_amount DECIMAL(12,2),
                ytd_audit_status VARCHAR(50),
                ytd_variance_percent DECIMAL(8,4),
                ytd_charge_breakdown TEXT,  -- JSON with YTD charge details
                
                -- LLM PDF Data  
                pdf_total_amount DECIMAL(12,2),
                pdf_currency VARCHAR(10),
                pdf_detailed_charges TEXT,  -- JSON with detailed PDF charges
                pdf_confidence FLOAT,
                
                -- Validation Results
                validation_status VARCHAR(50), -- MATCH, VARIANCE, MISMATCH, ERROR
                total_variance_amount DECIMAL(12,2),
                total_variance_percent DECIMAL(8,4),
                charge_level_validation TEXT,  -- JSON with per-charge validation
                validation_summary TEXT,       -- JSON with validation details
                manual_review_required BOOLEAN DEFAULT 0,
                
                -- Metadata
                validation_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                llm_model_used VARCHAR(100),
                validation_notes TEXT,
                
                UNIQUE(invoice_no)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def get_ytd_data(self, invoice_no: str) -> dict:
        """Get YTD data for an invoice"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM ytd_audit_results WHERE invoice_no = ?', (invoice_no,))
        row = cursor.fetchone()
        
        if not row:
            conn.close()
            return None
        
        # Get column names
        cursor.execute('PRAGMA table_info(ytd_audit_results)')
        columns = [col[1] for col in cursor.fetchall()]
        
        conn.close()
        
        # Build YTD data dict with standardized keys
        ytd_data = {}
        for i, value in enumerate(row):
            if value is not None:
                ytd_data[columns[i]] = value
        
        # Add standardized access keys
        ytd_data['total_amount'] = ytd_data.get('total_invoice_amount', 0)
        ytd_data['expected_amount'] = ytd_data.get('total_expected_amount', 0)
        ytd_data['variance_amount'] = ytd_data.get('total_variance', 0)
        
        return ytd_data
    
    def extract_detailed_pdf_charges(self, pdf_text: str) -> dict:
        """Extract detailed charge breakdown from PDF using LLM"""
        
        prompt = '''From this DHL invoice PDF, extract ALL individual charges with precise amounts.
        
Focus on the section with "DESCRIPTION GST IN AUD CHARGES IN AUD" and extract each line item.

Return JSON with this exact structure:
{
  "invoice_details": {
    "invoice_no": "string",
    "currency": "AUD/USD/etc",
    "exchange_rate": "number if mentioned",
    "total_amount": number,
    "gst_total": number
  },
  "detailed_charges": [
    {
      "description": "exact description from PDF",
      "base_amount_usd": number,
      "converted_amount_aud": number, 
      "gst_amount": number,
      "total_line_amount": number,
      "category": "PICKUP|ORIGIN_HANDLING|FREIGHT|FUEL|DESTINATION_HANDLING|DELIVERY|CUSTOMS|OTHER",
      "container_info": "container details if mentioned",
      "rate_info": "rate per container if mentioned"
    }
  ],
  "charge_summary": {
    "subtotal": number,
    "total_gst": number,
    "final_total": number
  }
}

Extract EVERY charge line precisely. Include USD amounts, AUD conversions, exchange rates, and GST details.'''
        
        llm_response = self.llm_processor.query_llm(prompt, pdf_text)
        
        if llm_response:
            try:
                # Extract JSON from response
                start_idx = llm_response.find('{')
                end_idx = llm_response.rfind('}')
                
                if start_idx != -1 and end_idx != -1:
                    json_str = llm_response[start_idx:end_idx + 1]
                    return json.loads(json_str)
            except json.JSONDecodeError as e:
                print(f"JSON parsing error: {e}")
        
        return None
    
    def categorize_ytd_charges(self, ytd_data: dict) -> dict:
        """Extract and categorize charges from YTD audit details"""
        audit_details = ytd_data.get('audit_details')
        
        if not audit_details:
            return {}
        
        try:
            details = json.loads(audit_details) if isinstance(audit_details, str) else audit_details
            
            # Extract charge breakdown from audit results
            charge_breakdown = {}
            
            if 'audit_results' in details and details['audit_results']:
                first_result = details['audit_results'][0]
                if 'charge_breakdown' in first_result:
                    breakdown = first_result['charge_breakdown']
                    
                    for charge_type, charge_data in breakdown.items():
                        if charge_data.get('invoice_amount_usd', 0) != 0:
                            charge_breakdown[charge_type] = {
                                'amount_usd': charge_data.get('invoice_amount_usd', 0),
                                'expected_usd': charge_data.get('rate_card_amount_usd', 0),
                                'variance_usd': charge_data.get('variance_usd', 0),
                                'variance_percent': charge_data.get('percentage_variance', 0),
                                'is_passthrough': charge_data.get('is_passthrough', False)
                            }
            
            return {
                'total_amount': ytd_data.get('total_invoice_amount', 0),
                'charge_breakdown': charge_breakdown,
                'audit_status': ytd_data.get('audit_status', 'unknown'),
                'variance_percent': ytd_data.get('variance_percent', 0)
            }
            
        except Exception as e:
            print(f"Error parsing YTD audit details: {e}")
            return {}
    
    def validate_pdf_vs_ytd(self, invoice_no: str) -> dict:
        """Comprehensive validation of PDF vs YTD data"""
        
        print(f"\\n{'='*60}")
        print(f"VALIDATING INVOICE {invoice_no}")
        print(f"{'='*60}")
        
        # Get YTD data
        ytd_data = self.get_ytd_data(invoice_no)
        if not ytd_data:
            return {'error': 'YTD data not found'}
        
        # Find and process PDF
        pdf_files = [f for f in os.listdir('uploads') if invoice_no in f and f.endswith('.pdf')]
        if not pdf_files:
            return {'error': 'PDF file not found'}
        
        pdf_path = os.path.join('uploads', pdf_files[0])
        
        # Extract PDF text
        pdf_text = self.llm_processor.extract_text_from_pdf(pdf_path)
        if not pdf_text:
            return {'error': 'Could not extract PDF text'}
        
        # Extract detailed charges from PDF
        pdf_charges = self.extract_detailed_pdf_charges(pdf_text)
        if not pdf_charges:
            return {'error': 'LLM failed to extract PDF charges'}
        
        # Categorize YTD charges
        ytd_charges = self.categorize_ytd_charges(ytd_data)
        
        # Perform validation
        validation_result = self.compare_charges(ytd_charges, pdf_charges, invoice_no)
        
        # Save validation results
        self.save_validation_results(invoice_no, ytd_data, pdf_charges, validation_result, pdf_path)
        
        return validation_result
    
    def compare_charges(self, ytd_charges: dict, pdf_charges: dict, invoice_no: str) -> dict:
        """Compare YTD and PDF charges in detail"""
        
        validation = {
            'invoice_no': invoice_no,
            'validation_status': 'UNKNOWN',
            'total_variance_amount': 0,
            'total_variance_percent': 0,
            'charge_validations': [],
            'summary': {},
            'manual_review_required': False
        }
        
        print(f"\\n📊 YTD DATA:")
        print(f"Total Amount: ${ytd_charges.get('total_amount', 0):,.2f} USD")
        print(f"Audit Status: {ytd_charges.get('audit_status', 'unknown')}")
        print(f"YTD Variance: {ytd_charges.get('variance_percent', 0):.2f}%")
        
        print(f"\\n🔍 PDF DATA:")
        pdf_total = pdf_charges.get('charge_summary', {}).get('final_total', 0)
        pdf_currency = pdf_charges.get('invoice_details', {}).get('currency', 'AUD')
        print(f"Total Amount: ${pdf_total:,.2f} {pdf_currency}")
        print(f"Number of detailed charges: {len(pdf_charges.get('detailed_charges', []))}")
        
        # Detailed charge comparison
        print(f"\\n💰 DETAILED CHARGE BREAKDOWN FROM PDF:")
        for charge in pdf_charges.get('detailed_charges', []):
            print(f"  • {charge.get('description', 'N/A')}")
            print(f"    USD: ${charge.get('base_amount_usd', 0):,.2f}")
            print(f"    AUD: ${charge.get('converted_amount_aud', 0):,.2f}")
            print(f"    GST: ${charge.get('gst_amount', 0):,.2f}")
            print(f"    Category: {charge.get('category', 'OTHER')}")
            print()
        
        print(f"\\n🔄 YTD CHARGE BREAKDOWN:")
        for charge_type, data in ytd_charges.get('charge_breakdown', {}).items():
            if data['amount_usd'] != 0:
                print(f"  • {charge_type}: ${data['amount_usd']:,.2f} USD")
        
        # Calculate validation status
        if abs(ytd_charges.get('variance_percent', 0)) < 2.0:
            validation['validation_status'] = 'APPROVED'
        elif abs(ytd_charges.get('variance_percent', 0)) < 5.0:
            validation['validation_status'] = 'WARNING'
        else:
            validation['validation_status'] = 'REJECTED'
            validation['manual_review_required'] = True
        
        validation['summary'] = {
            'ytd_total_usd': ytd_charges.get('total_amount', 0),
            'pdf_total_aud': pdf_total,
            'pdf_currency': pdf_currency,
            'ytd_variance_percent': ytd_charges.get('variance_percent', 0),
            'pdf_charge_count': len(pdf_charges.get('detailed_charges', [])),
            'ytd_charge_count': len([c for c in ytd_charges.get('charge_breakdown', {}).values() if c['amount_usd'] != 0])
        }
        
        return validation
    
    def save_validation_results(self, invoice_no: str, ytd_data: dict, pdf_charges: dict, 
                              validation: dict, pdf_path: str):
        """Save validation results to database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO pdf_ytd_validation
            (invoice_no, pdf_file_path, ytd_total_amount, ytd_audit_status, 
             ytd_variance_percent, ytd_charge_breakdown, pdf_total_amount, 
             pdf_currency, pdf_detailed_charges, validation_status, 
             total_variance_percent, validation_summary, manual_review_required,
             llm_model_used)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            invoice_no,
            pdf_path,
            ytd_data.get('total_invoice_amount', 0),
            ytd_data.get('audit_status', 'unknown'),
            ytd_data.get('variance_percent', 0),
            json.dumps(ytd_data.get('audit_details', {})),
            pdf_charges.get('charge_summary', {}).get('final_total', 0),
            pdf_charges.get('invoice_details', {}).get('currency', 'AUD'),
            json.dumps(pdf_charges, indent=2),
            validation.get('validation_status', 'UNKNOWN'),
            validation.get('total_variance_percent', 0),
            json.dumps(validation.get('summary', {}), indent=2),
            validation.get('manual_review_required', False),
            'llama3.2:3b'
        ))
        
        conn.commit()
        conn.close()
        
        print(f"\\n✅ Validation results saved to database")

def main():
    """Test the validation system with D2158876"""
    validator = PDFYTDValidator()
    
    # Validate D2158876
    result = validator.validate_pdf_vs_ytd('D2158876')
    
    if 'error' in result:
        print(f"❌ Validation failed: {result['error']}")
    else:
        print(f"\\n🎯 VALIDATION SUMMARY:")
        print(f"Status: {result.get('validation_status', 'unknown')}")
        print(f"Manual Review Required: {result.get('manual_review_required', False)}")
        
        summary = result.get('summary', {})
        print(f"\\nComparison:")
        print(f"  YTD Total: ${summary.get('ytd_total_usd', 0):,.2f} USD")
        print(f"  PDF Total: ${summary.get('pdf_total_aud', 0):,.2f} {summary.get('pdf_currency', 'AUD')}")
        print(f"  YTD Variance: {summary.get('ytd_variance_percent', 0):.2f}%")
        print(f"  Detailed Charges: {summary.get('pdf_charge_count', 0)} PDF vs {summary.get('ytd_charge_count', 0)} YTD")

if __name__ == "__main__":
    main()
