#!/usr/bin/env python3
"""
FedEx Invoice Number Extraction System
Extracts 10-digit invoice numbers from FedEx PDF invoices
"""

import fitz  # PyMuPDF
import PyPDF2
import re
import os
from pathlib import Path
import sqlite3
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FedExInvoiceExtractor:
    def __init__(self, db_path='dhl_audit.db'):
        self.db_path = db_path
        self.setup_database()
    
    def setup_database(self):
        """Create table for storing extracted invoice numbers"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fedex_extracted_invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT NOT NULL,
                file_name TEXT NOT NULL,
                invoice_number TEXT,
                extraction_method TEXT,
                extraction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'extracted',
                notes TEXT
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def extract_invoice_number_pymupdf(self, pdf_path):
        """
        Extract invoice number using PyMuPDF - more accurate for positioned text
        """
        try:
            doc = fitz.open(pdf_path)
            
            # Check both page 1 and page 2 as mentioned
            pages_to_check = [0, 1] if len(doc) > 1 else [0]
            
            for page_num in pages_to_check:
                if page_num >= len(doc):
                    continue
                    
                page = doc[page_num]
                text = page.get_text()
                
                # FedEx specific patterns - 10 digit invoice number
                patterns = [
                    r'Invoice Number[:\s]*(\d{10})',  # "Invoice Number: 951092043"
                    r'Invoice No[:\s]*(\d{10})',     # "Invoice No: 951092043"
                    r'账单号码[:\s]*(\d{10})',        # Chinese version
                    r'INV[:\s]*(\d{10})',            # "INV: 951092043"
                    r'\b(\d{10})\b',                 # Any standalone 10-digit number
                ]
                
                for pattern in patterns:
                    matches = re.findall(pattern, text, re.IGNORECASE)
                    if matches:
                        # Validate it looks like a FedEx invoice number
                        for match in matches:
                            if len(match) == 10 and match.isdigit():
                                doc.close()
                                return match, f"PyMuPDF_Page{page_num+1}_Pattern"
                
                # Try to find text blocks in specific positions (top-right area)
                text_dict = page.get_text("dict")
                page_width = page.rect.width
                page_height = page.rect.height
                
                # Look in top-right quadrant where invoice numbers typically appear
                for block in text_dict["blocks"]:
                    if "lines" in block:
                        for line in block["lines"]:
                            line_bbox = line["bbox"]
                            # Check if in top-right area (right 60%, top 40%)
                            if (line_bbox[0] > page_width * 0.4 and 
                                line_bbox[1] < page_height * 0.4):
                                
                                for span in line["spans"]:
                                    span_text = span["text"]
                                    # Look for 10-digit numbers in this area
                                    numbers = re.findall(r'\b(\d{10})\b', span_text)
                                    if numbers:
                                        doc.close()
                                        return numbers[0], f"PyMuPDF_Positioned_Page{page_num+1}"
            
            doc.close()
            return None, "PyMuPDF_NotFound"
            
        except Exception as e:
            logger.error(f"PyMuPDF extraction error for {pdf_path}: {e}")
            return None, f"PyMuPDF_Error: {str(e)}"
    
    def extract_invoice_number_pypdf2(self, pdf_path):
        """
        Fallback extraction using PyPDF2
        """
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                
                # Check first two pages
                pages_to_check = min(2, len(pdf_reader.pages))
                
                for page_num in range(pages_to_check):
                    page = pdf_reader.pages[page_num]
                    text = page.extract_text()
                    
                    # Same patterns as PyMuPDF
                    patterns = [
                        r'Invoice Number[:\s]*(\d{10})',
                        r'Invoice No[:\s]*(\d{10})',
                        r'账单号码[:\s]*(\d{10})',
                        r'INV[:\s]*(\d{10})',
                        r'\b(\d{10})\b',
                    ]
                    
                    for pattern in patterns:
                        matches = re.findall(pattern, text, re.IGNORECASE)
                        if matches:
                            for match in matches:
                                if len(match) == 10 and match.isdigit():
                                    return match, f"PyPDF2_Page{page_num+1}_Pattern"
                
                return None, "PyPDF2_NotFound"
                
        except Exception as e:
            logger.error(f"PyPDF2 extraction error for {pdf_path}: {e}")
            return None, f"PyPDF2_Error: {str(e)}"
    
    def extract_invoice_number(self, pdf_path):
        """
        Main extraction method that tries multiple approaches
        """
        # Try PyMuPDF first (more accurate)
        invoice_number, method = self.extract_invoice_number_pymupdf(pdf_path)
        
        if invoice_number:
            return invoice_number, method
        
        # Fallback to PyPDF2
        invoice_number, method = self.extract_invoice_number_pypdf2(pdf_path)
        
        return invoice_number, method
    
    def process_single_file(self, pdf_path):
        """
        Process a single PDF file and store results
        """
        file_name = os.path.basename(pdf_path)
        logger.info(f"Processing: {file_name}")
        
        invoice_number, method = self.extract_invoice_number(pdf_path)
        
        # Store in database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO fedex_extracted_invoices 
            (file_path, file_name, invoice_number, extraction_method, notes)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            pdf_path,
            file_name,
            invoice_number,
            method,
            f"Extracted on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        ))
        
        conn.commit()
        conn.close()
        
        return {
            'file_name': file_name,
            'invoice_number': invoice_number,
            'method': method,
            'success': invoice_number is not None
        }
    
    def process_folder(self, folder_path):
        """
        Process all PDF files in a folder
        """
        folder_path = Path(folder_path)
        pdf_files = list(folder_path.glob("*.pdf"))
        
        results = []
        success_count = 0
        
        logger.info(f"Found {len(pdf_files)} PDF files to process")
        
        for pdf_file in pdf_files:
            try:
                result = self.process_single_file(str(pdf_file))
                results.append(result)
                
                if result['success']:
                    success_count += 1
                    logger.info(f"✓ {result['file_name']} -> {result['invoice_number']}")
                else:
                    logger.warning(f"✗ {result['file_name']} -> Failed to extract")
                    
            except Exception as e:
                logger.error(f"Error processing {pdf_file}: {e}")
                results.append({
                    'file_name': pdf_file.name,
                    'invoice_number': None,
                    'method': f"Error: {str(e)}",
                    'success': False
                })
        
        logger.info(f"Processing complete: {success_count}/{len(pdf_files)} successful extractions")
        return results
    
    def get_extracted_invoices(self):
        """
        Get all extracted invoice numbers from database
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT file_name, invoice_number, extraction_method, extraction_date, status
            FROM fedex_extracted_invoices
            ORDER BY extraction_date DESC
        ''')
        
        results = cursor.fetchall()
        conn.close()
        
        return [
            {
                'file_name': row[0],
                'invoice_number': row[1],
                'method': row[2],
                'date': row[3],
                'status': row[4]
            }
            for row in results
        ]
    
    def clear_extracted_data(self):
        """
        Clear all extracted data (for testing)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM fedex_extracted_invoices')
        conn.commit()
        conn.close()
        logger.info("Cleared all extracted invoice data")

def main():
    """
    Test the extraction system with the sample files
    """
    extractor = FedExInvoiceExtractor()
    
    # Process the sample folder
    results = extractor.process_folder('c:/ChinaLogisticsAudit/fedexinvoice')
    
    print("\n" + "="*60)
    print("FEDEX INVOICE EXTRACTION RESULTS")
    print("="*60)
    
    for result in results:
        status = "✓ SUCCESS" if result['success'] else "✗ FAILED"
        print(f"{status} | {result['file_name']}")
        if result['success']:
            print(f"         Invoice Number: {result['invoice_number']}")
            print(f"         Method: {result['method']}")
        else:
            print(f"         Error: {result['method']}")
        print("-" * 60)
    
    # Show database contents
    print("\nDATABASE CONTENTS:")
    invoices = extractor.get_extracted_invoices()
    for inv in invoices:
        print(f"File: {inv['file_name']} -> Invoice: {inv['invoice_number']} ({inv['method']})")

if __name__ == "__main__":
    main()
