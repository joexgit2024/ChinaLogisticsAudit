#!/usr/bin/env python3
"""
Enhanced FedEx Invoice Number Extraction with OCR Support
Handles both text-based and image-based PDFs
"""

import fitz  # PyMuPDF
import pytesseract
import re
import os
from pathlib import Path
import sqlite3
from datetime import datetime
import logging
from PIL import Image
import io

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EnhancedFedExExtractor:
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
    
    def extract_text_from_pdf(self, pdf_path):
        """
        Extract text using multiple methods
        """
        try:
            doc = fitz.open(pdf_path)
            
            for page_num in range(min(2, len(doc))):  # Check first 2 pages
                page = doc[page_num]
                
                # Method 1: Direct text extraction
                text = page.get_text()
                if text.strip():
                    yield text, f"direct_text_page_{page_num + 1}"
                
                # Method 2: OCR on page image
                try:
                    # Convert page to image
                    pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))  # 2x zoom for better OCR
                    img_data = pix.tobytes("png")
                    
                    # OCR the image
                    image = Image.open(io.BytesIO(img_data))
                    ocr_text = pytesseract.image_to_string(image, lang='eng+chi_sim')
                    
                    if ocr_text.strip():
                        yield ocr_text, f"ocr_page_{page_num + 1}"
                        
                except Exception as e:
                    logger.warning(f"OCR failed for page {page_num + 1}: {e}")
                
                # Method 3: Text blocks with position (for better extraction)
                try:
                    text_dict = page.get_text("dict")
                    page_width = page.rect.width
                    page_height = page.rect.height
                    
                    # Extract text from top-right area where invoice numbers usually are
                    top_right_text = []
                    for block in text_dict["blocks"]:
                        if "lines" in block:
                            for line in block["lines"]:
                                line_bbox = line["bbox"]
                                # Top-right quadrant
                                if (line_bbox[0] > page_width * 0.3 and 
                                    line_bbox[1] < page_height * 0.5):
                                    
                                    for span in line["spans"]:
                                        top_right_text.append(span["text"])
                    
                    if top_right_text:
                        positioned_text = " ".join(top_right_text)
                        yield positioned_text, f"positioned_text_page_{page_num + 1}"
                        
                except Exception as e:
                    logger.warning(f"Positioned text extraction failed for page {page_num + 1}: {e}")
            
            doc.close()
            
        except Exception as e:
            logger.error(f"PDF extraction error for {pdf_path}: {e}")
    
    def find_invoice_number(self, text):
        """
        Find invoice number using multiple patterns
        """
        # Enhanced patterns for FedEx invoices
        patterns = [
            # Explicit invoice number patterns
            r'Invoice\s*Number[:\s]*(\d{10})',
            r'Invoice\s*No\.?[:\s]*(\d{10})',
            r'账单号码[:\s]*(\d{10})',
            r'帐单号码[:\s]*(\d{10})',
            r'INV\s*#?[:\s]*(\d{10})',
            r'Bill\s*Number[:\s]*(\d{10})',
            r'Reference[:\s]*(\d{10})',
            
            # Pattern for the specific format you showed: 951092043
            r'\b(9\d{8})\b',  # Starts with 9, followed by 8 more digits
            r'\b(\d{10})\b',  # Any 10-digit number
            
            # With separators
            r'(\d{3}-\d{3}-\d{4})',  # XXX-XXX-XXXX format
            r'(\d{4}-\d{3}-\d{3})',  # XXXX-XXX-XXX format
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE)
            for match in matches:
                # Clean the match (remove separators)
                clean_match = re.sub(r'[^\d]', '', match)
                
                # Validate it's a 10-digit number
                if len(clean_match) == 10 and clean_match.isdigit():
                    # Additional validation for FedEx format
                    # FedEx invoice numbers often start with certain digits
                    if clean_match[0] in ['9', '8', '7', '1', '2']:  # Common starting digits
                        return clean_match
        
        return None
    
    def extract_invoice_number(self, pdf_path):
        """
        Main extraction method
        """
        file_name = os.path.basename(pdf_path)
        logger.info(f"Extracting from: {file_name}")
        
        # Try different text extraction methods
        for text, method in self.extract_text_from_pdf(pdf_path):
            invoice_number = self.find_invoice_number(text)
            if invoice_number:
                logger.info(f"Found invoice number {invoice_number} using {method}")
                return invoice_number, method
            else:
                # Debug: show what text we extracted
                logger.debug(f"Method {method} extracted: {text[:200]}...")
        
        return None, "not_found"
    
    def process_single_file(self, pdf_path):
        """
        Process a single PDF file
        """
        file_name = os.path.basename(pdf_path)
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
                    print(f"✓ {result['file_name']} -> {result['invoice_number']}")
                else:
                    print(f"✗ {result['file_name']} -> Failed to extract")
                    
            except Exception as e:
                logger.error(f"Error processing {pdf_file}: {e}")
                results.append({
                    'file_name': pdf_file.name,
                    'invoice_number': None,
                    'method': f"Error: {str(e)}",
                    'success': False
                })
        
        print(f"\nProcessing complete: {success_count}/{len(pdf_files)} successful extractions")
        return results
    
    def debug_single_file(self, pdf_path):
        """
        Debug extraction for a single file
        """
        print(f"Debugging: {pdf_path}")
        print("="*60)
        
        for text, method in self.extract_text_from_pdf(pdf_path):
            print(f"\n--- {method.upper()} ---")
            print(text[:500] + "..." if len(text) > 500 else text)
            
            # Look for any numbers
            numbers = re.findall(r'\d+', text)
            ten_digit_numbers = [n for n in numbers if len(n) == 10]
            
            if ten_digit_numbers:
                print(f"10-digit numbers found: {ten_digit_numbers}")
            
            invoice_number = self.find_invoice_number(text)
            if invoice_number:
                print(f"INVOICE NUMBER FOUND: {invoice_number}")
                return invoice_number
            
            print("-" * 40)
        
        return None

def main():
    """
    Test the enhanced extraction system
    """
    extractor = EnhancedFedExExtractor()
    
    # Debug one file first
    test_file = "c:/ChinaLogisticsAudit/fedexinvoice/paper_extraction_9c1234c1f4ffa06d28c93c6fcb836c1a8e8aa474.pdf"
    
    if Path(test_file).exists():
        print("DEBUGGING SINGLE FILE:")
        result = extractor.debug_single_file(test_file)
        print(f"Result: {result}")
        print("\n" + "="*60)
    
    # Process all files
    print("PROCESSING ALL FILES:")
    results = extractor.process_folder('c:/ChinaLogisticsAudit/fedexinvoice')
    
    print("\n" + "="*60)
    print("SUMMARY:")
    for result in results:
        status = "✓" if result['success'] else "✗"
        print(f"{status} {result['file_name']} -> {result['invoice_number']} ({result['method']})")

if __name__ == "__main__":
    main()
