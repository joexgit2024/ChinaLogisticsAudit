import fitz  # PyMuPDF
import pytesseract
from PIL import Image
import re
import io
import os
import glob

def extract_fedex_invoice_number_pymupdf(pdf_path):
    """Extract FedEx invoice number using PyMuPDF + OCR"""
    try:
        # Open PDF
        doc = fitz.open(pdf_path)
        print(f"PDF has {doc.page_count} pages")
        
        # Focus on page 2 (index 1) where you mentioned the invoice number is
        page_index = 1 if doc.page_count >= 2 else 0
        page = doc[page_index]
        
        # First try text extraction
        text = page.get_text()
        if text.strip():
            print(f"Found extractable text on page {page_index + 1}")
            pattern = r'\b\d{10}\b'
            matches = re.findall(pattern, text)
            if matches:
                doc.close()
                return matches[0]
        
        # If no text, convert page to image and use OCR
        print(f"No extractable text, using OCR on page {page_index + 1}")
        pix = page.get_pixmap()
        img_data = pix.tobytes("png")
        img = Image.open(io.BytesIO(img_data))
        
        # Use OCR
        ocr_text = pytesseract.image_to_string(img)
        print(f"OCR Text sample: {ocr_text[:200]}...")
        
        # Look for invoice number
        pattern = r'\b\d{10}\b'
        matches = re.findall(pattern, ocr_text)
        
        doc.close()
        
        if matches:
            print(f"Found potential invoice numbers: {matches}")
            return matches[0]
        else:
            print("No 10-digit number found")
            return None
            
    except Exception as e:
        print(f"Error processing PDF: {e}")
        return None

if __name__ == "__main__":
    # Test with sample files
    pdf_files = glob.glob("fedexinvoice/*.pdf")
    
    if not pdf_files:
        print("No PDF files found in fedexinvoice folder")
    else:
        # Test first file
        pdf_file = pdf_files[0]
        print(f"\nProcessing: {pdf_file}")
        invoice_num = extract_fedex_invoice_number_pymupdf(pdf_file)
        if invoice_num:
            print(f"✓ Invoice Number: {invoice_num}")
        else:
            print("✗ No invoice number found")
            
        # Also try another file if available
        if len(pdf_files) > 1:
            pdf_file = pdf_files[1]
            print(f"\nProcessing: {pdf_file}")
            invoice_num = extract_fedex_invoice_number_pymupdf(pdf_file)
            if invoice_num:
                print(f"✓ Invoice Number: {invoice_num}")
            else:
                print("✗ No invoice number found")
