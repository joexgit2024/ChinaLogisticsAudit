import fitz  # PyMuPDF
import re
import os
import glob

def extract_fedex_invoice_manual_input(pdf_path):
    """Simple approach - show PDF info and ask for manual input"""
    try:
        doc = fitz.open(pdf_path)
        print(f"\nPDF: {os.path.basename(pdf_path)}")
        print(f"Pages: {doc.page_count}")
        
        # Try text extraction first
        for page_num in range(min(3, doc.page_count)):  # Check first 3 pages
            page = doc[page_num]
            text = page.get_text()
            if text.strip():
                print(f"\nPage {page_num + 1} has extractable text:")
                print(text[:300] + "..." if len(text) > 300 else text)
                
                # Look for invoice numbers
                pattern = r'\b\d{10}\b'
                matches = re.findall(pattern, text)
                if matches:
                    print(f"Found potential invoice numbers: {matches}")
                    return matches[0]
        
        print("\n" + "="*50)
        print("No extractable text found - this appears to be an image-based PDF")
        print("To extract invoice numbers, you need:")
        print("1. Install Tesseract OCR: https://github.com/UB-Mannheim/tesseract/wiki")
        print("2. Or manually enter invoice numbers from the PDF")
        print("="*50)
        
        doc.close()
        return None
        
    except Exception as e:
        print(f"Error: {e}")
        return None

def create_simple_extractor_function():
    """Create the function for use in other scripts"""
    return """
def extract_fedex_invoice_number(pdf_path):
    '''Simple FedEx invoice extractor - requires manual setup for OCR'''
    import fitz
    import re
    
    try:
        doc = fitz.open(pdf_path)
        
        # Check all pages for text
        for page_num in range(doc.page_count):
            page = doc[page_num]
            text = page.get_text()
            
            # Look for 10-digit invoice numbers
            pattern = r'\\b\\d{10}\\b'
            matches = re.findall(pattern, text)
            if matches:
                doc.close()
                return matches[0]
        
        doc.close()
        # For image-based PDFs, return None and handle manually
        return None
        
    except Exception as e:
        print(f"Error extracting from {pdf_path}: {e}")
        return None
"""

if __name__ == "__main__":
    print("FedEx Invoice Number Extractor - Simple Version")
    print("=" * 50)
    
    # Test with sample files
    pdf_files = glob.glob("fedexinvoice/*.pdf")
    
    if not pdf_files:
        print("No PDF files found in fedexinvoice folder")
    else:
        for i, pdf_file in enumerate(pdf_files[:2]):  # Test first 2 files
            print(f"\n[{i+1}] Testing: {os.path.basename(pdf_file)}")
            result = extract_fedex_invoice_manual_input(pdf_file)
            if result:
                print(f"✓ Found invoice number: {result}")
            else:
                print("✗ No invoice number extracted automatically")
    
    print(f"\nFunction code for integration:")
    print(create_simple_extractor_function())
