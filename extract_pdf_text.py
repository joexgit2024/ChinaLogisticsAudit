import pdfplumber
import json

def extract_pdf_text(pdf_path):
    """Extract text from PDF"""
    text_content = ""
    
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                print(f"\n=== PAGE {page_num + 1} ===")
                page_text = page.extract_text()
                if page_text:
                    print(page_text)
                    text_content += page_text + "\n"
    except Exception as e:
        print(f"Error extracting text: {e}")
    
    return text_content

# Extract text from the specific invoice
pdf_path = "uploads/ANDA_DZNA_D2130110.pdf"
text = extract_pdf_text(pdf_path)
