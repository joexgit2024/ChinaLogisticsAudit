#!/usr/bin/env python3
"""
Simple FedEx Invoice Number Extractor using pdfplumber
Extract PDF as text, then use regex to find 10-digit invoice numbers
"""

import pdfplumber
import re
import os
from pathlib import Path

def extract_text_from_pdf(pdf_path):
    """
    Extract all text from PDF using pdfplumber (better than PyPDF2)
    """
    text = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            # Extract text from first 2 pages (invoice numbers usually on page 1 or 2)
            for i, page in enumerate(pdf.pages[:2]):
                page_text = page.extract_text()
                if page_text:
                    text += f"PAGE {i+1}:\n{page_text}\n\n"
                
    except Exception as e:
        print(f"Error reading PDF {pdf_path}: {e}")
        
    return text

def find_invoice_number(text):
    """
    Use regex to find 10-digit invoice number
    """
    # Clean up text - remove extra whitespace
    text = ' '.join(text.split())
    
    # Look for 10-digit numbers that could be invoice numbers
    pattern = r'\b(\d{10})\b'
    
    matches = re.findall(pattern, text)
    
    # Return the first valid 10-digit number found
    for match in matches:
        # Basic validation - not all zeros, ones, or sequential numbers
        if not (all(c == '0' for c in match) or 
                all(c == '1' for c in match) or
                match == '1234567890'):
            return match
    
    return None

def extract_invoice_from_pdf(pdf_path):
    """
    Main function to extract invoice number from PDF
    """
    print(f"Processing: {os.path.basename(pdf_path)}")
    
    # Step 1: Extract text from PDF
    text = extract_text_from_pdf(pdf_path)
    
    if not text.strip():
        print("  ✗ No text found in PDF")
        return None
    
    print(f"  ✓ Extracted text from PDF ({len(text)} characters)")
    
    # Step 2: Find invoice number using regex
    invoice_number = find_invoice_number(text)
    
    if invoice_number:
        print(f"  ✓ Found invoice number: {invoice_number}")
        return invoice_number
    else:
        print("  ✗ No invoice number found")
        
        # Show all 10-digit numbers found for debugging
        all_numbers = re.findall(r'\b(\d{10})\b', text)
        if all_numbers:
            print(f"  10-digit numbers found: {all_numbers}")
        
        # Show sample text for debugging
        sample = text[:300].replace('\n', ' ')
        print(f"  Sample text: {sample}...")
        return None

def test_extraction():
    """
    Test the extraction on sample files
    """
    folder_path = Path("c:/ChinaLogisticsAudit/fedexinvoice")
    
    if not folder_path.exists():
        print(f"Folder not found: {folder_path}")
        return
    
    pdf_files = list(folder_path.glob("*.pdf"))
    print(f"Found {len(pdf_files)} PDF files\n")
    
    results = []
    
    for pdf_file in pdf_files:
        invoice_number = extract_invoice_from_pdf(str(pdf_file))
        results.append({
            'file': pdf_file.name,
            'invoice_number': invoice_number,
            'success': invoice_number is not None
        })
        print("-" * 50)
    
    # Summary
    print("\n" + "="*60)
    print("EXTRACTION RESULTS:")
    print("="*60)
    
    success_count = 0
    for result in results:
        status = "✓ SUCCESS" if result['success'] else "✗ FAILED"
        print(f"{status} | {result['file']}")
        if result['success']:
            print(f"         Invoice Number: {result['invoice_number']}")
            success_count += 1
        print()
    
    print(f"Success rate: {success_count}/{len(results)} ({success_count/len(results)*100:.1f}%)")

if __name__ == "__main__":
    test_extraction()
