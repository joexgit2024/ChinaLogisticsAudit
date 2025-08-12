#!/usr/bin/env python3
"""
Show the actual prompt used for PDF processing
"""

from schema_driven_llm_processor import SchemaDrivenLLMProcessor

def show_prompt():
    processor = SchemaDrivenLLMProcessor()
    
    # Sample invoice text
    sample_text = """
    DHL EXPRESS INVOICE
    Invoice No: INV-12345
    Date: 2024-08-01
    Customer: Test Company Ltd
    Service: Express Worldwide
    
    Charges:
    - Express shipping: $50.00
    - Fuel surcharge: $5.00
    - Security fee: $2.50
    
    Subtotal: $57.50
    GST (10%): $5.75
    Total: $63.25
    """
    
    # Generate the prompt
    prompt = processor.create_structured_prompt(sample_text)
    
    print("=== SCHEMA-DRIVEN PROMPT ===")
    print(f"Length: {len(prompt)} characters")
    print("="*50)
    print(prompt)
    print("="*50)

if __name__ == "__main__":
    show_prompt()
