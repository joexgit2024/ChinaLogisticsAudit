#!/usr/bin/env python3
"""Test the audit engine specifically for DIRECT SIGNATURE charges"""

import sqlite3
from pprint import pprint

def find_signature_invoices():
    """Find invoices with DIRECT SIGNATURE charges"""
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # First check the service charges
    print("DIRECT SIGNATURE service charge details:")
    cursor.execute('''
        SELECT * FROM dhl_express_services_surcharges 
        WHERE service_name LIKE '%SIGNATURE%'
    ''')
    signatures = cursor.fetchall()
    for sig in signatures:
        print(f"  ID: {sig[0]}")
        print(f"  Service Code: {sig[1]}")
        print(f"  Service Name: {sig[2]}")
        print(f"  Charge Type: {sig[3]}")
        print(f"  Charge Amount: ${sig[4]}")
        print()
    
    # Find invoices with SIGNATURE charges
    print("Looking for DIRECT SIGNATURE charges in invoices:")
    cursor.execute('''
        SELECT * FROM dhl_express_invoices 
        WHERE dhl_product_description LIKE '%SIGNATURE%'
    ''')
    
    invoices = cursor.fetchall()
    print(f"Found {len(invoices)} invoice line items with SIGNATURE:")
    
    for inv in invoices:
        print(f"\nInvoice: {inv[1]}")
        print(f"  Line Number: {inv[5]}")
        print(f"  Product Description: {inv[7]}")
        print(f"  Amount: ${inv[9]}")
        print(f"  AWB Number: {inv[15]}")
        print(f"  Date: {inv[18]}")
    
    # Check if there are any audit results for these invoices
    if invoices:
        invoice_ids = [inv[1] for inv in invoices]
        placeholders = ','.join(['?' for _ in invoice_ids])
        query = f'''
            SELECT * FROM dhl_express_audit_results 
            WHERE invoice_no IN ({placeholders})
        '''
        cursor.execute(query, invoice_ids)
        audit_results = cursor.fetchall()
        
        print(f"\nFound {len(audit_results)} audit results for these invoices:")
        for res in audit_results:
            print(f"Invoice: {res[1]}")
            print(f"  AWB Number: {res[2]}")
            print(f"  Audit Status: {res[8]}")
            print(f"  Total Invoice Amount: ${res[4]}")
            print(f"  Total Expected Amount: ${res[5]}")
            print(f"  Total Variance: ${res[6]}")
            print(f"  Variance Percentage: {res[7]}%")
            
            # Look for SIGNATURE line items in the detailed results
            import json
            details = json.loads(res[12])
            for item in details:
                if 'SIGNATURE' in item.get('product_description', ''):
                    print(f"\n  SIGNATURE line item:")
                    print(f"    Description: {item['product_description']}")
                    print(f"    Invoiced Amount: ${item['invoiced_amount']}")
                    print(f"    Expected Amount: ${item['expected_amount']}")
                    print(f"    Variance: ${item['variance']}")
                    print(f"    Result: {item['audit_result']}")
                    print(f"    Comments: {item['comments']}")
    
    conn.close()

if __name__ == "__main__":
    find_signature_invoices()
