#!/usr/bin/env python3
"""
Simple audit check for DGF system
"""

import sqlite3

# Check database tables
conn = sqlite3.connect('dhl_audit.db')
cursor = conn.cursor()

print("DGF DATABASE STATUS")
print("=" * 30)

# Check quotes
cursor.execute('SELECT quote_id, lane, total_charges FROM dgf_spot_quotes WHERE mode = "AIR"')
quotes = cursor.fetchall()
print(f"PDF Quotes: {len(quotes)}")
for quote in quotes:
    print(f"  - {quote[0]}: {quote[1]} (${quote[2]})")

print()

# Check invoices  
cursor.execute('SELECT quote_id, total_cny, status FROM dgf_invoices WHERE mode = "AIR"')
invoices = cursor.fetchall()
print(f"Excel Invoices: {len(invoices)}")
for invoice in invoices:
    print(f"  - {invoice[0]}: ¥{invoice[1]} ({invoice[2]})")

print()

# Match invoices to quotes
print("MATCHING ANALYSIS:")
print("-" * 20)
for invoice in invoices:
    invoice_quote_id = invoice[0]
    invoice_total_cny = invoice[1]
    
    # Find matching quote
    matching_quote = None
    for quote in quotes:
        if quote[0] == invoice_quote_id:
            matching_quote = quote
            break
    
    if matching_quote:
        quote_total_usd = matching_quote[2]
        # Simple conversion (assuming 7.18 CNY/USD from the data)
        quote_total_cny = quote_total_usd * 7.18
        variance = invoice_total_cny - quote_total_cny
        variance_pct = (variance / quote_total_cny) * 100 if quote_total_cny > 0 else 0
        
        print(f"\n{invoice_quote_id}:")
        print(f"  Quote (PDF): ${quote_total_usd:.2f} = ¥{quote_total_cny:.2f}")
        print(f"  Invoice (Excel): ¥{invoice_total_cny:.2f}")
        print(f"  Variance: ¥{variance:.2f} ({variance_pct:.1f}%)")
    else:
        print(f"\n{invoice_quote_id}: No matching quote found!")

conn.close()
