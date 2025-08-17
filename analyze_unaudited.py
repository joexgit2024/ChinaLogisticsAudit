#!/usr/bin/env python3
"""
Analyze Unaudited AWBs
======================

Check why some AWBs might not be getting audited
"""

import sqlite3

def analyze_unaudited():
    conn = sqlite3.connect('fedex_audit.db')
    cursor = conn.cursor()
    
    # Get counts
    cursor.execute('SELECT COUNT(*) FROM fedex_invoices WHERE audit_status IS NULL')
    unaudited_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT invoice_no) FROM fedex_invoices')
    total_invoices = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM fedex_invoices')
    total_awbs = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM fedex_invoices WHERE audit_status IS NOT NULL')
    audited_count = cursor.fetchone()[0]
    
    print(f"📊 AUDIT STATUS ANALYSIS")
    print("=" * 40)
    print(f"Total Invoices: {total_invoices}")
    print(f"Total AWBs: {total_awbs}")
    print(f"Audited AWBs: {audited_count}")
    print(f"Unaudited AWBs: {unaudited_count}")
    print()
    
    # Show some unaudited examples
    if unaudited_count > 0:
        print("🔍 SAMPLE UNAUDITED AWBs:")
        print("-" * 40)
        cursor.execute('''
            SELECT invoice_no, awb_number, service_type, origin_country, dest_country, 
                   actual_weight_kg, total_awb_amount_cny
            FROM fedex_invoices 
            WHERE audit_status IS NULL 
            LIMIT 10
        ''')
        
        for row in cursor.fetchall():
            print(f"Invoice: {row[0]} | AWB: {row[1]} | Service: {row[2]} | Route: {row[3]}->{row[4]} | Weight: {row[5]}kg | Amount: ¥{row[6]}")
    
    # Check invoice-level distribution
    print("\n📋 INVOICE-LEVEL ANALYSIS:")
    print("-" * 40)
    cursor.execute('''
        SELECT invoice_no, 
               COUNT(*) as total_awbs,
               COUNT(CASE WHEN audit_status IS NOT NULL THEN 1 END) as audited_awbs
        FROM fedex_invoices 
        GROUP BY invoice_no
        HAVING total_awbs != audited_awbs
        ORDER BY invoice_no
        LIMIT 10
    ''')
    
    incomplete_invoices = cursor.fetchall()
    if incomplete_invoices:
        print("Invoices with partial audits:")
        for invoice_no, total, audited in incomplete_invoices:
            print(f"  {invoice_no}: {audited}/{total} AWBs audited")
    else:
        print("All invoices have either 0% or 100% of AWBs audited")
    
    conn.close()

if __name__ == "__main__":
    analyze_unaudited()
