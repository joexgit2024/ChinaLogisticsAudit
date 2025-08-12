#!/usr/bin/env python3
"""Find the actual shipment weight for BONDED STORAGE calculation"""

import sqlite3

def find_shipment_weight():
    """Find actual shipment weight for MELIR00821620"""
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    print("🔍 INVESTIGATING MELIR00821620 WEIGHT")
    print("=" * 50)
    
    # Get all records for this invoice
    cursor.execute('''
        SELECT dhl_product_description, weight, amount, awb_number, line_number
        FROM dhl_express_invoices 
        WHERE invoice_no = 'MELIR00821620' 
        ORDER BY line_number
    ''')
    
    all_records = cursor.fetchall()
    print(f"📋 Total records: {len(all_records)}")
    
    max_weight = 0
    for desc, weight, amount, awb, line_num in all_records:
        weight_str = f"{weight}kg" if weight else "0kg"
        amount_str = f"${amount}" if amount else "$0"
        print(f"   Line {line_num}: {desc} | {weight_str} | {amount_str} | AWB: {awb}")
        
        if weight and weight > max_weight:
            max_weight = weight
    
    print(f"\n📦 Detected shipment weight: {max_weight}kg")
    
    # Manual calculation for BONDED STORAGE
    base_charge = 18.00  # per shipment
    weight_charge = max_weight * 0.35  # per kg
    expected_charge = max(base_charge, weight_charge)
    
    print(f"\n🧮 BONDED STORAGE CALCULATION:")
    print(f"   Base charge: ${base_charge} per shipment")
    print(f"   Weight charge: {max_weight}kg × $0.35 = ${weight_charge}")
    print(f"   MAX formula: MAX(${base_charge}, ${weight_charge}) = ${expected_charge}")
    
    # Find actual charged amount
    cursor.execute('''
        SELECT amount FROM dhl_express_invoices 
        WHERE invoice_no = 'MELIR00821620' 
        AND dhl_product_description = 'BONDED STORAGE'
    ''')
    
    actual_charge = cursor.fetchone()
    if actual_charge:
        actual_amount = actual_charge[0]
        variance = actual_amount - expected_charge
        print(f"   Actual charged: ${actual_amount}")
        print(f"   Variance: ${variance} ({'OVERCHARGE' if variance > 0 else 'UNDERCHARGE' if variance < 0 else 'CORRECT'})")
    
    conn.close()

if __name__ == "__main__":
    find_shipment_weight()
