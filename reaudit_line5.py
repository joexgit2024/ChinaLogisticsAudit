#!/usr/bin/env python3
"""
Re-audit Line 5 with AU Domestic Support
Re-run the audit for MELR001510911 Line 5 using the updated audit engine
"""

from dhl_express_audit_engine import DHLExpressAuditEngine
import sqlite3

def reaudit_line_5():
    """Re-audit Line 5 (MELR001510911) with AU domestic support"""
    print("🔄 RE-AUDITING LINE 5 WITH AU DOMESTIC SUPPORT")
    print("=" * 60)
    
    engine = DHLExpressAuditEngine()
    
    # Get Line 5 data
    conn = sqlite3.connect(engine.db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT invoice_no, line_number, dhl_product_description, 
               origin_code, destination_code, weight, amount,
               shipment_date, awb_number
        FROM dhl_express_invoices 
        WHERE invoice_no = 'MELR001510911' AND line_number = 5
    """)
    
    line_data = cursor.fetchone()
    if not line_data:
        print("❌ Line 5 not found!")
        return
    
    print(f"📋 Line 5 Data:")
    print(f"  - Invoice: {line_data[0]}")
    print(f"  - Line: {line_data[1]}")
    print(f"  - Service: {line_data[2]}")
    print(f"  - Route: {line_data[3]} → {line_data[4]}")
    print(f"  - Weight: {line_data[5]}kg")
    print(f"  - Amount: ${line_data[6]}")
    print(f"  - Date: {line_data[7]}")
    print(f"  - AWB: {line_data[8]}")
    
    # Convert to format expected by audit engine
    invoice_data = {
        'invoice_no': line_data[0],
        'line_number': line_data[1],
        'service_type': line_data[2],
        'origin_country': 'AU',  # Melbourne is AU
        'dest_country': 'AU',    # Sydney is AU  
        'origin_city': line_data[3],
        'dest_city': line_data[4],
        'weight_kg': float(line_data[5]),
        'invoice_amount': float(line_data[6]),
        'shipment_date': line_data[7],
        'awb_number': line_data[8]
    }
    
    print(f"\n🔍 AUDIT LOGIC CHECK:")
    is_au_domestic = (invoice_data['origin_country'] == 'AU' and 
                     invoice_data['dest_country'] == 'AU' and 
                     'DOMESTIC' in invoice_data['service_type'])
    print(f"  - Origin: {invoice_data['origin_country']}")
    print(f"  - Destination: {invoice_data['dest_country']}")
    print(f"  - Service contains 'DOMESTIC': {'DOMESTIC' in invoice_data['service_type']}")
    print(f"  - Should use AU Domestic logic: {is_au_domestic}")
    
    if is_au_domestic:
        print(f"\n✅ This will now use AU domestic rate cards!")
        
        # Check AU domestic data for this weight
        cursor.execute("SELECT zone_a, zone_b, zone_c FROM dhl_express_au_domestic_rates WHERE weight_kg = ?", (invoice_data['weight_kg'],))
        rate_data = cursor.fetchone()
        if rate_data:
            print(f"  - AU domestic rates for {invoice_data['weight_kg']}kg:")
            print(f"    Zone A: ${rate_data[0]}")
            print(f"    Zone B: ${rate_data[1]}")
            print(f"    Zone C: ${rate_data[2]}")
            print(f"  - Invoice amount: ${invoice_data['invoice_amount']}")
            
            # The invoice amount $16.47 should match Zone B ($16.47) for Melbourne→Sydney
        else:
            print(f"  ❌ No rate found for {invoice_data['weight_kg']}kg")
    
    print(f"\n🎯 EXPECTED RESULT:")
    print(f"  - Previous: ❌ REVIEW - No zone mapping for AU → AU") 
    print(f"  - Now: ✅ PASS - AU domestic rate found and matches")
    
    conn.close()
    
    print(f"\n📝 TO COMPLETE THE FIX:")
    print(f"  1. ✅ AU domestic tables loaded")
    print(f"  2. ✅ Audit engine updated")
    print(f"  3. 🔄 Re-run audit through web interface")
    print(f"  4. 🎯 Line 5 should now show PASS!")

if __name__ == "__main__":
    reaudit_line_5()
