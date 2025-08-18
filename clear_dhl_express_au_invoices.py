#!/usr/bin/env python3
"""
Clear DHL Express AU Invoice Data for China Migration
===================================================
"""
import sqlite3
from datetime import datetime

def clear_au_invoice_data():
    """Clear all AU DHL Express invoice data to prepare for Chinese invoices"""
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    print("🧹 Clearing DHL Express AU invoice data for China migration...")
    
    # Tables to clear
    invoice_tables = [
        'dhl_express_invoices',
        'dhl_express_audit_results',
        'audit_results',
        'charge_codes'
    ]
    
    cleared_count = 0
    
    for table in invoice_tables:
        try:
            cursor.execute(f'SELECT COUNT(*) FROM {table}')
            count = cursor.fetchone()[0]
            
            if count > 0:
                cursor.execute(f'DELETE FROM {table}')
                print(f"   ✅ Cleared {count} records from {table}")
                cleared_count += count
            else:
                print(f"   ➖ {table} already empty")
                
        except Exception as e:
            print(f"   ⚠️  Error clearing {table}: {e}")
    
    # Reset auto-increment sequences
    cursor.execute("DELETE FROM sqlite_sequence WHERE name LIKE 'dhl_express_%'")
    
    conn.commit()
    
    # Verification
    print(f"\n📋 Verification:")
    for table in invoice_tables:
        try:
            cursor.execute(f'SELECT COUNT(*) FROM {table}')
            count = cursor.fetchone()[0]
            status = "✅ Empty" if count == 0 else f"❌ Still has {count} records"
            print(f"   {table}: {status}")
        except:
            print(f"   {table}: ❓ Could not verify")
    
    conn.close()
    
    print(f"\n🎉 AU invoice data cleanup completed at {datetime.now()}")
    print(f"   Total records cleared: {cleared_count}")
    print("   Ready for Chinese invoice loading!")

if __name__ == '__main__':
    clear_au_invoice_data()
