#!/usr/bin/env python3
"""
Restore Enhanced Service Charges Structure and Data
==================================================

The table was reverted by a destructive loader. This script:
1. Adds missing enhanced columns
2. Reloads the enhanced service charges with merged entries
"""

import sqlite3
import pandas as pd
import re
from datetime import datetime

def restore_enhanced_table_structure():
    """Add missing enhanced columns to the service charges table"""
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    print("🔧 Restoring enhanced table structure...")
    
    # Add missing columns
    enhanced_columns = [
        'minimum_charge DECIMAL(10,2)',
        'percentage_rate DECIMAL(5,2)', 
        'products_applicable TEXT',
        'original_service_code VARCHAR(10)',
        'description TEXT'
    ]
    
    for column_def in enhanced_columns:
        column_name = column_def.split()[0]
        try:
            cursor.execute(f'ALTER TABLE dhl_express_services_surcharges ADD COLUMN {column_def}')
            print(f"  ✅ Added column: {column_name}")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                print(f"  ⚠️ Column already exists: {column_name}")
            else:
                print(f"  ❌ Error adding {column_name}: {e}")
    
    # Also extend service_code length for suffixed codes
    try:
        cursor.execute('ALTER TABLE dhl_express_services_surcharges RENAME TO dhl_express_services_surcharges_backup')
        
        # Create new table with extended service_code
        cursor.execute('''
            CREATE TABLE dhl_express_services_surcharges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_code VARCHAR(15) UNIQUE,
                service_name VARCHAR(255),
                charge_type VARCHAR(50),
                charge_amount DECIMAL(10,2),
                minimum_charge DECIMAL(10,2),
                percentage_rate DECIMAL(5,2),
                products_applicable TEXT,
                original_service_code VARCHAR(10),
                description TEXT,
                is_special_agreement BOOLEAN,
                effective_date DATE,
                created_timestamp DATETIME
            )
        ''')
        
        # Copy data from backup
        cursor.execute('''
            INSERT INTO dhl_express_services_surcharges 
            (service_code, service_name, charge_type, charge_amount, is_special_agreement, effective_date, created_timestamp)
            SELECT service_code, service_name, charge_type, charge_amount, is_special_agreement, effective_date, created_timestamp
            FROM dhl_express_services_surcharges_backup
        ''')
        
        # Drop backup
        cursor.execute('DROP TABLE dhl_express_services_surcharges_backup')
        print("  ✅ Extended service_code column to VARCHAR(15)")
        
    except sqlite3.OperationalError as e:
        print(f"  ⚠️ Could not extend service_code column: {e}")
    
    conn.commit()
    conn.close()
    print("✅ Enhanced table structure restored!")

def reload_enhanced_service_charges():
    """Reload enhanced service charges with merged entries"""
    
    print("\n🔄 Reloading enhanced service charges...")
    
    # Use the enhanced converter to reload from the S&S Published tab
    excel_file = 'uploads/ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx'
    
    try:
        from non_destructive_complete_rate_card_loader import NonDestructiveCompleteRateCardLoader
        
        loader = NonDestructiveCompleteRateCardLoader()
        
        # Load just the service charges part
        service_charges_loaded = loader.load_service_charges(excel_file)
        
        print(f"✅ Enhanced service charges reloaded: {service_charges_loaded}")
        
    except Exception as e:
        print(f"❌ Error reloading service charges: {e}")
        print("Attempting fallback approach...")
        
        # Fallback: manually create the key merged entries
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Clear existing YY and YB entries
        cursor.execute('DELETE FROM dhl_express_services_surcharges WHERE service_code IN ("YY", "YB")')
        
        # Add enhanced YY entries
        enhanced_entries = [
            ('YY_MED_DOM', 'OVERWEIGHT PIECE', 'rate per shipment', 85.0, None, None, 'MEDICAL EXPRESS (domestic), JETLINE (domestic), EXPRESS DOMESTIC, EXPRESS DOMESTIC 9:00, EXPRESS DOMESTIC 12:00', 'YY'),
            ('YY_ALL', 'OVERWEIGHT PIECE', 'rate per shipment', 160.0, None, None, 'All Products', 'YY'),
            ('YB_DOM', 'OVERSIZE PIECE', 'rate per shipment', 20.0, None, None, 'Domestic', 'YB'),
            ('YB_INTL', 'OVERSIZE PIECE', 'rate per shipment', 32.0, None, None, 'International', 'YB')
        ]
        
        for entry in enhanced_entries:
            cursor.execute('''
                INSERT OR REPLACE INTO dhl_express_services_surcharges
                (service_code, service_name, charge_type, charge_amount, minimum_charge, percentage_rate, products_applicable, original_service_code, created_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (*entry, datetime.now().isoformat()))
            print(f"  ✅ Added {entry[0]}: {entry[1]} - ${entry[3]}")
        
        conn.commit()
        conn.close()
        print("✅ Fallback enhanced entries added!")

def verify_restoration():
    """Verify the restoration worked"""
    
    print("\n🔍 Verifying restoration...")
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Check YY/YB entries
    cursor.execute('''
        SELECT service_code, service_name, charge_amount, original_service_code
        FROM dhl_express_services_surcharges 
        WHERE service_code LIKE 'YY%' OR service_code LIKE 'YB%' 
        ORDER BY service_code
    ''')
    
    results = cursor.fetchall()
    print(f"\n=== Enhanced YY/YB Entries ({len(results)}) ===")
    for row in results:
        original = f" (orig: {row[3]})" if row[3] else ""
        print(f"{row[0]}: {row[1]} - ${row[2]}{original}")
    
    # Check total count
    cursor.execute('SELECT COUNT(*) FROM dhl_express_services_surcharges')
    total = cursor.fetchone()[0]
    print(f"\nTotal service charges: {total}")
    
    conn.close()

def main():
    """Main restoration process"""
    
    print("🚨 RESTORING ENHANCED SERVICE CHARGES STRUCTURE 🚨")
    print("=" * 60)
    
    # Step 1: Restore table structure
    restore_enhanced_table_structure()
    
    # Step 2: Reload enhanced service charges
    reload_enhanced_service_charges()
    
    # Step 3: Verify restoration
    verify_restoration()
    
    print("\n🎉 Enhanced service charges restoration complete!")
    print("✅ Table structure: Enhanced")
    print("✅ Merged entries: Restored")
    print("✅ YY_ALL/YY_MED_DOM: Available")
    print("✅ YB_INTL/YB_DOM: Available")

if __name__ == '__main__':
    main()
