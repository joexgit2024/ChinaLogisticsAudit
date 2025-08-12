#!/usr/bin/env python3
"""
Service Charges Table Migration Script
====================================

Migrates the dhl_express_services_surcharges table to the enhanced structure
with support for merged entries, descriptions, and enhanced charge types.
"""

import sqlite3
from datetime import datetime

def migrate_service_charges_table():
    """Migrate dhl_express_services_surcharges table to enhanced structure"""
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    print("🔄 Migrating service charges table to enhanced structure...")
    
    try:
        # Check current table structure
        cursor.execute("PRAGMA table_info(dhl_express_services_surcharges)")
        current_columns = [column[1] for column in cursor.fetchall()]
        
        print(f"📋 Current columns: {current_columns}")
        
        # Define required columns for enhanced structure
        required_columns = {
            'description': 'TEXT',
            'charge_type': 'VARCHAR(50)',
            'charge_amount': 'DECIMAL(10,2)',
            'minimum_charge': 'DECIMAL(10,2)',
            'percentage_rate': 'DECIMAL(5,2)',
            'products_applicable': 'TEXT',
            'original_service_code': 'VARCHAR(10)'
        }
        
        # Add missing columns
        for column_name, column_type in required_columns.items():
            if column_name not in current_columns:
                print(f"➕ Adding column: {column_name} {column_type}")
                cursor.execute(f"ALTER TABLE dhl_express_services_surcharges ADD COLUMN {column_name} {column_type}")
        
        # Update existing records to have default charge_type if NULL
        cursor.execute("""
            UPDATE dhl_express_services_surcharges 
            SET charge_type = 'rate per shipment' 
            WHERE charge_type IS NULL OR charge_type = ''
        """)
        
        # Migrate flat_rate to charge_amount if flat_rate column exists
        if 'flat_rate' in current_columns and 'charge_amount' in required_columns:
            print("🔄 Migrating flat_rate data to charge_amount...")
            cursor.execute("""
                UPDATE dhl_express_services_surcharges 
                SET charge_amount = flat_rate 
                WHERE charge_amount IS NULL AND flat_rate IS NOT NULL
            """)
        
        conn.commit()
        
        # Verify final structure
        cursor.execute("PRAGMA table_info(dhl_express_services_surcharges)")
        final_columns = [column[1] for column in cursor.fetchall()]
        print(f"✅ Final columns: {final_columns}")
        
        # Check current record count
        cursor.execute("SELECT COUNT(*) FROM dhl_express_services_surcharges")
        record_count = cursor.fetchone()[0]
        print(f"📊 Current records: {record_count}")
        
        # Show sample records
        cursor.execute("SELECT service_code, service_name, charge_amount, charge_type FROM dhl_express_services_surcharges LIMIT 5")
        sample_records = cursor.fetchall()
        print(f"📋 Sample records:")
        for record in sample_records:
            print(f"  {record[0]}: {record[1]} - ${record[2]} ({record[3]})")
        
        conn.close()
        print("✅ Migration completed successfully!")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        conn.rollback()
        conn.close()

if __name__ == "__main__":
    migrate_service_charges_table()
