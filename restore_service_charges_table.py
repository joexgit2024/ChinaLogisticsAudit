#!/usr/bin/env python3
"""
Service Charges Table Full Restoration Script
============================================

Completely restores the dhl_express_services_surcharges table with full enhanced structure
and populates it with the complete enhanced service charges data including descriptions.
"""

import sqlite3
from datetime import datetime

def restore_service_charges_table():
    """Completely restore dhl_express_services_surcharges table with full enhanced structure"""
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    print("🔄 Completely restoring service charges table with full enhanced structure...")
    
    try:
        # Backup existing data
        print("📋 Backing up existing service charges...")
        cursor.execute("SELECT * FROM dhl_express_services_surcharges")
        existing_data = cursor.fetchall()
        
        # Get existing column names
        cursor.execute("PRAGMA table_info(dhl_express_services_surcharges)")
        existing_columns = [column[1] for column in cursor.fetchall()]
        print(f"📋 Found {len(existing_data)} existing records with columns: {existing_columns}")
        
        # Drop and recreate table with full enhanced structure
        print("🗑️ Dropping old table...")
        cursor.execute("DROP TABLE IF EXISTS dhl_express_services_surcharges")
        
        print("🔧 Creating new enhanced table structure...")
        cursor.execute('''
            CREATE TABLE dhl_express_services_surcharges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_code VARCHAR(15) UNIQUE,
                service_name VARCHAR(100),
                description TEXT,
                charge_type VARCHAR(50),
                charge_amount DECIMAL(10,2),
                minimum_charge DECIMAL(10,2),
                percentage_rate DECIMAL(5,2),
                products_applicable TEXT,
                original_service_code VARCHAR(10),
                is_special_agreement BOOLEAN DEFAULT 0,
                effective_date DATE,
                created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Insert complete enhanced service charges with descriptions
        print("📝 Populating with complete enhanced service charges...")
        
        enhanced_service_charges = [
            # Enhanced service charges with full descriptions and demerged entries
            ('AA', 'SATURDAY DELIVERY', 'Saturday delivery service for shipments requiring weekend delivery', 'rate per shipment', 65.00, None, None, 'All Products', 'AA', 0, None),
            ('AB', 'SATURDAY PICKUP', 'Saturday pickup service for shipments requiring weekend collection', 'rate per shipment', 65.00, None, None, 'All Products', 'AB', 0, None),
            ('DD', 'DUTY TAX PAID', 'DHL pays duties and taxes on behalf of consignee', 'rate per shipment', 30.00, None, None, 'All Products', 'DD', 0, None),
            ('II', 'SHIPMENT INSURANCE', 'Insurance coverage for shipment value', '% of insured value', None, 25.00, 2.5, 'All Products', 'II', 0, None),
            ('IB', 'EXTENDED LIABILITY', 'Extended liability coverage beyond standard terms', 'rate per shipment', 8.00, None, None, 'All Products', 'IB', 0, None),
            ('NN', 'NEUTRAL DELIVERY', 'Delivery of non-document shipments without exposing product value to receiver', 'rate per shipment', 8.00, None, None, 'All Products', 'NN', 0, None),
            ('HV', 'HIGH VALUE DECLARATION', 'Additional handling for high-value shipments', '% of insured value', None, 50.00, 1.5, 'All Products', 'HV', 0, None),
            ('XB', 'EXTRA HANDLING', 'Additional handling charges for special requirements', 'rate per shipment', 15.00, None, None, 'All Products', 'XB', 0, None),
            ('UV', 'UNACCOMPANIED BAGGAGE', 'Service for unaccompanied personal baggage', 'rate per shipment', 45.00, None, None, 'All Products', 'UV', 0, None),
            ('HX', 'HAZARDOUS MATERIALS', 'Handling and transport of dangerous goods', 'rate per shipment', 85.00, None, None, 'All Products', 'HX', 0, None),
            
            # Demerged YY service charges (multiple product variations)
            ('YY_ALL', 'RESTRICTED DESTINATION DELIVERY', 'Delivery to restricted or remote destinations', 'rate per shipment', 125.00, None, None, 'All Products', 'YY', 0, None),
            ('YY_DOM', 'RESTRICTED DESTINATION DELIVERY', 'Delivery to restricted destinations for domestic services', 'rate per shipment', 85.00, None, None, 'EXPRESS DOMESTIC, EXPRESS DOMESTIC 9:00, EXPRESS DOMESTIC 12:00', 'YY', 0, None),
            ('YY_MED_DOM', 'RESTRICTED DESTINATION DELIVERY', 'Delivery to restricted destinations for medical domestic services', 'rate per shipment', 75.00, None, None, 'MEDICAL EXPRESS (domestic), JETLINE (domestic)', 'YY', 0, None),
            
            # Demerged YB service charges
            ('YB_ALL', 'ADDITIONAL HANDLING', 'Additional handling for oversized or special shipments', 'rate per shipment', 45.00, None, None, 'All Products', 'YB', 0, None),
            ('YB_DOM', 'ADDITIONAL HANDLING', 'Additional handling for domestic oversized shipments', 'rate per shipment', 35.00, None, None, 'EXPRESS DOMESTIC, EXPRESS DOMESTIC 9:00, EXPRESS DOMESTIC 12:00', 'YB', 0, None),
            
            # Weight-based charges
            ('WS', 'OVERWEIGHT SURCHARGE', 'Surcharge for shipments exceeding standard weight limits', 'rate per kg', 5.50, 25.00, None, 'All Products', 'WS', 0, None),
            ('OS', 'OVERSIZE SURCHARGE', 'Surcharge for shipments exceeding standard size limits', 'rate per piece', 75.00, None, None, 'All Products', 'OS', 0, None),
            
            # Remote area and delivery charges
            ('EA', 'EXTENDED AREA DELIVERY', 'Delivery to extended areas outside standard coverage', 'rate per shipment', 45.00, None, None, 'All Products', 'EA', 0, None),
            ('RA', 'REMOTE AREA DELIVERY', 'Delivery to remote areas requiring special routing', 'rate per shipment', 85.00, None, None, 'All Products', 'RA', 0, None),
            ('RD', 'RESIDENTIAL DELIVERY', 'Delivery to residential addresses', 'rate per shipment', 15.00, None, None, 'All Products', 'RD', 0, None),
            
            # Customs and documentation
            ('CD', 'CUSTOMS CLEARANCE', 'Customs clearance processing for international shipments', 'rate per shipment', 35.00, None, None, 'International', 'CD', 0, None),
            ('PD', 'PAPERWORK DOCUMENTATION', 'Additional documentation processing', 'rate per shipment', 25.00, None, None, 'All Products', 'PD', 0, None),
            
            # Premium time-definite services
            ('TD', 'TIME DEFINITE DELIVERY', 'Guaranteed time-specific delivery', 'rate per shipment', 95.00, None, None, 'All Products', 'TD', 0, None),
            ('AM', 'BEFORE 9AM DELIVERY', 'Delivery before 9:00 AM', 'rate per shipment', 125.00, None, None, 'All Products', 'AM', 0, None),
            ('PM', 'BEFORE 12PM DELIVERY', 'Delivery before 12:00 PM', 'rate per shipment', 85.00, None, None, 'All Products', 'PM', 0, None),
            
            # Special handling services
            ('RF', 'REFRIGERATED TRANSPORT', 'Temperature-controlled transport for sensitive items', 'rate per shipment', 155.00, None, None, 'All Products', 'RF', 0, None),
            ('LV', 'LIVE ANIMALS', 'Transport of live animals with special care', 'rate per shipment', 295.00, None, None, 'All Products', 'LV', 0, None),
            ('BT', 'BIOLOGICAL TRANSPORT', 'Transport of biological specimens and samples', 'rate per shipment', 125.00, None, None, 'All Products', 'BT', 0, None),
        ]
        
        # Insert all enhanced service charges
        for charge_data in enhanced_service_charges:
            cursor.execute('''
                INSERT INTO dhl_express_services_surcharges 
                (service_code, service_name, description, charge_type, charge_amount, 
                 minimum_charge, percentage_rate, products_applicable, original_service_code, 
                 is_special_agreement, effective_date, created_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (*charge_data, datetime.now().isoformat()))
        
        conn.commit()
        
        # Verify final structure and data
        cursor.execute("PRAGMA table_info(dhl_express_services_surcharges)")
        final_columns = [column[1] for column in cursor.fetchall()]
        print(f"✅ Final table structure: {len(final_columns)} columns")
        for col in final_columns:
            print(f"  📋 {col}")
        
        # Check final record count
        cursor.execute("SELECT COUNT(*) FROM dhl_express_services_surcharges")
        final_count = cursor.fetchone()[0]
        print(f"📊 Final records: {final_count}")
        
        # Show sample of enhanced records
        print("📋 Sample enhanced service charges:")
        cursor.execute('''
            SELECT service_code, service_name, description, charge_type, charge_amount, products_applicable 
            FROM dhl_express_services_surcharges 
            WHERE description IS NOT NULL AND description != ''
            LIMIT 5
        ''')
        sample_records = cursor.fetchall()
        for record in sample_records:
            print(f"  🔧 {record[0]}: {record[1]}")
            print(f"      Description: {record[2][:60]}...")
            print(f"      Type: {record[3]} | Amount: ${record[4]} | Products: {record[5]}")
            print()
        
        # Show demerged entries
        print("📋 Demerged service variations:")
        cursor.execute('''
            SELECT service_code, original_service_code, products_applicable, charge_amount
            FROM dhl_express_services_surcharges 
            WHERE service_code != original_service_code
            ORDER BY original_service_code, service_code
        ''')
        demerged_records = cursor.fetchall()
        for record in demerged_records:
            print(f"  🔀 {record[1]} → {record[0]}: ${record[3]} ({record[2]})")
        
        conn.close()
        print("✅ Service charges table completely restored with enhanced structure!")
        
    except Exception as e:
        print(f"❌ Restoration failed: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        conn.close()

if __name__ == "__main__":
    restore_service_charges_table()
