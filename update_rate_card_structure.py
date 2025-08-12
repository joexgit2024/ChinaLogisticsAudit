#!/usr/bin/env python3
"""
Update DHL Express Rate Card Table Structure
This script updates the rate card table to support both Export (9 zones) and Import (19 zones)
"""

import sqlite3

def update_rate_card_structure():
    """Update the rate card table structure to support 19 zones"""
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    print("Updating DHL Express Rate Card table structure...")
    
    try:
        # Drop the existing table
        print("1. Dropping existing dhl_express_rate_cards table...")
        cursor.execute('DROP TABLE IF EXISTS dhl_express_rate_cards')
        
        # Create new table with 19 zones
        print("2. Creating new table structure with 19 zones...")
        cursor.execute('''
            CREATE TABLE dhl_express_rate_cards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_type VARCHAR(20) NOT NULL,  -- 'Export', 'Import', '3rd Party'
                rate_section VARCHAR(50) NOT NULL,  -- 'Documents', 'Non-documents'
                weight_from DECIMAL(10,3) NOT NULL,
                weight_to DECIMAL(10,3),
                zone_1 DECIMAL(10,2),
                zone_2 DECIMAL(10,2),
                zone_3 DECIMAL(10,2),
                zone_4 DECIMAL(10,2),
                zone_5 DECIMAL(10,2),
                zone_6 DECIMAL(10,2),
                zone_7 DECIMAL(10,2),
                zone_8 DECIMAL(10,2),
                zone_9 DECIMAL(10,2),
                zone_10 DECIMAL(10,2),
                zone_11 DECIMAL(10,2),
                zone_12 DECIMAL(10,2),
                zone_13 DECIMAL(10,2),
                zone_14 DECIMAL(10,2),
                zone_15 DECIMAL(10,2),
                zone_16 DECIMAL(10,2),
                zone_17 DECIMAL(10,2),
                zone_18 DECIMAL(10,2),
                zone_19 DECIMAL(10,2),
                is_multiplier BOOLEAN DEFAULT FALSE,
                weight_range_from DECIMAL(10,3),
                weight_range_to DECIMAL(10,3),
                created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for better performance
        print("3. Creating indexes...")
        cursor.execute('''
            CREATE INDEX idx_rate_cards_service_section 
            ON dhl_express_rate_cards(service_type, rate_section)
        ''')
        
        cursor.execute('''
            CREATE INDEX idx_rate_cards_weight 
            ON dhl_express_rate_cards(weight_from, weight_to)
        ''')
        
        conn.commit()
        print("4. Table structure updated successfully!")
        
        # Verify the new structure
        print("\n5. Verifying new table structure:")
        cursor.execute('PRAGMA table_info(dhl_express_rate_cards)')
        columns = cursor.fetchall()
        
        print(f"   Total columns: {len(columns)}")
        zone_columns = [col for col in columns if col[1].startswith('zone_')]
        print(f"   Zone columns: {len(zone_columns)} (zone_1 to zone_{len(zone_columns)})")
        
        for col in zone_columns:
            print(f"   - {col[1]}")
            
        print("\nTable is now ready for rate card uploads!")
        print("- Export rate cards will use zones 1-9")
        print("- Import rate cards will use zones 1-19")
        
    except Exception as e:
        print(f"Error updating table structure: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()
    
    return True

if __name__ == "__main__":
    success = update_rate_card_structure()
    if success:
        print("\n✅ Rate card table structure updated successfully!")
        print("You can now reload your rate card files.")
    else:
        print("\n❌ Failed to update rate card table structure.")
