"""
Simple FedEx Rate Card Test Data Loader

This script creates minimal test data to demonstrate the management interface.
"""

import sqlite3
from datetime import datetime, date

def load_simple_test_data():
    """Load minimal test data into FedEx rate card tables"""
    conn = sqlite3.connect('fedex_audit.db')
    cursor = conn.cursor()
    
    try:
        # Simple regions
        cursor.execute('''
            INSERT OR IGNORE INTO fedex_zone_regions 
            (region_code, region_name, description, active) 
            VALUES (?, ?, ?, ?)
        ''', ('US', 'United States', 'US domestic region', 1))
        
        cursor.execute('''
            INSERT OR IGNORE INTO fedex_zone_regions 
            (region_code, region_name, description, active) 
            VALUES (?, ?, ?, ?)
        ''', ('INT', 'International', 'International regions', 1))
        
        # Simple zone matrix
        cursor.execute('''
            INSERT OR IGNORE INTO fedex_zone_matrix 
            (origin_region, destination_region, zone_code, service_type) 
            VALUES (?, ?, ?, ?)
        ''', ('US', 'US', 'A', 'EXPRESS'))
        
        cursor.execute('''
            INSERT OR IGNORE INTO fedex_zone_matrix 
            (origin_region, destination_region, zone_code, service_type) 
            VALUES (?, ?, ?, ?)
        ''', ('US', 'INT', 'B', 'EXPRESS'))
        
        # Simple country zones
        cursor.execute('''
            INSERT OR IGNORE INTO fedex_country_zones 
            (country_code, country_name, region_code, zone_letter, currency_code, exchange_rate, active) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', ('US', 'United States', 'US', 'A', 'USD', 1.0, 1))
        
        cursor.execute('''
            INSERT OR IGNORE INTO fedex_country_zones 
            (country_code, country_name, region_code, zone_letter, currency_code, exchange_rate, active) 
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', ('CA', 'Canada', 'INT', 'B', 'CAD', 0.75, 1))
        
        # Simple rate cards
        cursor.execute('''
            INSERT OR IGNORE INTO fedex_rate_cards 
            (service_code, zone_letter, weight_from, weight_to, rate_type, rate, currency_code, active) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', ('EXPRESS', 'A', 0.0, 1.0, 'standard', 25.50, 'USD', 1))
        
        cursor.execute('''
            INSERT OR IGNORE INTO fedex_rate_cards 
            (service_code, zone_letter, weight_from, weight_to, rate_type, rate, currency_code, active) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', ('EXPRESS', 'B', 0.0, 1.0, 'standard', 45.75, 'USD', 1))
        
        # Simple service types
        cursor.execute('''
            INSERT OR IGNORE INTO fedex_service_types 
            (service_code, service_name, description, active) 
            VALUES (?, ?, ?, ?)
        ''', ('EXPRESS', 'FedEx Express', 'Express delivery service', 1))
        
        # Simple surcharges
        cursor.execute('''
            INSERT OR IGNORE INTO fedex_surcharges 
            (surcharge_code, surcharge_name, description, surcharge_type, rate, is_percentage, currency_code, active, effective_date) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', ('FUEL', 'Fuel Surcharge', 'Variable fuel surcharge', 'fuel', 15.5, 1, 'USD', 1, date.today()))
        
        cursor.execute('''
            INSERT OR IGNORE INTO fedex_surcharges 
            (surcharge_code, surcharge_name, description, surcharge_type, rate, is_percentage, currency_code, active, effective_date) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', ('RES', 'Residential Surcharge', 'Residential delivery surcharge', 'residential', 5.20, 0, 'USD', 1, date.today()))
        
        conn.commit()
        print("✅ Simple FedEx test data loaded successfully!")
        
        # Show counts
        tables = ['fedex_zone_regions', 'fedex_zone_matrix', 'fedex_country_zones', 
                 'fedex_rate_cards', 'fedex_service_types', 'fedex_surcharges']
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  • {table}: {count} records")
            
    except Exception as e:
        print(f"Error loading test data: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == '__main__':
    load_simple_test_data()
