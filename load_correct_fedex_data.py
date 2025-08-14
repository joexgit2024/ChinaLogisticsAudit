"""
Correct FedEx Rate Card Test Data Loader

This script creates test data matching the actual database schema.
"""

import sqlite3
from datetime import datetime, date

def load_correct_test_data():
    """Load test data into FedEx rate card tables with correct schema"""
    conn = sqlite3.connect('fedex_audit.db')
    cursor = conn.cursor()
    
    try:
        # Regions
        regions_data = [
            ('US', 'United States', 'US domestic region', True),
            ('CA', 'Canada', 'Canadian region', True),
            ('EU', 'Europe', 'European region', True),
            ('AP', 'Asia Pacific', 'Asia Pacific region', True)
        ]
        
        for region_code, region_name, description, active in regions_data:
            cursor.execute('''
                INSERT OR IGNORE INTO fedex_zone_regions 
                (region_code, region_name, description, active) 
                VALUES (?, ?, ?, ?)
            ''', (region_code, region_name, description, active))
        
        # Zone matrix
        zone_matrix_data = [
            ('US', 'US', 'A', 'EXPRESS'),
            ('US', 'CA', 'B', 'EXPRESS'),
            ('US', 'EU', 'C', 'EXPRESS'),
            ('US', 'AP', 'D', 'EXPRESS')
        ]
        
        for origin, dest, zone, service in zone_matrix_data:
            cursor.execute('''
                INSERT OR IGNORE INTO fedex_zone_matrix 
                (origin_region, destination_region, zone_code, service_type) 
                VALUES (?, ?, ?, ?)
            ''', (origin, dest, zone, service))
        
        # Country zones
        country_data = [
            ('US', 'United States', 'US', None, 'A', 'USD', 1.0, True),
            ('CA', 'Canada', 'CA', None, 'B', 'CAD', 0.75, True),
            ('GB', 'United Kingdom', 'EU', 'Western Europe', 'C', 'GBP', 1.25, True),
            ('JP', 'Japan', 'AP', 'East Asia', 'D', 'JPY', 0.007, True)
        ]
        
        for country_code, country_name, region_code, sub_region, zone_letter, currency_code, exchange_rate, active in country_data:
            cursor.execute('''
                INSERT OR IGNORE INTO fedex_country_zones 
                (country_code, country_name, region_code, sub_region, zone_letter, currency_code, exchange_rate, active) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (country_code, country_name, region_code, sub_region, zone_letter, currency_code, exchange_rate, active))
        
        # Service types
        service_data = [
            ('FX', 'FedEx Express', 'Express delivery service', True, False, 1, True),
            ('FE', 'FedEx Economy', 'Economy delivery service', False, True, 3, True),
            ('FP', 'FedEx Priority', 'Priority delivery service', True, False, 2, True),
            ('FI', 'FedEx International', 'International delivery service', True, False, 2, True)
        ]
        
        for service_code, service_name, description, is_express, is_economy, priority, active in service_data:
            cursor.execute('''
                INSERT OR IGNORE INTO fedex_service_types 
                (service_code, service_name, service_description, is_express, is_economy, priority_level, active) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (service_code, service_name, description, is_express, is_economy, priority, active))
        
        # Rate cards
        rate_data = [
            ('US Domestic Express', 'EXPRESS', 'US', 'US', 'A', 0.0, 1.0, 25.50, 'BASE', date.today(), None),
            ('US Domestic Express', 'EXPRESS', 'US', 'US', 'A', 1.0, 5.0, 35.75, 'BASE', date.today(), None),
            ('US to Canada Express', 'EXPRESS', 'US', 'CA', 'B', 0.0, 1.0, 45.90, 'BASE', date.today(), None),
            ('US to Canada Express', 'EXPRESS', 'US', 'CA', 'B', 1.0, 5.0, 65.25, 'BASE', date.today(), None),
            ('US to Europe Express', 'EXPRESS', 'US', 'EU', 'C', 0.0, 1.0, 75.00, 'BASE', date.today(), None),
            ('US to Europe Express', 'EXPRESS', 'US', 'EU', 'C', 1.0, 5.0, 95.50, 'BASE', date.today(), None),
            ('US to Asia Pacific Express', 'EXPRESS', 'US', 'AP', 'D', 0.0, 1.0, 85.00, 'BASE', date.today(), None),
            ('US to Asia Pacific Express', 'EXPRESS', 'US', 'AP', 'D', 1.0, 5.0, 110.75, 'BASE', date.today(), None)
        ]
        
        for rate_card_name, service_type, origin, dest, zone, weight_from, weight_to, rate, rate_type, effective_date, expiry_date in rate_data:
            cursor.execute('''
                INSERT OR IGNORE INTO fedex_rate_cards 
                (rate_card_name, service_type, origin_region, destination_region, zone_code, weight_from, weight_to, rate_usd, rate_type, effective_date, expiry_date) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (rate_card_name, service_type, origin, dest, zone, weight_from, weight_to, rate, rate_type, effective_date, expiry_date))
        
        # Surcharges
        surcharge_data = [
            ('FUEL', 'Fuel Surcharge', 'Variable fuel surcharge based on current fuel prices', 'PERCENTAGE', 15.5, None, None, 'ALL', 'ALL', 'ALL', None, True, date.today(), None),
            ('RES', 'Residential Delivery', 'Additional charge for residential delivery', 'FLAT', 5.20, None, None, 'ALL', 'ALL', 'ALL', None, True, date.today(), None),
            ('DAS', 'Delivery Area Surcharge', 'Extended delivery area surcharge', 'FLAT', 6.50, None, None, 'ALL', 'ALL', 'ALL', None, True, date.today(), None),
            ('OS', 'Oversize Surcharge', 'Surcharge for oversized packages', 'FLAT', 85.00, None, None, 'ALL', 'ALL', 'ALL', 25.0, True, date.today(), None),
            ('COD', 'Cash on Delivery', 'Cash on delivery service fee', 'PERCENTAGE', 2.5, 15.00, 150.00, 'EXPRESS', 'ALL', 'ALL', None, True, date.today(), None)
        ]
        
        for surcharge_code, surcharge_name, description, rate_type, rate_value, min_charge, max_charge, applies_to, origin_regions, dest_regions, weight_threshold, active, effective_date, expiry_date in surcharge_data:
            cursor.execute('''
                INSERT OR IGNORE INTO fedex_surcharges 
                (surcharge_code, surcharge_name, surcharge_description, rate_type, rate_value, minimum_charge, maximum_charge, applies_to_service, origin_regions, destination_regions, weight_threshold, active, effective_date, expiry_date) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (surcharge_code, surcharge_name, description, rate_type, rate_value, min_charge, max_charge, applies_to, origin_regions, dest_regions, weight_threshold, active, effective_date, expiry_date))
        
        conn.commit()
        print("✅ FedEx test data loaded successfully!")
        
        # Show counts
        tables = [
            ('fedex_zone_regions', 'Regions'),
            ('fedex_zone_matrix', 'Zone Matrix'),
            ('fedex_country_zones', 'Country Zones'),
            ('fedex_service_types', 'Service Types'),
            ('fedex_rate_cards', 'Rate Cards'),
            ('fedex_surcharges', 'Surcharges')
        ]
        
        for table, label in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  • {label}: {count} records")
            
    except Exception as e:
        print(f"Error loading test data: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()

if __name__ == '__main__':
    load_correct_test_data()
