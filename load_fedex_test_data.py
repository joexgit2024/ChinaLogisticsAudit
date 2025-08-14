"""
FedEx Rate Card Test Data Loader

This script creates sample rate card data for testing the management interface.
"""

import sqlite3
from datetime import datetime, date

def load_test_data():
    """Load sample test data into FedEx rate card tables"""
    conn = sqlite3.connect('fedex_audit.db')
    cursor = conn.cursor()
    
    # Sample regions
    regions = [
        ('NA', 'North America', 'North American region', True, datetime.now()),
        ('EU', 'Europe', 'European region', True, datetime.now()),
        ('AP', 'Asia Pacific', 'Asia Pacific region', True, datetime.now()),
        ('LA', 'Latin America', 'Latin American region', True, datetime.now()),
        ('AF', 'Africa', 'African region', True, datetime.now()),
        ('ME', 'Middle East', 'Middle Eastern region', True, datetime.now())
    ]
    
    cursor.executemany('''
        INSERT OR REPLACE INTO fedex_zone_regions 
        (region_code, region_name, description, active, created_timestamp)
        VALUES (?, ?, ?, ?, ?)
    ''', regions)
    
    # Sample zones
    zones = [
        ('A', 'Zone A', 'Domestic and nearby countries', True, datetime.now()),
        ('B', 'Zone B', 'Regional countries', True, datetime.now()),
        ('C', 'Zone C', 'Distant regional countries', True, datetime.now()),
        ('D', 'Zone D', 'Far international countries', True, datetime.now()),
        ('E', 'Zone E', 'Remote international countries', True, datetime.now())
    ]
    
    cursor.executemany('''
        INSERT OR REPLACE INTO fedex_zone_matrix 
        (zone_letter, zone_name, description, active, created_at)
        VALUES (?, ?, ?, ?, ?)
    ''', zones)
    
    # Sample countries
    countries = [
        ('US', 'United States', 'NA', 'North America', 'North America', 'A', 'USD', 1.0, True, datetime.now()),
        ('CA', 'Canada', 'NA', 'North America', 'North America', 'A', 'CAD', 0.75, True, datetime.now()),
        ('GB', 'United Kingdom', 'EU', 'Europe', 'Western Europe', 'B', 'GBP', 1.25, True, datetime.now()),
        ('DE', 'Germany', 'EU', 'Europe', 'Western Europe', 'B', 'EUR', 1.10, True, datetime.now()),
        ('JP', 'Japan', 'AP', 'Asia Pacific', 'East Asia', 'C', 'JPY', 0.007, True, datetime.now()),
        ('AU', 'Australia', 'AP', 'Asia Pacific', 'Oceania', 'C', 'AUD', 0.68, True, datetime.now()),
        ('BR', 'Brazil', 'LA', 'Latin America', 'South America', 'D', 'BRL', 0.20, True, datetime.now()),
        ('IN', 'India', 'AP', 'Asia Pacific', 'South Asia', 'D', 'INR', 0.012, True, datetime.now()),
        ('ZA', 'South Africa', 'AF', 'Africa', 'Southern Africa', 'E', 'ZAR', 0.055, True, datetime.now()),
        ('EG', 'Egypt', 'AF', 'Africa', 'North Africa', 'E', 'EGP', 0.032, True, datetime.now())
    ]
    
    cursor.executemany('''
        INSERT OR REPLACE INTO fedex_country_zones 
        (country_code, country_name, region_code, region_name, sub_region, 
         zone_letter, currency_code, exchange_rate, active, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', countries)
    
    # Sample service types
    services = [
        ('IE', 'International Economy', 'Economy international shipping service', True, datetime.now()),
        ('IP', 'International Priority', 'Priority international shipping service', True, datetime.now()),
        ('IF', 'International First', 'Express international shipping service', True, datetime.now()),
        ('IDX', 'International Express', 'Fast express international service', True, datetime.now())
    ]
    
    cursor.executemany('''
        INSERT OR REPLACE INTO fedex_service_types 
        (service_code, service_name, description, active, created_at)
        VALUES (?, ?, ?, ?, ?)
    ''', services)
    
    # Sample rate cards
    rate_cards = []
    service_codes = ['IE', 'IP', 'IF', 'IDX']
    zone_letters = ['A', 'B', 'C', 'D', 'E']
    
    # Weight ranges and rates
    weight_ranges = [
        (0.0, 0.5, [25.00, 30.00, 35.00, 40.00, 45.00]),
        (0.5, 1.0, [28.00, 33.00, 38.00, 43.00, 48.00]),
        (1.0, 2.0, [32.00, 37.00, 42.00, 47.00, 52.00]),
        (2.0, 5.0, [40.00, 45.00, 50.00, 55.00, 60.00]),
        (5.0, 10.0, [55.00, 60.00, 65.00, 70.00, 75.00]),
        (10.0, 25.0, [75.00, 80.00, 85.00, 90.00, 95.00]),
        (25.0, None, [100.00, 105.00, 110.00, 115.00, 120.00])
    ]
    
    for service_idx, service_code in enumerate(service_codes):
        for zone_idx, zone_letter in enumerate(zone_letters):
            for weight_from, weight_to, rates in weight_ranges:
                base_rate = rates[zone_idx]
                # Adjust rate based on service type
                if service_code == 'IE':  # Economy - cheapest
                    rate = base_rate * 0.8
                elif service_code == 'IP':  # Priority - medium
                    rate = base_rate
                elif service_code == 'IF':  # First - expensive
                    rate = base_rate * 1.3
                elif service_code == 'IDX':  # Express - most expensive
                    rate = base_rate * 1.5
                
                rate_cards.append((
                    service_code, zone_letter, weight_from, weight_to,
                    'standard', rate, None, 'USD', True,
                    datetime.now(), datetime.now()
                ))
    
    cursor.executemany('''
        INSERT OR REPLACE INTO fedex_rate_cards 
        (service_code, zone_letter, weight_from, weight_to, rate_type, 
         rate, rate_per_kg, currency_code, active, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', rate_cards)
    
    # Sample surcharges
    surcharges = [
        ('FUEL', 'Fuel Surcharge', 'Variable fuel surcharge', 'fuel', None, 15.5, None, True, False, None, None, 'USD', True, date.today(), None, datetime.now()),
        ('RES', 'Residential Surcharge', 'Residential delivery surcharge', 'residential', None, 5.20, None, False, False, None, None, 'USD', True, date.today(), None, datetime.now()),
        ('DAS', 'Delivery Area Surcharge', 'Extended delivery area surcharge', 'delivery', None, 6.50, None, False, False, None, None, 'USD', True, date.today(), None, datetime.now()),
        ('RDA', 'Remote Delivery Area', 'Remote area delivery surcharge', 'delivery', None, 12.50, None, False, False, None, None, 'USD', True, date.today(), None, datetime.now()),
        ('OS', 'Oversize Surcharge', 'Oversize package surcharge', 'handling', None, 85.00, None, False, False, None, None, 'USD', True, date.today(), None, datetime.now()),
        ('OW', 'Overweight Surcharge', 'Overweight package surcharge', 'handling', None, 125.00, 2.50, False, True, None, None, 'USD', True, date.today(), None, datetime.now()),
        ('SIG', 'Signature Required', 'Signature required service', 'delivery', None, 8.00, None, False, False, None, None, 'USD', True, date.today(), None, datetime.now()),
        ('COD', 'Cash on Delivery', 'Cash on delivery service', 'delivery', None, 0.00, None, True, False, 15.00, 150.00, 'USD', True, date.today(), None, datetime.now())
    ]
    
    cursor.executemany('''
        INSERT OR REPLACE INTO fedex_surcharges 
        (surcharge_code, surcharge_name, description, surcharge_type, zone_letter,
         rate, rate_per_kg, is_percentage, weight_based, min_charge, max_charge,
         currency_code, active, effective_date, expiry_date, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', surcharges)
    
    conn.commit()
    conn.close()
    
    print("✅ Sample FedEx rate card data loaded successfully!")
    print(f"  • {len(regions)} regions")
    print(f"  • {len(zones)} zones") 
    print(f"  • {len(countries)} countries")
    print(f"  • {len(services)} service types")
    print(f"  • {len(rate_cards)} rate cards")
    print(f"  • {len(surcharges)} surcharges")

if __name__ == '__main__':
    load_test_data()
