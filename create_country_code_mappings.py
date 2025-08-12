#!/usr/bin/env python3
"""
Create DHL Express Country Code Mappings
=======================================

This script creates a table to map city codes to country codes
for DHL Express zone mappings.
"""

import sqlite3

def create_country_code_mappings():
    """Create and populate dhl_express_country_codes table"""
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Create the table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dhl_express_country_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city_code TEXT,
            country_code TEXT,
            city_name TEXT,
            country_name TEXT
        )
    ''')
    
    # Add index for faster lookups
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_city_code ON dhl_express_country_codes(city_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_country_code ON dhl_express_country_codes(country_code)')
    
    # Check if we already have data
    cursor.execute('SELECT COUNT(*) FROM dhl_express_country_codes')
    count = cursor.fetchone()[0]
    
    if count == 0:
        print("Populating country code mappings...")
        
        # Add mappings - focusing on those needed for invoice MELIR00831127
        mappings = [
            # Format: city_code, country_code, city_name, country_name
            ('HARLINGEN', 'US', 'HARLINGEN', 'United States'),
            ('EINDHOVEN', 'NL', 'EINDHOVEN', 'Netherlands'),
            ('SYD. SYDNEY', 'AU', 'SYDNEY', 'Australia'),
            ('AMSTERDAM', 'NL', 'AMSTERDAM', 'Netherlands'),
            ('FRANKFURT', 'DE', 'FRANKFURT', 'Germany'),
            ('LONDON', 'GB', 'LONDON', 'United Kingdom'),
            ('PARIS', 'FR', 'PARIS', 'France'),
            ('MADRID', 'ES', 'MADRID', 'Spain'),
            ('MILAN', 'IT', 'MILAN', 'Italy'),
            ('ZURICH', 'CH', 'ZURICH', 'Switzerland'),
            ('BRUSSELS', 'BE', 'BRUSSELS', 'Belgium'),
            ('HONG KONG', 'HK', 'HONG KONG', 'Hong Kong'),
            ('SINGAPORE', 'SG', 'SINGAPORE', 'Singapore'),
            ('TOKYO', 'JP', 'TOKYO', 'Japan'),
            ('SHANGHAI', 'CN', 'SHANGHAI', 'China'),
            ('SEOUL', 'KR', 'SEOUL', 'South Korea'),
            ('BANGKOK', 'TH', 'BANGKOK', 'Thailand'),
            ('NEW YORK', 'US', 'NEW YORK', 'United States'),
            ('LOS ANGELES', 'US', 'LOS ANGELES', 'United States'),
            ('CHICAGO', 'US', 'CHICAGO', 'United States'),
            ('MIAMI', 'US', 'MIAMI', 'United States'),
        ]
        
        cursor.executemany('''
            INSERT INTO dhl_express_country_codes
            (city_code, country_code, city_name, country_name)
            VALUES (?, ?, ?, ?)
        ''', mappings)
        
        conn.commit()
        print(f"Added {len(mappings)} country code mappings.")
    else:
        print(f"Country code mappings already exist ({count} records).")
    
    # Check US mappings specifically
    cursor.execute('''
        SELECT city_code, country_code, city_name, country_name
        FROM dhl_express_country_codes
        WHERE country_code = 'US'
    ''')
    
    us_cities = cursor.fetchall()
    print(f"\nUS city mappings:")
    for city in us_cities:
        print(f"  {city[0]} → {city[1]} ({city[3]})")
    
    conn.close()
    print("\nCountry code mappings setup complete!")

if __name__ == "__main__":
    create_country_code_mappings()
