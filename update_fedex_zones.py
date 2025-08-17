#!/usr/bin/env python3
"""
FedEx Zone Mapping Data Loader
Update zone mappings based on FedEx destination sub-regions
"""
import sqlite3

def create_fedex_zone_mappings():
    conn = sqlite3.connect('fedex_audit.db')
    cursor = conn.cursor()
    
    # Clear existing zone data
    cursor.execute('DELETE FROM fedex_country_zones')
    
    # FedEx Zone Mappings based on destination sub-regions
    zone_mappings = {
        'Asia One': ['HK', 'MO', 'MY', 'SG', 'TH', 'TW'],
        'Asia Two': ['AU', 'ID', 'NZ', 'PH', 'VN'],
        'Asia Other': ['AS', 'BN', 'CK', 'FJ', 'FM', 'KH', 'KI', 'LA', 'MH', 'MM', 'MN', 'MP', 'NC', 'NR', 'NU', 'PF', 'PG', 'PW', 'SB', 'TL', 'TO', 'TV', 'VU', 'WF', 'WS'],
        'NPAC': ['JP', 'KR'],
        'South America': ['AR', 'BO', 'BR', 'CL', 'CO', 'EC', 'GF', 'GY', 'KN', 'LC', 'PE', 'PY', 'SR', 'UY', 'VE'],
        'Central America': ['BZ', 'CR', 'GT', 'HN', 'NI', 'PA', 'SV'],
        'Caribbean': ['AG', 'AI', 'AN', 'AW', 'BB', 'BM', 'BS', 'DM', 'DO', 'GD', 'GP', 'HT', 'JM', 'KY', 'MQ', 'MS', 'TC', 'TT', 'VC', 'VG', 'VI'],
        'Europe One': ['AT', 'MC', 'VA', 'BE', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'LU', 'NL', 'PT'],
        'Europe Two': ['GL', 'DK', 'FI', 'FO', 'NO', 'SE', 'HU', 'CZ'],
        'Eastern Europe': ['AL', 'AM', 'AZ', 'BA', 'BG', 'BY', 'CS', 'EE', 'GE', 'HR', 'KG', 'KZ', 'LT', 'LV', 'MD', 'ME', 'MK', 'PL', 'RO', 'RS', 'RU', 'SI', 'SK', 'TM', 'UA', 'UZ'],
        'Europe Other': ['AD', 'GI', 'GR', 'IS', 'LI', 'MT', 'CH'],
        'Middle East': ['IL', 'PS', 'TR', 'AE', 'BH', 'CY', 'EG', 'IR', 'JO', 'KW', 'LB', 'OM', 'QA', 'SY', 'YE'],
        'India Subcontinent': ['BD', 'BT', 'IN', 'LK', 'MV', 'NP', 'PK'],
        'Africa': ['AO', 'BF', 'BI', 'BJ', 'BW', 'CD', 'CF', 'CG', 'CI', 'CM', 'CV', 'DJ', 'DZ', 'ER', 'ET', 'GA', 'GH', 'GM', 'GN', 'GQ', 'GW', 'KE', 'LR', 'LS', 'LY', 'MA', 'MG', 'ML', 'MR', 'MU', 'MW', 'MZ', 'NA', 'NE', 'NG', 'RE', 'RW', 'SC', 'SD', 'SL', 'SN', 'SO', 'SZ', 'TD', 'TG', 'TN', 'TZ', 'UG', 'ZA', 'ZM', 'ZW']
    }
    
    # Country names mapping (basic mapping for display purposes)
    country_names = {
        'HK': 'Hong Kong', 'MO': 'Macau', 'MY': 'Malaysia', 'SG': 'Singapore', 'TH': 'Thailand', 'TW': 'Taiwan',
        'AU': 'Australia', 'ID': 'Indonesia', 'NZ': 'New Zealand', 'PH': 'Philippines', 'VN': 'Vietnam',
        'JP': 'Japan', 'KR': 'South Korea', 'CN': 'China', 'US': 'United States',
        'GB': 'United Kingdom', 'DE': 'Germany', 'FR': 'France', 'IT': 'Italy', 'ES': 'Spain',
        'CA': 'Canada', 'BR': 'Brazil', 'AR': 'Argentina', 'IN': 'India', 'RU': 'Russia'
        # Add more as needed
    }
    
    # Assign zone letters for easier rate calculation
    zone_letters = {
        'Asia One': 'A1',
        'Asia Two': 'A2', 
        'Asia Other': 'A3',
        'NPAC': 'N1',
        'South America': 'S1',
        'Central America': 'C1',
        'Caribbean': 'C2',
        'Europe One': 'E1',
        'Europe Two': 'E2',
        'Eastern Europe': 'E3',
        'Europe Other': 'E4',
        'Middle East': 'M1',
        'India Subcontinent': 'I1',
        'Africa': 'F1'
    }
    
    # Insert zone mappings
    total_inserted = 0
    for region, countries in zone_mappings.items():
        zone_letter = zone_letters[region]
        for country_code in countries:
            country_name = country_names.get(country_code, country_code)
            
            cursor.execute('''
                INSERT INTO fedex_country_zones 
                (country_code, country_name, region_code, sub_region, zone_letter, 
                 currency_code, exchange_rate, active, created_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ''', (country_code, country_name, region, region, zone_letter, 'USD', 1.0, 1))
            total_inserted += 1
    
    # Add China as origin (not in destination zones)
    cursor.execute('''
        INSERT INTO fedex_country_zones 
        (country_code, country_name, region_code, sub_region, zone_letter, 
         currency_code, exchange_rate, active, created_timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    ''', ('CN', 'China', 'Origin', 'China Origin', 'CN', 'CNY', 7.3, 1))
    total_inserted += 1
    
    print(f"Inserted {total_inserted} zone mappings")
    
    conn.commit()
    conn.close()
    print("FedEx zone mappings updated successfully!")

if __name__ == "__main__":
    create_fedex_zone_mappings()
