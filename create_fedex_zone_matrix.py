#!/usr/bin/env python3
"""
FedEx Zone Matrix Loader
Populate the complete FedEx zone mapping matrix
"""
import sqlite3

def create_fedex_zone_matrix():
    conn = sqlite3.connect('fedex_audit.db')
    cursor = conn.cursor()
    
    # Drop and recreate the zone matrix table with correct structure
    cursor.execute('DROP TABLE IF EXISTS fedex_zone_matrix')
    
    # Create the zone matrix table
    cursor.execute('''
        CREATE TABLE fedex_zone_matrix (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin_country TEXT NOT NULL,
            destination_region TEXT NOT NULL,
            zone_letter TEXT NOT NULL,
            active INTEGER DEFAULT 1,
            created_timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(origin_country, destination_region)
        )
    ''')
    
    # Define the destination regions and their countries
    destination_regions = {
        'Africa': ['AO', 'BF', 'BI', 'BJ', 'BW', 'CD', 'CF', 'CG', 'CI', 'CM', 'CV', 'DJ', 'DZ', 'ER', 'ET', 'GA', 'GH', 'GM', 'GN', 'GQ', 'GW', 'KE', 'LR', 'LS', 'LY', 'MA', 'MG', 'ML', 'MR', 'MU', 'MW', 'MZ', 'NA', 'NE', 'NG', 'RE', 'RW', 'SC', 'SD', 'SL', 'SN', 'SO', 'SZ', 'TD', 'TG', 'TN', 'TZ', 'UG', 'ZA', 'ZM', 'ZW'],
        'Asia One': ['HK', 'MO', 'MY', 'SG', 'TH', 'TW'],
        'Asia Two': ['AU', 'ID', 'NZ', 'PH', 'VN'],
        'Asia Other': ['AS', 'BN', 'CK', 'FJ', 'FM', 'KH', 'KI', 'LA', 'MH', 'MM', 'MN', 'MP', 'NC', 'NR', 'NU', 'PF', 'PG', 'PW', 'SB', 'TL', 'TO', 'TV', 'VU', 'WF', 'WS'],
        'Canada': ['CA'],
        'Caribbean': ['AG', 'AI', 'AN', 'AW', 'BB', 'BM', 'BS', 'DM', 'DO', 'GD', 'GP', 'HT', 'JM', 'KY', 'MQ', 'MS', 'TC', 'TT', 'VC', 'VG', 'VI'],
        'China': ['CN'],
        'Central America': ['BZ', 'CR', 'GT', 'HN', 'NI', 'PA', 'SV'],
        'South America': ['AR', 'BO', 'BR', 'CL', 'CO', 'EC', 'GF', 'GY', 'KN', 'LC', 'PE', 'PY', 'SR', 'UY', 'VE'],
        'Europe One': ['AT', 'MC', 'VA', 'BE', 'DE', 'ES', 'FR', 'GB', 'IE', 'IT', 'LU', 'NL', 'PT'],
        'Europe Two': ['GL', 'DK', 'FI', 'FO', 'NO', 'SE', 'HU', 'CZ'],
        'Europe Other': ['AD', 'GI', 'GR', 'IS', 'LI', 'MT', 'CH'],
        'Eastern Europe': ['AL', 'AM', 'AZ', 'BA', 'BG', 'BY', 'CS', 'EE', 'GE', 'HR', 'KG', 'KZ', 'LT', 'LV', 'MD', 'ME', 'MK', 'PL', 'RO', 'RS', 'RU', 'SI', 'SK', 'TM', 'UA', 'UZ'],
        'India Sub.': ['BD', 'BT', 'IN', 'LK', 'MV', 'NP', 'PK'],
        'Middle East': ['IL', 'PS', 'TR', 'AE', 'BH', 'CY', 'EG', 'IR', 'JO', 'KW', 'LB', 'OM', 'QA', 'SY', 'YE'],
        'Mexico': ['MX'],
        'NPAC': ['JP', 'KR'],
        'US, AK, HI, PR': ['US', 'AK', 'HI', 'PR'],
        'IQ, AF, SA': ['IQ', 'AF', 'SA']
    }
    
    # Zone mapping data from the matrix
    zone_mapping = {
        'Argentina': {'Africa': 'M', 'Asia One': 'K', 'Asia Two': 'K', 'Asia Other': 'M', 'Canada': 'I', 'Caribbean': 'I', 'China': 'K', 'Central America': 'I', 'South America': 'H', 'Europe One': 'K', 'Europe Two': 'K', 'Europe Other': 'K', 'Eastern Europe': 'L', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'J', 'NPAC': 'K', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'M'},
        'Australia': {'Africa': 'M', 'Asia One': 'B', 'Asia Two': 'B', 'Asia Other': 'G', 'Canada': 'K', 'Caribbean': 'L', 'China': 'B', 'Central America': 'L', 'South America': 'L', 'Europe One': 'K', 'Europe Two': 'K', 'Europe Other': 'L', 'Eastern Europe': 'L', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'K', 'NPAC': 'D', 'US, AK, HI, PR': 'J', 'IQ, AF, SA': 'M'},
        'Austria': {'Africa': 'N', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'J', 'Caribbean': 'M', 'China': 'L', 'Central America': 'M', 'South America': 'M', 'Europe One': 'O', 'Europe Two': 'O', 'Europe Other': 'O', 'Eastern Europe': 'P', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'L', 'NPAC': 'L', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'N'},
        'Barbados': {'Africa': 'M', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'H', 'Caribbean': 'F', 'China': 'L', 'Central America': 'I', 'South America': 'I', 'Europe One': 'L', 'Europe Two': 'L', 'Europe Other': 'L', 'Eastern Europe': 'L', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'J', 'NPAC': 'L', 'US, AK, HI, PR': 'H', 'IQ, AF, SA': 'M'},
        'Belgium': {'Africa': 'N', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'J', 'Caribbean': 'M', 'China': 'L', 'Central America': 'M', 'South America': 'M', 'Europe One': 'O', 'Europe Two': 'O', 'Europe Other': 'O', 'Eastern Europe': 'P', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'L', 'NPAC': 'L', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'N'},
        'Brazil': {'Africa': 'M', 'Asia One': 'K', 'Asia Two': 'K', 'Asia Other': 'M', 'Canada': 'H', 'Caribbean': 'H', 'China': 'K', 'Central America': 'I', 'South America': 'H', 'Europe One': 'J', 'Europe Two': 'K', 'Europe Other': 'K', 'Eastern Europe': 'L', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'I', 'NPAC': 'K', 'US, AK, HI, PR': 'H', 'IQ, AF, SA': 'M'},
        'Canada': {'Africa': 'M', 'Asia One': 'K', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'N/A', 'Caribbean': 'K', 'China': 'L', 'Central America': 'M', 'South America': 'M', 'Europe One': 'K', 'Europe Two': 'K', 'Europe Other': 'L', 'Eastern Europe': 'M', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'K', 'NPAC': 'L', 'US, AK, HI, PR': 'G', 'IQ, AF, SA': 'M'},
        'Chile': {'Africa': 'M', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'I', 'Caribbean': 'I', 'China': 'L', 'Central America': 'I', 'South America': 'H', 'Europe One': 'K', 'Europe Two': 'K', 'Europe Other': 'K', 'Eastern Europe': 'M', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'J', 'NPAC': 'L', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'L'},
        'China': {'Africa': 'M', 'Asia One': 'E', 'Asia Two': 'F', 'Asia Other': 'G', 'Canada': 'K', 'Caribbean': 'L', 'China': 'N/A', 'Central America': 'L', 'South America': 'L', 'Europe One': 'J', 'Europe Two': 'J', 'Europe Other': 'K', 'Eastern Europe': 'L', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'K', 'NPAC': 'E', 'US, AK, HI, PR': 'J', 'IQ, AF, SA': 'M'},
        'Colombia': {'Africa': 'M', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'G', 'Caribbean': 'I', 'China': 'L', 'Central America': 'H', 'South America': 'H', 'Europe One': 'K', 'Europe Two': 'L', 'Europe Other': 'K', 'Eastern Europe': 'L', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'H', 'NPAC': 'L', 'US, AK, HI, PR': 'G', 'IQ, AF, SA': 'M'},
        'Costa Rica': {'Africa': 'M', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'H', 'Caribbean': 'I', 'China': 'L', 'Central America': 'H', 'South America': 'I', 'Europe One': 'K', 'Europe Two': 'L', 'Europe Other': 'K', 'Eastern Europe': 'L', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'I', 'NPAC': 'L', 'US, AK, HI, PR': 'H', 'IQ, AF, SA': 'M'},
        'Denmark': {'Africa': 'N', 'Asia One': 'M', 'Asia Two': 'M', 'Asia Other': 'M', 'Canada': 'L', 'Caribbean': 'N', 'China': 'L', 'Central America': 'N', 'South America': 'N', 'Europe One': 'P', 'Europe Two': 'P', 'Europe Other': 'P', 'Eastern Europe': 'P', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'M', 'NPAC': 'M', 'US, AK, HI, PR': 'L', 'IQ, AF, SA': 'N'},
        'Dominican Republic': {'Africa': 'M', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'G', 'Caribbean': 'F', 'China': 'L', 'Central America': 'I', 'South America': 'I', 'Europe One': 'K', 'Europe Two': 'K', 'Europe Other': 'K', 'Eastern Europe': 'L', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'H', 'NPAC': 'L', 'US, AK, HI, PR': 'F', 'IQ, AF, SA': 'H'},
        'Finland': {'Africa': 'N', 'Asia One': 'M', 'Asia Two': 'M', 'Asia Other': 'N', 'Canada': 'L', 'Caribbean': 'N', 'China': 'L', 'Central America': 'N', 'South America': 'N', 'Europe One': 'P', 'Europe Two': 'P', 'Europe Other': 'P', 'Eastern Europe': 'P', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'M', 'NPAC': 'M', 'US, AK, HI, PR': 'L', 'IQ, AF, SA': 'N'},
        'France': {'Africa': 'N', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'J', 'Caribbean': 'M', 'China': 'L', 'Central America': 'M', 'South America': 'M', 'Europe One': 'O', 'Europe Two': 'O', 'Europe Other': 'O', 'Eastern Europe': 'P', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'L', 'NPAC': 'L', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'N'},
        'Germany': {'Africa': 'N', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'J', 'Caribbean': 'M', 'China': 'L', 'Central America': 'M', 'South America': 'M', 'Europe One': 'O', 'Europe Two': 'O', 'Europe Other': 'O', 'Eastern Europe': 'P', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'L', 'NPAC': 'L', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'N'},
        'Great Britain (U.K.)': {'Africa': 'N', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'J', 'Caribbean': 'M', 'China': 'L', 'Central America': 'M', 'South America': 'M', 'Europe One': 'O', 'Europe Two': 'O', 'Europe Other': 'O', 'Eastern Europe': 'P', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'L', 'NPAC': 'L', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'N'},
        'Guatemala': {'Africa': 'M', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'I', 'Caribbean': 'I', 'China': 'L', 'Central America': 'H', 'South America': 'I', 'Europe One': 'K', 'Europe Two': 'L', 'Europe Other': 'K', 'Eastern Europe': 'M', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'I', 'NPAC': 'L', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'M'},
        'Hong Kong': {'Africa': 'M', 'Asia One': 'A', 'Asia Two': 'B', 'Asia Other': 'G', 'Canada': 'I', 'Caribbean': 'L', 'China': 'A', 'Central America': 'L', 'South America': 'L', 'Europe One': 'I', 'Europe Two': 'I', 'Europe Other': 'J', 'Eastern Europe': 'L', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'I', 'NPAC': 'B', 'US, AK, HI, PR': 'H', 'IQ, AF, SA': 'M'},
        'India': {'Africa': 'M', 'Asia One': 'G', 'Asia Two': 'H', 'Asia Other': 'K', 'Canada': 'K', 'Caribbean': 'L', 'China': 'G', 'Central America': 'L', 'South America': 'L', 'Europe One': 'H', 'Europe Two': 'I', 'Europe Other': 'I', 'Eastern Europe': 'L', 'India Sub.': 'E', 'Middle East': 'F', 'Mexico': 'L', 'NPAC': 'H', 'US, AK, HI, PR': 'J', 'IQ, AF, SA': 'M'},
        'Indonesia': {'Africa': 'M', 'Asia One': 'F', 'Asia Two': 'G', 'Asia Other': 'G', 'Canada': 'K', 'Caribbean': 'L', 'China': 'F', 'Central America': 'L', 'South America': 'L', 'Europe One': 'K', 'Europe Two': 'K', 'Europe Other': 'L', 'Eastern Europe': 'L', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'K', 'NPAC': 'F', 'US, AK, HI, PR': 'J', 'IQ, AF, SA': 'M'},
        'Ireland': {'Africa': 'N', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'J', 'Caribbean': 'M', 'China': 'L', 'Central America': 'M', 'South America': 'M', 'Europe One': 'O', 'Europe Two': 'O', 'Europe Other': 'O', 'Eastern Europe': 'P', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'L', 'NPAC': 'L', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'N'},
        'Italy': {'Africa': 'N', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'J', 'Caribbean': 'M', 'China': 'L', 'Central America': 'M', 'South America': 'M', 'Europe One': 'O', 'Europe Two': 'O', 'Europe Other': 'O', 'Eastern Europe': 'P', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'L', 'NPAC': 'L', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'N'},
        'Jamaica': {'Africa': 'M', 'Asia One': 'L', 'Asia Two': 'M', 'Asia Other': 'M', 'Canada': 'H', 'Caribbean': 'F', 'China': 'L', 'Central America': 'I', 'South America': 'I', 'Europe One': 'L', 'Europe Two': 'M', 'Europe Other': 'L', 'Eastern Europe': 'L', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'J', 'NPAC': 'L', 'US, AK, HI, PR': 'H', 'IQ, AF, SA': 'M'},
        'Japan': {'Africa': 'M', 'Asia One': 'B', 'Asia Two': 'C', 'Asia Other': 'G', 'Canada': 'I', 'Caribbean': 'L', 'China': 'B', 'Central America': 'L', 'South America': 'L', 'Europe One': 'J', 'Europe Two': 'J', 'Europe Other': 'K', 'Eastern Europe': 'L', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'I', 'NPAC': 'B', 'US, AK, HI, PR': 'H', 'IQ, AF, SA': 'M'},
        'Luxembourg': {'Africa': 'N', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'J', 'Caribbean': 'M', 'China': 'L', 'Central America': 'M', 'South America': 'M', 'Europe One': 'F', 'Europe Two': 'G', 'Europe Other': 'H', 'Eastern Europe': 'K', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'L', 'NPAC': 'L', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'N'},
        'Malaysia': {'Africa': 'M', 'Asia One': 'A', 'Asia Two': 'B', 'Asia Other': 'G', 'Canada': 'J', 'Caribbean': 'L', 'China': 'A', 'Central America': 'L', 'South America': 'L', 'Europe One': 'K', 'Europe Two': 'K', 'Europe Other': 'L', 'Eastern Europe': 'L', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'J', 'NPAC': 'B', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'M'},
        'Mexico': {'Africa': 'M', 'Asia One': 'J', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'D', 'Caribbean': 'G', 'China': 'J', 'Central America': 'H', 'South America': 'H', 'Europe One': 'I', 'Europe Two': 'J', 'Europe Other': 'I', 'Eastern Europe': 'K', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'N/A', 'NPAC': 'J', 'US, AK, HI, PR': 'D', 'IQ, AF, SA': 'M'},
        'Netherlands': {'Africa': 'N', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'J', 'Caribbean': 'M', 'China': 'L', 'Central America': 'M', 'South America': 'M', 'Europe One': 'O', 'Europe Two': 'O', 'Europe Other': 'O', 'Eastern Europe': 'P', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'L', 'NPAC': 'L', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'N'},
        'New Zealand': {'Africa': 'M', 'Asia One': 'D', 'Asia Two': 'B', 'Asia Other': 'G', 'Canada': 'K', 'Caribbean': 'L', 'China': 'D', 'Central America': 'L', 'South America': 'L', 'Europe One': 'K', 'Europe Two': 'K', 'Europe Other': 'L', 'Eastern Europe': 'L', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'K', 'NPAC': 'E', 'US, AK, HI, PR': 'J', 'IQ, AF, SA': 'M'},
        'Norway': {'Africa': 'N', 'Asia One': 'M', 'Asia Two': 'M', 'Asia Other': 'N', 'Canada': 'L', 'Caribbean': 'N', 'China': 'L', 'Central America': 'M', 'South America': 'N', 'Europe One': 'P', 'Europe Two': 'P', 'Europe Other': 'P', 'Eastern Europe': 'P', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'M', 'NPAC': 'M', 'US, AK, HI, PR': 'L', 'IQ, AF, SA': 'N'},
        'Panama': {'Africa': 'M', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'H', 'Caribbean': 'I', 'China': 'L', 'Central America': 'H', 'South America': 'I', 'Europe One': 'K', 'Europe Two': 'L', 'Europe Other': 'K', 'Eastern Europe': 'L', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'I', 'NPAC': 'L', 'US, AK, HI, PR': 'H', 'IQ, AF, SA': 'M'},
        'Philippines': {'Africa': 'M', 'Asia One': 'A', 'Asia Two': 'B', 'Asia Other': 'G', 'Canada': 'K', 'Caribbean': 'L', 'China': 'A', 'Central America': 'L', 'South America': 'L', 'Europe One': 'K', 'Europe Two': 'K', 'Europe Other': 'L', 'Eastern Europe': 'L', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'K', 'NPAC': 'D', 'US, AK, HI, PR': 'J', 'IQ, AF, SA': 'M'},
        'Poland': {'Africa': 'N', 'Asia One': 'M', 'Asia Two': 'M', 'Asia Other': 'N', 'Canada': 'L', 'Caribbean': 'N', 'China': 'M', 'Central America': 'N', 'South America': 'N', 'Europe One': 'P', 'Europe Two': 'P', 'Europe Other': 'P', 'Eastern Europe': 'P', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'M', 'NPAC': 'M', 'US, AK, HI, PR': 'L', 'IQ, AF, SA': 'N'},
        'Singapore': {'Africa': 'M', 'Asia One': 'A', 'Asia Two': 'B', 'Asia Other': 'G', 'Canada': 'J', 'Caribbean': 'L', 'China': 'A', 'Central America': 'L', 'South America': 'L', 'Europe One': 'K', 'Europe Two': 'K', 'Europe Other': 'L', 'Eastern Europe': 'L', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'J', 'NPAC': 'A', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'M'},
        'South Korea': {'Africa': 'M', 'Asia One': 'B', 'Asia Two': 'B', 'Asia Other': 'G', 'Canada': 'I', 'Caribbean': 'L', 'China': 'B', 'Central America': 'L', 'South America': 'L', 'Europe One': 'K', 'Europe Two': 'K', 'Europe Other': 'L', 'Eastern Europe': 'L', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'I', 'NPAC': 'A', 'US, AK, HI, PR': 'H', 'IQ, AF, SA': 'M'},
        'Spain': {'Africa': 'N', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'J', 'Caribbean': 'M', 'China': 'L', 'Central America': 'M', 'South America': 'M', 'Europe One': 'O', 'Europe Two': 'O', 'Europe Other': 'O', 'Eastern Europe': 'P', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'L', 'NPAC': 'L', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'N'},
        'Sweden': {'Africa': 'N', 'Asia One': 'M', 'Asia Two': 'M', 'Asia Other': 'M', 'Canada': 'L', 'Caribbean': 'N', 'China': 'L', 'Central America': 'N', 'South America': 'N', 'Europe One': 'P', 'Europe Two': 'P', 'Europe Other': 'P', 'Eastern Europe': 'P', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'M', 'NPAC': 'M', 'US, AK, HI, PR': 'L', 'IQ, AF, SA': 'N'},
        'Switzerland': {'Africa': 'N', 'Asia One': 'M', 'Asia Two': 'M', 'Asia Other': 'N', 'Canada': 'L', 'Caribbean': 'N', 'China': 'L', 'Central America': 'N', 'South America': 'N', 'Europe One': 'P', 'Europe Two': 'P', 'Europe Other': 'P', 'Eastern Europe': 'P', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'M', 'NPAC': 'M', 'US, AK, HI, PR': 'L', 'IQ, AF, SA': 'N'},
        'Taiwan': {'Africa': 'M', 'Asia One': 'A', 'Asia Two': 'B', 'Asia Other': 'G', 'Canada': 'I', 'Caribbean': 'L', 'China': 'A', 'Central America': 'L', 'South America': 'L', 'Europe One': 'K', 'Europe Two': 'K', 'Europe Other': 'L', 'Eastern Europe': 'L', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'I', 'NPAC': 'A', 'US, AK, HI, PR': 'H', 'IQ, AF, SA': 'M'},
        'Thailand': {'Africa': 'M', 'Asia One': 'A', 'Asia Two': 'B', 'Asia Other': 'G', 'Canada': 'J', 'Caribbean': 'L', 'China': 'A', 'Central America': 'L', 'South America': 'L', 'Europe One': 'K', 'Europe Two': 'K', 'Europe Other': 'L', 'Eastern Europe': 'L', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'J', 'NPAC': 'B', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'M'},
        'United Arab Emirates': {'Africa': 'M', 'Asia One': 'G', 'Asia Two': 'H', 'Asia Other': 'K', 'Canada': 'K', 'Caribbean': 'L', 'China': 'G', 'Central America': 'L', 'South America': 'L', 'Europe One': 'H', 'Europe Two': 'I', 'Europe Other': 'I', 'Eastern Europe': 'L', 'India Sub.': 'E', 'Middle East': 'F', 'Mexico': 'L', 'NPAC': 'H', 'US, AK, HI, PR': 'J', 'IQ, AF, SA': 'M'},
        'United States, PR': {'Africa': 'L', 'Asia One': 'F', 'Asia Two': 'H', 'Asia Other': 'I', 'Canada': 'B', 'Caribbean': 'G', 'China': 'F', 'Central America': 'H', 'South America': 'G', 'Europe One': 'E', 'Europe Two': 'G', 'Europe Other': 'G', 'Eastern Europe': 'L', 'India Sub.': 'G', 'Middle East': 'I', 'Mexico': 'B', 'NPAC': 'F', 'US, AK, HI, PR': 'N/A', 'IQ, AF, SA': 'L'},
        'Uruguay': {'Africa': 'M', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'I', 'Caribbean': 'I', 'China': 'L', 'Central America': 'I', 'South America': 'H', 'Europe One': 'K', 'Europe Two': 'M', 'Europe Other': 'K', 'Eastern Europe': 'L', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'J', 'NPAC': 'L', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'M'},
        'Venezuela': {'Africa': 'M', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'G', 'Caribbean': 'I', 'China': 'L', 'Central America': 'H', 'South America': 'H', 'Europe One': 'K', 'Europe Two': 'L', 'Europe Other': 'K', 'Eastern Europe': 'L', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'H', 'NPAC': 'L', 'US, AK, HI, PR': 'G', 'IQ, AF, SA': 'M'},
        'Vietnam': {'Africa': 'M', 'Asia One': 'C', 'Asia Two': 'G', 'Asia Other': 'G', 'Canada': 'K', 'Caribbean': 'L', 'China': 'C', 'Central America': 'L', 'South America': 'L', 'Europe One': 'K', 'Europe Two': 'K', 'Europe Other': 'L', 'Eastern Europe': 'L', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'K', 'NPAC': 'F', 'US, AK, HI, PR': 'J', 'IQ, AF, SA': 'M'},
        'Hungary': {'Africa': 'N', 'Asia One': 'L', 'Asia Two': 'L', 'Asia Other': 'M', 'Canada': 'J', 'Caribbean': 'M', 'China': 'L', 'Central America': 'M', 'South America': 'M', 'Europe One': 'O', 'Europe Two': 'O', 'Europe Other': 'O', 'Eastern Europe': 'P', 'India Sub.': 'L', 'Middle East': 'L', 'Mexico': 'L', 'NPAC': 'L', 'US, AK, HI, PR': 'I', 'IQ, AF, SA': 'N'},
        'Czech republic': {'Africa': 'N', 'Asia One': 'M', 'Asia Two': 'M', 'Asia Other': 'N', 'Canada': 'L', 'Caribbean': 'N', 'China': 'M', 'Central America': 'N', 'South America': 'N', 'Europe One': 'P', 'Europe Two': 'P', 'Europe Other': 'P', 'Eastern Europe': 'P', 'India Sub.': 'M', 'Middle East': 'M', 'Mexico': 'M', 'NPAC': 'M', 'US, AK, HI, PR': 'L', 'IQ, AF, SA': 'N'}
    }
    
    # Insert the zone mapping data
    count = 0
    for origin, dest_zones in zone_mapping.items():
        for dest_region, zone in dest_zones.items():
            if zone != 'N/A':  # Skip N/A entries
                cursor.execute('''
                    INSERT INTO fedex_zone_matrix 
                    (origin_country, destination_region, zone_letter, created_timestamp)
                    VALUES (?, ?, ?, datetime('now'))
                ''', (origin, dest_region, zone))
                count += 1
    
    print(f"Inserted {count} zone mapping entries")
    
    # Create a helper view for easy zone lookup
    cursor.execute('''
        CREATE VIEW IF NOT EXISTS fedex_zone_lookup AS
        SELECT 
            zm.origin_country,
            cz.country_code as destination_country,
            zm.zone_letter,
            zm.destination_region
        FROM fedex_zone_matrix zm
        JOIN fedex_country_zones cz ON 
            (zm.destination_region = 'Africa' AND cz.country_code IN ('AO', 'BF', 'BI', 'BJ', 'BW', 'CD', 'CF', 'CG', 'CI', 'CM', 'CV', 'DJ', 'DZ', 'ER', 'ET', 'GA', 'GH', 'GM', 'GN', 'GQ', 'GW', 'KE', 'LR', 'LS', 'LY', 'MA', 'MG', 'ML', 'MR', 'MU', 'MW', 'MZ', 'NA', 'NE', 'NG', 'RE', 'RW', 'SC', 'SD', 'SL', 'SN', 'SO', 'SZ', 'TD', 'TG', 'TN', 'TZ', 'UG', 'ZA', 'ZM', 'ZW')) OR
            (zm.destination_region = 'Asia One' AND cz.country_code IN ('HK', 'MO', 'MY', 'SG', 'TH', 'TW')) OR
            (zm.destination_region = 'Asia Two' AND cz.country_code IN ('AU', 'ID', 'NZ', 'PH', 'VN')) OR
            (zm.destination_region = 'China' AND cz.country_code = 'CN') OR
            (zm.destination_region = 'NPAC' AND cz.country_code IN ('JP', 'KR'))
        WHERE zm.active = 1
    ''')
    
    conn.commit()
    conn.close()
    print("FedEx zone matrix created successfully!")

if __name__ == "__main__":
    create_fedex_zone_matrix()
