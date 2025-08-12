"""Constants for DHL Express audit system."""

# Country code mappings for address parsing
COUNTRY_MAPPINGS = {
    'ITALY': 'IT',
    'ITALIA': 'IT',
    'GERMANY': 'DE',
    'DEUTSCHLAND': 'DE',
    'AUSTRALIA': 'AU',
    'NETHERLANDS': 'NL',
    'HOLLAND': 'NL',
    'NEDERLAND': 'NL',
    'UNITED STATES': 'US',
    'USA': 'US',
    'UNITED KINGDOM': 'GB',
    'UK': 'GB',
    'FRANCE': 'FR',
    'SPAIN': 'ES',
    'CHINA': 'CN'
}

# Fuzzy service description mappings
FUZZY_SERVICE_MAPPINGS = {
    'CHANGE OF BILLING': 'KA',
    'OVER LENGTH': 'KA', 
    'OVERWEIGHT': 'KA',
    'DIRECT SIGNATURE': 'SF',
    'ADULT SIGNATURE': 'SD',
    'SIGNATURE': 'SF',
    'REMOTE AREA PICKUP': 'OB',
    'REMOTE AREA DELIVERY': 'OO',
    'REMOTE AREA': 'OO',
    'BONDED STORAGE': 'WK',
    'EXPORT DECLARATION': 'WO',
    'NEUTRAL DELIVERY': 'NN',
    'NON CONVEYABLE PIECE': 'YO',
    'OVERWEIGHT PIECE': 'YY',
    'OVERSIZE PIECE': 'YB',
    'SATURDAY DELIVERY': 'AA',
    'SATURDAY PICKUP': 'AB',
    'RESIDENTIAL ADDRESS': 'TK',
    'ADDRESS CORRECTION': 'MA'
}

# Third party indicators
THIRD_PARTY_INDICATORS = [
    '3RD PARTY',
    'THIRD PARTY', 
    'EXPRESS WORLDWIDE',
    'EXPRESS 3RDCTY',
    'THIRD COUNTRY'
]

# Australian domestic city zone mapping
AU_DOMESTIC_CITY_ZONE_MAPPING = {
    # Major cities with specific zones
    'MELBOURNE': 1, 'MEL': 1,
    'BRISBANE': 2, 'BNE': 2,
    'SYDNEY': 3, 'SYD': 3,
    'CANBERRA': 4, 'CBR': 4,
    
    # Cities that go to "Rest of Australia" Zone 5
    'ADELAIDE': 5, 'ADL': 5,
    'PERTH': 5, 'PER': 5,
    'HOBART': 5, 'HBA': 5,
    'DARWIN': 5, 'DRW': 5,
    
    # State codes - use with caution, city is more specific
    'VIC': 1, 'VICTORIA': 1,        # Melbourne area
    'QLD': 2, 'QUEENSLAND': 2,      # Brisbane area  
    'NSW': 3, 'NEW SOUTH WALES': 3, # Sydney area
    'ACT': 4,                       # Canberra
    'SA': 5, 'SOUTH AUSTRALIA': 5, # Adelaide -> Rest of AU
    'WA': 5, 'WESTERN AUSTRALIA': 5, # Perth -> Rest of AU
    'TAS': 5, 'TASMANIA': 5,       # Hobart -> Rest of AU
    'NT': 5, 'NORTHERN TERRITORY': 5 # Darwin -> Rest of AU
}

# Audit variance thresholds
VARIANCE_THRESHOLD_PASS = 0.05  # 5%
VARIANCE_THRESHOLD_REVIEW = 0.15  # 15%

# Bonded storage charge constants
BONDED_STORAGE_BASE_CHARGE = 18.00  # AUD per shipment
BONDED_STORAGE_PER_KG_CHARGE = 0.35  # AUD per kg

# Service charge types that need variant lookup
VARIANT_LOOKUP_SERVICE_CODES = ['YY', 'YB', 'II', 'OO']

# Date formats for parsing
DATE_FORMATS = [
    '%d/%m/%Y',  # 6/05/2025
    '%m/%d/%Y',  # 05/06/2025
    '%Y-%m-%d',  # 2025-05-06
    '%d-%m-%Y',  # 06-05-2025
    '%Y/%m/%d'   # 2025/05/06
]
