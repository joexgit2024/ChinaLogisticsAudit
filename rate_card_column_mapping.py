#!/usr/bin/env python3
"""
DHL Rate Card - Excel to Database Column Mapping Configuration
This file contains the definitive mapping between Excel columns and database fields.
"""

# Complete Excel to Database Column Mapping
# This mapping was created based on the actual Excel file structure and database schema
EXCEL_TO_DB_MAPPING = {
    # Basic Information
    'Rate Check': 'rate_check',
    'Bidder Name': 'bidder_name',
    'Contract Validity': 'contract_validity',
    'Rate Validity': 'rate_validity',
    'Award': 'award_status',
    
    # Lane Information
    'Lane ID': 'lane_id',
    'Lane Description': 'lane_description',
    'RouteID': 'route_id',
    'Service': 'service',
    
    # Origin Information
    'Origin Region': 'origin_region',
    'Origin Country': 'origin_country',
    'Lane Origin': 'lane_origin',
    'Cities Included in Origin Lane': 'cities_included_origin',
    
    # Destination Information (Note: 'Destinaiton' is misspelled in Excel)
    'Destination Region': 'destination_region',
    'Destinaiton Country': 'destination_country',
    'Lane Destination': 'lane_destination',
    'Cities Included in Dest Lane': 'cities_included_dest',
    
    # Route Requirements
    'Est Avg Monthly Weight (KG)': 'est_monthly_weight_kg',
    'Route Requirements': 'route_requirements',
    'Bidder Comments': 'bidder_comments',
    
    # Transit Information
    'Target Transit': 'target_transit',
    'Transit Validation': 'transit_validation',
    
    # Port Codes (these have dynamic matching based on content)
    # Pattern: 'Origin Port Code' + 'IATA' -> origin_port_code
    # Pattern: 'Destination Port Code' + 'IATA' -> destination_port_code
    
    # DTP Charges
    'DTP Min Charge (USD)': 'dtp_min_charge',
    # Pattern: 'DTP Freight Cost' + 'USD/KG' -> dtp_freight_cost
    
    # Origin Charges
    # Pattern: 'Origin Min Charge' + 'USD' -> origin_min_charge
    # Pattern: 'Origin Fees' + 'THC' + 'USD/KG' -> origin_fees
    
    # ATA Charges
    # Pattern: 'ATA Fuel' + 'USD/KG' -> fuel_surcharge
    # Pattern: 'ATA Min Charge' + 'USD' -> ata_min_charge
    # Pattern: 'ATA Cost' + '< 1000 KG' -> ata_cost_lt1000kg
    # Pattern: 'ATA Cost' + '1000 - 1999 KG' -> ata_cost_1000_1999kg
    # Pattern: 'ATA Cost' + '2000 - 3000 KG' -> ata_cost_2000_3000kg
    # Pattern: 'ATA Cost' + '>3000 KG' -> ata_cost_gt3000kg
    
    # Destination Charges
    # Pattern: 'Destination Min Charge' + 'USD' -> destination_min_charge
    # Pattern: 'Destination Fees' + 'THC' + 'USD/KG' -> destination_fees
    
    # PTD Charges
    'PTD Min Charge (USD)': 'ptd_min_charge',
    'PTD Freight Charge (USD/KG)': 'ptd_freight_charge',
    
    # PSS Information
    'PSS Validity': 'pss_validity',
    'PSS (USD/KG)': 'pss_charge',
    
    # Validation and Totals
    'Rate Validation': 'rate_validation',
    'Total Min Charge': 'total_min_charge',
    
    # Base Rates by Weight Tiers
    # Pattern: 'Total Base Rate' + '< 1K' -> base_rate_lt1000kg
    # Pattern: 'Total Base Rate' + '1K - <2K' -> base_rate_1000to2000kg
    # Pattern: 'Total Base Rate' + '2K - 3K' -> base_rate_2000to3000kg
    # Pattern: 'Total Base Rate' + '>3K' -> base_rate_gt3000kg
    
    # Base + PSS Rates by Weight Tiers
    # Pattern: 'Base+PSS' + '< 1K' -> base_pss_lt1000kg
    # Pattern: 'Base+PSS' + '1K - <2K' -> base_pss_1000_2000kg
    # Pattern: 'Base+PSS' + '2K - 3K' -> base_pss_2000_3000kg
    # Pattern: 'Base+PSS' + '>3K' -> base_pss_gt3000kg
    
    # Breakeven Information
    # Pattern: 'Minium Charge Breakeven' -> breakeven_kg
}

# Pattern-based mappings for complex column names with newlines
PATTERN_MAPPINGS = [
    # Transit Time (contains newlines)
    ({'contains': ['Transit time', 'Business Days']}, 'transit_time'),
    
    # Port Codes
    ({'contains': ['Origin Port Code', 'IATA']}, 'origin_port_code'),
    ({'contains': ['Destination Port Code', 'IATA']}, 'destination_port_code'),
    
    # DTP Charges
    ({'contains': ['DTP Freight Cost', 'USD/KG']}, 'dtp_freight_cost'),
    
    # Origin Charges
    ({'contains': ['Origin Min Charge', 'USD']}, 'origin_min_charge'),
    ({'contains': ['Origin Fees', 'THC', 'USD/KG']}, 'origin_fees'),
    
    # ATA Charges
    ({'contains': ['ATA Fuel', 'USD/KG']}, 'fuel_surcharge'),
    ({'contains': ['ATA Min Charge', 'USD']}, 'ata_min_charge'),
    ({'contains': ['ATA Cost', '< 1000 KG']}, 'ata_cost_lt1000kg'),
    ({'contains': ['ATA Cost', '1000 - 1999 KG']}, 'ata_cost_1000_1999kg'),
    ({'contains': ['ATA Cost', '2000 - 3000 KG']}, 'ata_cost_2000_3000kg'),
    ({'contains': ['ATA Cost', '>3000 KG']}, 'ata_cost_gt3000kg'),
    
    # Destination Charges
    ({'contains': ['Destination Min Charge', 'USD']}, 'destination_min_charge'),
    ({'contains': ['Destination Fees', 'THC', 'USD/KG']}, 'destination_fees'),
    
    # Base Rates
    ({'contains': ['Total Base Rate', '< 1K']}, 'base_rate_lt1000kg'),
    ({'contains': ['Total Base Rate', '1K - <2K']}, 'base_rate_1000to2000kg'),
    ({'contains': ['Total Base Rate', '2K - 3K']}, 'base_rate_2000to3000kg'),
    ({'contains': ['Total Base Rate', '>3K']}, 'base_rate_gt3000kg'),
    
    # Base + PSS Rates
    ({'contains': ['Base+PSS', '< 1K']}, 'base_pss_lt1000kg'),
    ({'contains': ['Base+PSS', '1K - <2K']}, 'base_pss_1000_2000kg'),
    ({'contains': ['Base+PSS', '2K - 3K']}, 'base_pss_2000_3000kg'),
    ({'contains': ['Base+PSS', '>3K']}, 'base_pss_gt3000kg'),
    
    # Breakeven
    ({'contains': ['Minium Charge Breakeven']}, 'breakeven_kg'),
]

def get_db_column_for_excel(excel_column_name):
    """
    Get the database column name for a given Excel column name.
    
    Args:
        excel_column_name (str): The Excel column name
        
    Returns:
        str: The corresponding database column name, or None if not found
    """
    # First try exact match
    if excel_column_name in EXCEL_TO_DB_MAPPING:
        return EXCEL_TO_DB_MAPPING[excel_column_name]
    
    # Try pattern matching
    for pattern, db_column in PATTERN_MAPPINGS:
        if 'contains' in pattern:
            if all(keyword in excel_column_name for keyword in pattern['contains']):
                return db_column
    
    return None

def get_all_mappings(excel_columns):
    """
    Get all mappings for a list of Excel columns.
    
    Args:
        excel_columns (list): List of Excel column names
        
    Returns:
        dict: Dictionary mapping Excel columns to database columns
    """
    mappings = {}
    unmapped = []
    
    for excel_col in excel_columns:
        db_col = get_db_column_for_excel(excel_col)
        if db_col:
            mappings[excel_col] = db_col
        else:
            unmapped.append(excel_col)
    
    return mappings, unmapped

def print_mapping_summary(excel_columns):
    """Print a summary of the column mappings."""
    mappings, unmapped = get_all_mappings(excel_columns)
    
    print(f"📊 COLUMN MAPPING SUMMARY")
    print(f"=" * 50)
    print(f"Total Excel columns: {len(excel_columns)}")
    print(f"Mapped columns: {len(mappings)}")
    print(f"Unmapped columns: {len(unmapped)}")
    
    if unmapped:
        print(f"\n⚠️ UNMAPPED COLUMNS:")
        for col in unmapped:
            print(f"  - {col}")

if __name__ == "__main__":
    # Test with sample columns
    test_columns = [
        'Lane ID',
        'Origin Region', 
        'ATA Cost\nCharge Weight < 1000 KG\n(USD/KG)',
        'Base+PSS USD/KG\n< 1K (w/o fuel)'
    ]
    
    print("🧪 TESTING COLUMN MAPPING")
    print("=" * 40)
    for col in test_columns:
        db_col = get_db_column_for_excel(col)
        print(f"'{col}' -> '{db_col}'")
