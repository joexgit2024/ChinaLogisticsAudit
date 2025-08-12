"""Utility functions for DHL Express audit system."""

from typing import Optional
from datetime import datetime
from dhl_express_audit_constants import COUNTRY_MAPPINGS, AU_DOMESTIC_CITY_ZONE_MAPPING, DATE_FORMATS


def extract_country_code(address_details: str) -> Optional[str]:
    """Extract country code from address details string."""
    if not address_details:
        return None
    
    # Split by semicolon and get the last meaningful part before any email
    parts = address_details.split(';')
    
    # Look for 2-letter country code (should be near the end)
    for part in reversed(parts):
        part = part.strip()
        # Check if this looks like a country code (2 uppercase letters)
        if len(part) == 2 and part.isupper() and part.isalpha():
            return part
    
    # Fallback: try common country name mappings
    address_upper = address_details.upper()
    
    for country_name, country_code in COUNTRY_MAPPINGS.items():
        if country_name in address_upper:
            return country_code
            
    return None


def get_au_domestic_zone(address_details: str) -> Optional[int]:
    """Extract AU domestic zone from address details using actual zone data."""
    if not address_details:
        return None
        
    address_upper = address_details.upper()
    
    # Check full state names first (most specific to avoid substring issues)
    full_states = ['SOUTH AUSTRALIA', 'WESTERN AUSTRALIA', 'NEW SOUTH WALES', 
                   'NORTHERN TERRITORY', 'QUEENSLAND', 'TASMANIA']
    for state in full_states:
        if state in address_upper:
            return AU_DOMESTIC_CITY_ZONE_MAPPING[state]
    
    # Check full city names
    for city_name in ['MELBOURNE', 'BRISBANE', 'SYDNEY', 'CANBERRA', 
                      'ADELAIDE', 'PERTH', 'HOBART', 'DARWIN']:
        if city_name in address_upper:
            return AU_DOMESTIC_CITY_ZONE_MAPPING[city_name]
    
    # Check city codes (3-letter codes)
    for city_code in ['MEL', 'BNE', 'SYD', 'CBR', 'ADL', 'PER', 'HBA', 'DRW']:
        if city_code in address_upper:
            return AU_DOMESTIC_CITY_ZONE_MAPPING[city_code]
    
    # Fallback to short state codes (least specific)
    for state in ['VIC', 'VICTORIA', 'QLD', 'NSW', 'ACT', 'SA', 'WA', 'TAS', 'NT']:
        if state in address_upper:
            return AU_DOMESTIC_CITY_ZONE_MAPPING[state]
            
    # Default to Zone 5 (Rest of Australia) for unmatched AU addresses
    return 5


def parse_date(date_str: str) -> Optional[str]:
    """Parse date string to standard format."""
    if not date_str or not date_str.strip():
        return None
        
    date_str = date_str.strip()
    
    # Try different date formats
    for fmt in DATE_FORMATS:
        try:
            parsed_date = datetime.strptime(date_str, fmt)
            return parsed_date.strftime('%Y-%m-%d')
        except ValueError:
            continue
            
    # If no format works, return the original string
    return date_str


def is_domestic_shipment(shipper_details: str, receiver_details: str) -> bool:
    """Determine if a shipment is domestic (within Australia) or international."""
    try:
        # Extract country codes from addresses (look for AU)
        shipper_country = extract_country_code(shipper_details)
        receiver_country = extract_country_code(receiver_details)
        
        # Domestic if both origin and destination are AU (Australia)
        return shipper_country == 'AU' and receiver_country == 'AU'
        
    except Exception as e:
        print(f"Error determining domestic/international: {e}")
        return False  # Default to international on error
