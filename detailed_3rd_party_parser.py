#!/usr/bin/env python3
"""
DHL Express 3rd Party Rate Card Detailed Parser
==============================================

This script parses the specific structure of each 3rd party sheet
to understand the data layout and extract meaningful information.
"""

import pandas as pd
import numpy as np
import re

def parse_zones_sheet(excel_path: str):
    """Parse the AU Zones 3rdCty TD sheet to understand zone mappings"""
    
    print("\n=== Parsing Zone Mappings (AU Zones 3rdCty TD) ===")
    
    try:
        df = pd.read_excel(excel_path, sheet_name='AU Zones 3rdCty TD')
        
        # Skip header rows and find the actual data
        # Look for the row with "Countries & Territories" and "Zone"
        data_start_row = None
        for i, row in df.iterrows():
            if isinstance(row.iloc[0], str) and 'Countries & Territories' in str(row.iloc[0]):
                data_start_row = i + 1  # Start from next row
                break
        
        if data_start_row is not None:
            print(f"Data starts at row {data_start_row}")
            
            # Extract data from multiple columns (there appear to be multiple country-zone pairs per row)
            zones_data = []
            
            for i in range(data_start_row, len(df)):
                row = df.iloc[i]
                
                # Check each pair of columns for country-zone data
                for col_offset in range(0, len(row), 3):  # Every 3 columns: country, zone, empty
                    if col_offset + 1 < len(row):
                        country_cell = row.iloc[col_offset]
                        zone_cell = row.iloc[col_offset + 1]
                        
                        if pd.notna(country_cell) and pd.notna(zone_cell):
                            country_str = str(country_cell).strip()
                            
                            # Extract country code from format like "Japan (JP)"
                            match = re.search(r'\(([A-Z]{2})\)', country_str)
                            if match:
                                country_code = match.group(1)
                                country_name = country_str.split('(')[0].strip()
                                zone = int(zone_cell) if pd.notna(zone_cell) else None
                                
                                zones_data.append({
                                    'country_name': country_name,
                                    'country_code': country_code,
                                    'zone': zone
                                })
            
            print(f"Found {len(zones_data)} country-zone mappings")
            print("Sample mappings:")
            for item in zones_data[:10]:
                print(f"  {item['country_name']} ({item['country_code']}) → Zone {item['zone']}")
            
            # Look for Japan and New Zealand specifically
            jp_zone = next((item['zone'] for item in zones_data if item['country_code'] == 'JP'), None)
            nz_zone = next((item['zone'] for item in zones_data if item['country_code'] == 'NZ'), None)
            
            print(f"\nRelevant for our example:")
            print(f"  Japan (JP) → Zone {jp_zone}")
            print(f"  New Zealand (NZ) → Zone {nz_zone}")
            
            return zones_data
        
    except Exception as e:
        print(f"Error parsing zones sheet: {str(e)}")
    
    return []

def parse_matrix_sheet(excel_path: str):
    """Parse the AU Matrix TD 3rdCtry sheet to understand zone intersections"""
    
    print("\n=== Parsing Zone Matrix (AU Matrix TD 3rdCtry) ===")
    
    try:
        df = pd.read_excel(excel_path, sheet_name='AU Matrix TD 3rdCtry')
        
        # Find the matrix data by looking for numeric zones
        matrix_data = {}
        
        # Look for the actual matrix starting point
        for i, row in df.iterrows():
            row_str = str(row.iloc[0]).strip()
            if row_str.isdigit():  # Found a row that starts with a zone number
                origin_zone = int(row_str)
                print(f"Found origin zone {origin_zone} at row {i}")
                
                # Extract the destination zone mappings
                for j in range(1, len(row)):
                    cell_value = row.iloc[j]
                    if pd.notna(cell_value) and isinstance(cell_value, str) and len(cell_value.strip()) == 1:
                        dest_zone = j  # Column index represents destination zone
                        result_zone = cell_value.strip().upper()
                        matrix_data[(origin_zone, dest_zone)] = result_zone
                        print(f"  Zone {origin_zone} → Zone {dest_zone} = Zone {result_zone}")
        
        print(f"\nFound {len(matrix_data)} zone intersections")
        
        # Look for our specific example (Zone 4 → Zone 5)
        result_zone = matrix_data.get((4, 5), None)
        print(f"Zone 4 → Zone 5 = Zone {result_zone}")
        
        return matrix_data
        
    except Exception as e:
        print(f"Error parsing matrix sheet: {str(e)}")
    
    return {}

def parse_rates_sheet(excel_path: str):
    """Parse the AU TD 3rdCty WW sheet to understand rate structure"""
    
    print("\n=== Parsing Rate Cards (AU TD 3rdCty WW) ===")
    
    try:
        df = pd.read_excel(excel_path, sheet_name='AU TD 3rdCty WW')
        
        # Look for weight ranges and zone columns
        rates_data = []
        current_section = None
        
        for i, row in df.iterrows():
            row_str = str(row.iloc[0]).strip().lower()
            
            # Identify sections
            if 'documents' in row_str:
                current_section = 'DOCUMENTS'
                print(f"Found section: {current_section} at row {i}")
            elif 'non-documents' in row_str or 'nondocuments' in row_str:
                current_section = 'NON_DOCUMENTS'
                print(f"Found section: {current_section} at row {i}")
            elif 'multiplier' in row_str:
                current_section = 'MULTIPLIER'
                print(f"Found section: {current_section} at row {i}")
            
            # Look for weight ranges (format like "0.5" or "0.5-1.0")
            if current_section and pd.notna(row.iloc[0]):
                weight_str = str(row.iloc[0]).strip()
                
                # Try to parse weight ranges
                if re.match(r'^\d+\.?\d*$', weight_str) or '-' in weight_str:
                    print(f"  Weight row: {weight_str}")
                    
                    # Extract zone rates (columns should be A, B, C, D, etc.)
                    zone_rates = {}
                    for j in range(1, min(9, len(row))):  # Zones A through H
                        cell_value = row.iloc[j]
                        if pd.notna(cell_value) and isinstance(cell_value, (int, float)):
                            zone_letter = chr(ord('A') + j - 1)
                            zone_rates[zone_letter] = float(cell_value)
                    
                    if zone_rates:
                        rates_data.append({
                            'section': current_section,
                            'weight_range': weight_str,
                            'zones': zone_rates
                        })
                        
                        # Show zone D specifically for our example
                        if 'D' in zone_rates:
                            print(f"    Zone D: ${zone_rates['D']:.2f}")
        
        print(f"\nFound {len(rates_data)} rate entries")
        
        # Look for 15kg rate in Zone D
        print(f"\nLooking for 15kg rate in Zone D:")
        for rate_entry in rates_data:
            if 'D' in rate_entry['zones']:
                print(f"  {rate_entry['section']} - {rate_entry['weight_range']}: Zone D = ${rate_entry['zones']['D']:.2f}")
        
        return rates_data
        
    except Exception as e:
        print(f"Error parsing rates sheet: {str(e)}")
    
    return []

def main():
    """Main parsing function"""
    
    excel_path = r"uploads\ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx"
    
    print("=== DHL Express 3rd Party Rate Card Detailed Analysis ===")
    
    # Parse each sheet
    zones_data = parse_zones_sheet(excel_path)
    matrix_data = parse_matrix_sheet(excel_path)
    rates_data = parse_rates_sheet(excel_path)
    
    # Simulate the lookup process for our example
    print("\n=== Simulating Lookup Process ===")
    print("Example: Tokyo to Auckland, 15kg (Invoice MELIR00819599)")
    
    # Step 1: Country codes
    print("\nStep 1: Extract country codes")
    print("  Shipper: Tokyo, Japan → JP")
    print("  Consignee: Auckland, New Zealand → NZ")
    
    # Step 2: Zone lookup
    print("\nStep 2: Zone lookup")
    jp_zone = next((item['zone'] for item in zones_data if item['country_code'] == 'JP'), 'Not found')
    nz_zone = next((item['zone'] for item in zones_data if item['country_code'] == 'NZ'), 'Not found')
    print(f"  Japan (JP) → Zone {jp_zone}")
    print(f"  New Zealand (NZ) → Zone {nz_zone}")
    
    # Step 3: Matrix lookup
    print("\nStep 3: Matrix intersection")
    if isinstance(jp_zone, int) and isinstance(nz_zone, int):
        result_zone = matrix_data.get((jp_zone, nz_zone), 'Not found')
        print(f"  Zone {jp_zone} × Zone {nz_zone} → Zone {result_zone}")
    else:
        result_zone = 'Cannot calculate'
        print(f"  Cannot calculate matrix: missing zone data")
    
    # Step 4: Rate lookup
    print("\nStep 4: Rate lookup (15kg, Zone D)")
    if result_zone == 'D':
        for rate_entry in rates_data:
            if 'D' in rate_entry['zones']:
                weight_range = rate_entry['weight_range']
                rate = rate_entry['zones']['D']
                section = rate_entry['section']
                print(f"  {section} - {weight_range}: ${rate:.2f}")
                
                # Check if 15kg falls in this range
                if '15' in weight_range or ('10' in weight_range and '20' in weight_range):
                    print(f"    ✓ 15kg matches this range: ${rate:.2f}")
    
    print(f"\n🎯 Expected result: $461.09 (from invoice)")

if __name__ == "__main__":
    main()
