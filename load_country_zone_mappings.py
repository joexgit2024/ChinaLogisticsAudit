#!/usr/bin/env python3
"""
DHL Express Country Zone Mapping Script
=======================================

This script loads country-to-zone mappings from DHL Express rate card
for both Import and Export services. It uses country codes rather than
city names for proper global mapping.
"""

import sqlite3
import pandas as pd
import re
import os

def extract_zone_mappings_from_sheet(file_path, sheet_name, service_type):
    """
    Extract country-to-zone mappings from a specific sheet in the rate card
    
    Args:
        file_path: Path to the Excel rate card
        sheet_name: Name of the sheet containing zone mappings
        service_type: 'Import' or 'Export'
        
    Returns:
        List of tuples: (country_code, destination_country, zone_number, service_type)
    """
    print(f"Processing {sheet_name} sheet for {service_type} mappings...")
    
    # Load the sheet
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    
    # Extract zone mappings
    mappings = []
    destination = 'AUSTRALIA' if service_type == 'Import' else 'All Countries'
    
    # Process each column to find country and zone pairs
    for col_idx, col in enumerate(df.columns):
        for row_idx, value in enumerate(df[col]):
            # Convert to string to handle non-string values
            cell_value = str(value)
            
            # Look for pattern like 'Country Name (CODE)'
            if '(' in cell_value and ')' in cell_value and cell_value != 'nan':
                # Extract country code
                match = re.search(r'\(([A-Z]{2,3})\)', cell_value)
                if match:
                    country_code = match.group(1)
                    country_name = cell_value.split('(')[0].strip()
                    
                    # Try to get the zone from the next column
                    try:
                        next_col = df.columns[col_idx + 1]
                        zone_value = df.iloc[row_idx, col_idx + 1]
                        
                        # Check if zone value is valid
                        if pd.notna(zone_value) and str(zone_value).isdigit():
                            if service_type == 'Import':
                                # For Import: Foreign Country -> Australia
                                mappings.append((country_code, 'AU', int(zone_value), service_type))
                            else:
                                # For Export: Australia -> Foreign Country
                                mappings.append(('AU', country_code, int(zone_value), service_type))
                    except:
                        # If we can't find a zone, skip this entry
                        continue
    
    print(f"Found {len(mappings)} {service_type} zone mappings")
    return mappings

def load_zone_mappings_to_db(mappings, conn):
    """
    Load zone mappings into the database
    
    Args:
        mappings: List of tuples (origin_code, destination_code, zone_number, service_type)
        conn: SQLite connection
    """
    cursor = conn.cursor()
    
    # Check if we need to update the table schema
    cursor.execute("PRAGMA table_info(dhl_express_zone_mapping)")
    columns = [col[1] for col in cursor.fetchall()]
    
    if 'country_code' not in columns:
        # Add country_code column if it doesn't exist
        cursor.execute("ALTER TABLE dhl_express_zone_mapping ADD COLUMN country_code TEXT")
    
    # Clear existing mappings (optional - you might want to keep the city-based ones too)
    cursor.execute("DELETE FROM dhl_express_zone_mapping WHERE origin_code IN ('AU') OR destination_code IN ('AU')")
    
    # Insert new mappings
    for origin, destination, zone, service_type in mappings:
        cursor.execute('''
            INSERT INTO dhl_express_zone_mapping 
            (origin_code, destination_code, zone_number, service_type)
            VALUES (?, ?, ?, ?)
        ''', (origin, destination, zone, service_type))
    
    conn.commit()

def main():
    """Main function to load zone mappings"""
    # Define file path
    file_path = os.path.join('uploads', 'ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx')
    
    if not os.path.exists(file_path):
        print(f"Error: Rate card file not found at {file_path}")
        return
    
    # Get all sheet names
    try:
        xl = pd.ExcelFile(file_path)
        sheets = xl.sheet_names
        print(f"Available sheets in rate card: {sheets}")
        
        # Find the correct import and export sheets
        import_sheet = None
        export_sheet = None
        
        for sheet in sheets:
            if 'zone' in sheet.lower() and 'import' in sheet.lower():
                import_sheet = sheet
            elif 'zone' in sheet.lower() and 'export' in sheet.lower():
                export_sheet = sheet
        
        if not import_sheet or not export_sheet:
            print("Error: Could not find Import and Export zone sheets")
            return
        
        print(f"Found Import sheet: {import_sheet}")
        print(f"Found Export sheet: {export_sheet}")
        
        # Extract mappings
        import_mappings = extract_zone_mappings_from_sheet(file_path, import_sheet, 'Import')
        export_mappings = extract_zone_mappings_from_sheet(file_path, export_sheet, 'Export')
        
        # Combine mappings
        all_mappings = import_mappings + export_mappings
        
        # Load to database
        conn = sqlite3.connect('dhl_audit.db')
        load_zone_mappings_to_db(all_mappings, conn)
        
        # Verify results
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dhl_express_zone_mapping")
        total_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM dhl_express_zone_mapping WHERE origin_code='AU'")
        export_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM dhl_express_zone_mapping WHERE destination_code='AU'")
        import_count = cursor.fetchone()[0]
        
        print("\nZone Mapping Summary:")
        print(f"Total zone mappings: {total_count}")
        print(f"Export mappings (AU to other countries): {export_count}")
        print(f"Import mappings (Other countries to AU): {import_count}")
        
        # Show US mapping specifically
        cursor.execute("""
            SELECT origin_code, destination_code, zone_number, service_type 
            FROM dhl_express_zone_mapping 
            WHERE origin_code='US' OR destination_code='US'
        """)
        us_mappings = cursor.fetchall()
        print("\nUS Mappings:")
        for mapping in us_mappings:
            print(f"{mapping[0]} → {mapping[1]}: Zone {mapping[2]} ({mapping[3]})")
        
        conn.close()
        print("\nZone mappings successfully loaded!")
        
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
