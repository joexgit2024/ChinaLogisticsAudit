#!/usr/bin/env python3
"""
Extract zones from CN Zones TDI Exp+Imp sheet and populate DHL Express zone tables.
"""

import pandas as pd
import sqlite3
import os
from datetime import datetime


def connect_to_database():
    """Connect to the DHL audit database."""
    db_path = "dhl_audit.db"
    if not os.path.exists(db_path):
        print(f"Error: Database file {db_path} not found!")
        return None
    
    try:
        conn = sqlite3.connect(db_path)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


def extract_zones_from_sheet(excel_file):
    """Extract zone data from the CN Zones TDI Exp+Imp sheet."""
    try:
        # Read the specific sheet
        df = pd.read_excel(excel_file, sheet_name='CN Zones TDI Exp+Imp', engine='openpyxl')
        print(f"Sheet shape: {df.shape}")
        
        zones = []
        
        # The data starts from row 4 (index 3) based on the analysis
        # Column structure: Countries & Territories | Zone | empty | Countries & Territories | Zone | etc.
        
        # Process data starting from row 4
        for idx in range(3, len(df)):
            row = df.iloc[idx]
            
            # Process columns in groups of 3 (country, zone, empty)
            for col_group in range(0, len(df.columns), 3):
                if col_group + 1 < len(df.columns):
                    country_val = row.iloc[col_group]
                    zone_val = row.iloc[col_group + 1]
                    
                    if pd.notna(country_val) and pd.notna(zone_val):
                        country_str = str(country_val).strip()
                        zone_str = str(zone_val).strip()
                        
                        # Skip header rows
                        if 'Countries & Territories' in country_str or zone_str == 'Zone':
                            continue
                        
                        # Extract country code from format like "Afghanistan (AF)"
                        if '(' in country_str and ')' in country_str:
                            country_name = country_str.split('(')[0].strip()
                            country_code = country_str.split('(')[1].replace(')', '').strip()
                        else:
                            country_name = country_str
                            country_code = country_str[:2].upper()
                        
                        zone_data = {
                            'country_code': country_code,
                            'country_name': country_name,
                            'zone': zone_str
                        }
                        zones.append(zone_data)
                        print(f"Found: {country_name} ({country_code}) -> Zone {zone_str}")
        
        print(f"Total zones extracted: {len(zones)}")
        return zones
        
    except Exception as e:
        print(f"Error extracting zones: {e}")
        return []


def populate_zone_tables(conn, zones):
    """Populate both import and export zone tables with the same data."""
    cursor = conn.cursor()
    
    try:
        # Clear existing data
        print("Clearing existing zone data...")
        cursor.execute("DELETE FROM dhl_express_import_zones")
        cursor.execute("DELETE FROM dhl_express_export_zones")
        
        # Add timestamp
        timestamp = datetime.now().isoformat()
        
        # Insert into import zones
        print(f"Inserting {len(zones)} import zones...")
        for zone in zones:
            cursor.execute("""
                INSERT INTO dhl_express_import_zones
                (country_code, country_name, zone, created_timestamp)
                VALUES (?, ?, ?, ?)
            """, (zone['country_code'], zone['country_name'], zone['zone'], timestamp))
        
        # Insert into export zones (same data)
        print(f"Inserting {len(zones)} export zones...")
        for zone in zones:
            cursor.execute("""
                INSERT INTO dhl_express_export_zones
                (country_code, country_name, zone, created_timestamp)
                VALUES (?, ?, ?, ?)
            """, (zone['country_code'], zone['country_name'], zone['zone'], timestamp))
        
        conn.commit()
        print("Zone data successfully populated!")
        
        # Verify the data
        cursor.execute("SELECT COUNT(*) FROM dhl_express_import_zones")
        import_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM dhl_express_export_zones")
        export_count = cursor.fetchone()[0]
        
        print(f"Import zones in database: {import_count}")
        print(f"Export zones in database: {export_count}")
        
        # Show sample data
        print("\nSample import zones:")
        cursor.execute("SELECT country_code, country_name, zone FROM dhl_express_import_zones LIMIT 10")
        for row in cursor.fetchall():
            print(f"  {row[0]} - {row[1]} -> Zone {row[2]}")
        
    except Exception as e:
        print(f"Error populating zone tables: {e}")
        conn.rollback()


def main():
    # File path
    excel_file = "uploads/ID_104249_01_AP_V01_Commscope_CN_20241113-074659-898.xlsx"
    
    if not os.path.exists(excel_file):
        print(f"Error: Excel file {excel_file} not found!")
        return
    
    # Connect to database
    conn = connect_to_database()
    if not conn:
        return
    
    try:
        # Extract zones from the specific sheet
        print(f"Extracting zones from {excel_file}")
        zones = extract_zones_from_sheet(excel_file)
        
        if zones:
            # Populate database
            print("\n" + "="*50)
            print("POPULATING DATABASE")
            print("="*50)
            populate_zone_tables(conn, zones)
        else:
            print("No zone data found in the Excel file.")
    
    finally:
        conn.close()


if __name__ == "__main__":
    main()
