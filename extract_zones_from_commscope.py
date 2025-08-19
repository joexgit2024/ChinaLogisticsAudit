#!/usr/bin/env python3
"""
Extract import and export zones from Commscope Excel file and populate DHL Express zone tables.
This script reads the Excel file and populates dhl_express_import_zones and dhl_express_export_zones tables.
"""

import pandas as pd
import sqlite3
import sys
import os
from pathlib import Path

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

def read_excel_file(file_path):
    """Read the Excel file and return all sheets."""
    try:
        # Read all sheets from the Excel file
        excel_data = pd.read_excel(file_path, sheet_name=None, engine='openpyxl')
        print(f"Found {len(excel_data)} sheets in the Excel file:")
        for sheet_name in excel_data.keys():
            print(f"  - {sheet_name}")
        return excel_data
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return None

def analyze_sheets_for_zones(excel_data):
    """Analyze all sheets to find zone-related data."""
    zone_data = {
        'import_zones': [],
        'export_zones': []
    }
    
    for sheet_name, df in excel_data.items():
        print(f"\nAnalyzing sheet: {sheet_name}")
        print(f"Shape: {df.shape}")
        
        # Display first few rows to understand structure
        print("First 10 rows:")
        print(df.head(10))
        
        # Look for zone-related columns
        zone_columns = []
        for col in df.columns:
            if any(keyword in str(col).lower() for keyword in ['zone', 'country', 'destination', 'origin']):
                zone_columns.append(col)
        
        if zone_columns:
            print(f"Found potential zone columns: {zone_columns}")
            
            # Try to extract zone data
            for col in zone_columns:
                unique_values = df[col].dropna().unique()
                if len(unique_values) > 0:
                    print(f"Unique values in {col}: {list(unique_values)[:20]}...")  # Show first 20
    
    return zone_data

def extract_zone_mappings(excel_data):
    """Extract zone mappings from the Excel data."""
    import_zones = []
    export_zones = []
    
    for sheet_name, df in excel_data.items():
        print(f"\nProcessing sheet: {sheet_name}")
        
        # Look for different possible zone mapping patterns
        # Pattern 1: Look for sheets with "zone" in the name
        if 'zone' in sheet_name.lower():
            print(f"Found zone sheet: {sheet_name}")
            
            # Try to identify zone columns
            for col in df.columns:
                if 'country' in str(col).lower() or 'destination' in str(col).lower():
                    country_col = col
                    break
            else:
                country_col = df.columns[0] if len(df.columns) > 0 else None
            
            for col in df.columns:
                if 'zone' in str(col).lower():
                    zone_col = col
                    break
            else:
                zone_col = df.columns[1] if len(df.columns) > 1 else None
            
            if country_col and zone_col:
                for _, row in df.iterrows():
                    country = row[country_col]
                    zone = row[zone_col]
                    if pd.notna(country) and pd.notna(zone):
                        zone_data = {
                            'country_code': str(country).strip(),
                            'country_name': str(country).strip(),
                            'zone': str(zone).strip()
                        }
                        
                        # Determine if import or export based on sheet name or context
                        if 'import' in sheet_name.lower() or 'imp' in sheet_name.lower():
                            import_zones.append(zone_data)
                        elif 'export' in sheet_name.lower() or 'exp' in sheet_name.lower():
                            export_zones.append(zone_data)
                        else:
                            # Default to both if unclear
                            import_zones.append(zone_data)
                            export_zones.append(zone_data.copy())
        
        # Pattern 2: Look for rate cards with zone information
        elif any(keyword in sheet_name.lower() for keyword in ['rate', 'tariff', 'price']):
            print(f"Found rate sheet: {sheet_name}")
            
            # Look for zone information in rate sheets
            for col in df.columns:
                if 'zone' in str(col).lower():
                    unique_zones = df[col].dropna().unique()
                    for zone in unique_zones:
                        if str(zone).strip() and str(zone).strip() != '':
                            zone_data = {
                                'country_code': 'UNKNOWN',
                                'country_name': 'UNKNOWN',
                                'zone': str(zone).strip()
                            }
                            import_zones.append(zone_data)
                            export_zones.append(zone_data.copy())
    
    return import_zones, export_zones

def populate_zone_tables(conn, import_zones, export_zones):
    """Populate the DHL Express zone tables."""
    cursor = conn.cursor()
    
    try:
        # Clear existing data
        print("Clearing existing zone data...")
        cursor.execute("DELETE FROM dhl_express_import_zones")
        cursor.execute("DELETE FROM dhl_express_export_zones")
        
        # Insert import zones
        print(f"Inserting {len(import_zones)} import zones...")
        for zone in import_zones:
            cursor.execute("""
                INSERT OR REPLACE INTO dhl_express_import_zones
                (country_code, country_name, zone)
                VALUES (?, ?, ?)
            """, (zone['country_code'], zone['country_name'], zone['zone']))
        
        # Insert export zones
        print(f"Inserting {len(export_zones)} export zones...")
        for zone in export_zones:
            cursor.execute("""
                INSERT OR REPLACE INTO dhl_express_export_zones
                (country_code, country_name, zone)
                VALUES (?, ?, ?)
            """, (zone['country_code'], zone['country_name'], zone['zone']))
        
        conn.commit()
        print("Zone data successfully populated!")
        
        # Verify the data
        cursor.execute("SELECT COUNT(*) FROM dhl_express_import_zones")
        import_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM dhl_express_export_zones")
        export_count = cursor.fetchone()[0]
        
        print(f"Import zones in database: {import_count}")
        print(f"Export zones in database: {export_count}")
        
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
        # Read Excel file
        print(f"Reading Excel file: {excel_file}")
        excel_data = read_excel_file(excel_file)
        if not excel_data:
            return
        
        # Analyze sheets for zone data
        print("\n" + "="*50)
        print("ANALYZING SHEETS FOR ZONE DATA")
        print("="*50)
        analyze_sheets_for_zones(excel_data)
        
        # Extract zone mappings
        print("\n" + "="*50)
        print("EXTRACTING ZONE MAPPINGS")
        print("="*50)
        import_zones, export_zones = extract_zone_mappings(excel_data)
        
        print(f"Extracted {len(import_zones)} import zones")
        print(f"Extracted {len(export_zones)} export zones")
        
        if import_zones or export_zones:
            # Populate database
            print("\n" + "="*50)
            print("POPULATING DATABASE")
            print("="*50)
            populate_zone_tables(conn, import_zones, export_zones)
        else:
            print("No zone data found in the Excel file.")
            print("Please check the file structure and try manual mapping.")
    
    finally:
        conn.close()

if __name__ == "__main__":
    main()
