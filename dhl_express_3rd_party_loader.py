#!/usr/bin/env python3
"""
DHL Express 3rd Party Charge Database Schema and Loader
======================================================

This script creates the database schema and loads the 3rd party charge data.
"""

import sqlite3
import pandas as pd
import re
from typing import Dict, List, Tuple, Optional

class DHLExpress3rdPartyLoader:
    """Loader for DHL Express 3rd Party rate cards"""
    
    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path

    def create_3rd_party_tables(self):
        """Create database tables for 3rd party charges"""
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Drop existing tables
        cursor.execute('DROP TABLE IF EXISTS dhl_express_3rd_party_zones')
        cursor.execute('DROP TABLE IF EXISTS dhl_express_3rd_party_matrix') 
        cursor.execute('DROP TABLE IF EXISTS dhl_express_3rd_party_rates')
        
        # Create zones table
        cursor.execute('''
            CREATE TABLE dhl_express_3rd_party_zones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                country_code VARCHAR(2) NOT NULL,
                country_name VARCHAR(100),
                zone_number INTEGER NOT NULL,
                created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(country_code)
            )
        ''')
        
        # Create matrix table
        cursor.execute('''
            CREATE TABLE dhl_express_3rd_party_matrix (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                origin_zone INTEGER NOT NULL,
                destination_zone INTEGER NOT NULL,
                result_zone VARCHAR(1) NOT NULL,
                created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(origin_zone, destination_zone)
            )
        ''')
        
        # Create rates table
        cursor.execute('''
            CREATE TABLE dhl_express_3rd_party_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rate_type VARCHAR(20) NOT NULL,  -- 'INTERNATIONAL', 'DOMESTIC'
                document_type VARCHAR(20) NOT NULL,  -- 'DOCUMENT', 'NON_DOCUMENT'
                weight_kg DECIMAL(10,3) NOT NULL,
                zone_a DECIMAL(10,2),
                zone_b DECIMAL(10,2),
                zone_c DECIMAL(10,2),
                zone_d DECIMAL(10,2),
                zone_e DECIMAL(10,2),
                zone_f DECIMAL(10,2),
                zone_g DECIMAL(10,2),
                zone_h DECIMAL(10,2),
                created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes
        cursor.execute('CREATE INDEX idx_3rd_party_zones_country ON dhl_express_3rd_party_zones(country_code)')
        cursor.execute('CREATE INDEX idx_3rd_party_matrix_zones ON dhl_express_3rd_party_matrix(origin_zone, destination_zone)')
        cursor.execute('CREATE INDEX idx_3rd_party_rates_weight ON dhl_express_3rd_party_rates(weight_kg)')
        
        conn.commit()
        conn.close()
        
        print("Created 3rd party charge database tables")

    def load_zones_data(self, excel_path: str) -> int:
        """Load zone mappings from Excel file"""
        
        print("Loading zone mappings...")
        
        try:
            df = pd.read_excel(excel_path, sheet_name='AU Zones 3rdCty TD')
            
            # Find data start row
            data_start_row = None
            for i, row in df.iterrows():
                if isinstance(row.iloc[0], str) and 'Countries & Territories' in str(row.iloc[0]):
                    data_start_row = i + 1
                    break
            
            if data_start_row is None:
                print("Could not find zone data start row")
                return 0
            
            zones_data = []
            
            for i in range(data_start_row, len(df)):
                row = df.iloc[i]
                
                # Check each pair of columns for country-zone data
                for col_offset in range(0, len(row), 3):
                    if col_offset + 1 < len(row):
                        country_cell = row.iloc[col_offset]
                        zone_cell = row.iloc[col_offset + 1]
                        
                        if pd.notna(country_cell) and pd.notna(zone_cell):
                            country_str = str(country_cell).strip()
                            
                            # Extract country code from format like "Japan (JP)"
                            match = re.search(r'\\(([A-Z]{2})\\)', country_str)
                            if match:
                                country_code = match.group(1)
                                country_name = country_str.split('(')[0].strip()
                                zone = int(zone_cell) if pd.notna(zone_cell) else None
                                
                                zones_data.append((country_code, country_name, zone))
            
            # Insert into database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.executemany('''
                INSERT INTO dhl_express_3rd_party_zones (country_code, country_name, zone_number)
                VALUES (?, ?, ?)
            ''', zones_data)
            
            conn.commit()
            conn.close()
            
            print(f"Loaded {len(zones_data)} zone mappings")
            return len(zones_data)
            
        except Exception as e:
            print(f"Error loading zones: {str(e)}")
            return 0

    def load_matrix_data(self, excel_path: str) -> int:
        """Load zone matrix from Excel file"""
        
        print("Loading zone matrix...")
        
        try:
            df = pd.read_excel(excel_path, sheet_name='AU Matrix TD 3rdCtry', header=None)
            
            # Find matrix header (row with destination zones 1,2,3,4,5...)
            matrix_start_row = None
            for i in range(len(df)):
                row = df.iloc[i]
                if (pd.notna(row.iloc[2]) and str(row.iloc[2]).strip() == '1' and 
                    pd.notna(row.iloc[3]) and str(row.iloc[3]).strip() == '2'):
                    matrix_start_row = i
                    break
            
            if matrix_start_row is None:
                print("Could not find matrix start row")
                return 0
            
            # Get destination zones from header
            header_row = df.iloc[matrix_start_row]
            dest_zones = []
            for j in range(2, len(header_row)):
                cell = header_row.iloc[j]
                if pd.notna(cell) and str(cell).strip().isdigit():
                    dest_zones.append(int(str(cell).strip()))
            
            print(f"Destination zones: {dest_zones}")
            
            # Extract matrix data
            matrix_data = []
            
            for i in range(matrix_start_row + 1, len(df)):
                row = df.iloc[i]
                origin_cell = row.iloc[1]
                
                if pd.notna(origin_cell) and str(origin_cell).strip().replace('.0', '').isdigit():
                    origin_zone = int(float(str(origin_cell).strip()))
                    
                    for j, dest_zone in enumerate(dest_zones):
                        if j + 2 < len(row):
                            cell_value = row.iloc[j + 2]
                            if pd.notna(cell_value) and isinstance(cell_value, str):
                                result_zone = cell_value.strip().upper()
                                matrix_data.append((origin_zone, dest_zone, result_zone))
            
            # Insert into database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.executemany('''
                INSERT INTO dhl_express_3rd_party_matrix (origin_zone, destination_zone, result_zone)
                VALUES (?, ?, ?)
            ''', matrix_data)
            
            conn.commit()
            conn.close()
            
            print(f"Loaded {len(matrix_data)} matrix entries")
            
            # Verify our test case
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                SELECT result_zone FROM dhl_express_3rd_party_matrix 
                WHERE origin_zone = 4 AND destination_zone = 5
            ''')
            result = cursor.fetchone()
            conn.close()
            
            if result:
                print(f"✓ Zone 4 → Zone 5 = Zone {result[0]}")
            else:
                print("✗ Could not find Zone 4 → Zone 5 mapping")
            
            return len(matrix_data)
            
        except Exception as e:
            print(f"Error loading matrix: {str(e)}")
            return 0

    def load_rates_data(self, excel_path: str) -> int:
        """Load rate data from Excel file"""
        
        print("Loading rate data...")
        
        try:
            df = pd.read_excel(excel_path, sheet_name='AU TD 3rdCty WW', header=None)
            
            rates_data = []
            current_section = 'NON_DOCUMENT'  # Default to non-document
            
            for i in range(len(df)):
                row = df.iloc[i]
                cell_0 = row.iloc[0]
                
                if pd.notna(cell_0):
                    cell_str = str(cell_0).strip().lower()
                    
                    # Detect section type
                    if 'documents' in cell_str and 'non' not in cell_str:
                        current_section = 'DOCUMENT'
                        continue
                    elif 'non-documents' in cell_str or 'nondocuments' in cell_str:
                        current_section = 'NON_DOCUMENT'
                        continue
                    
                    # Check if this is a weight row (numeric)
                    if str(cell_0).strip().replace('.0', '').replace('.5', '').isdigit():
                        weight = float(str(cell_0).strip())
                        
                        # Extract zone rates
                        zone_rates = {}
                        zone_letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
                        
                        for j, zone_letter in enumerate(zone_letters):
                            if j + 1 < len(row):
                                rate_cell = row.iloc[j + 1]
                                if pd.notna(rate_cell) and isinstance(rate_cell, (int, float)):
                                    zone_rates[zone_letter] = float(rate_cell)
                        
                        if zone_rates:  # Only add if we have rates
                            rate_entry = [
                                'INTERNATIONAL',
                                current_section,
                                weight,
                                zone_rates.get('A'),
                                zone_rates.get('B'),
                                zone_rates.get('C'),
                                zone_rates.get('D'),
                                zone_rates.get('E'),
                                zone_rates.get('F'),
                                zone_rates.get('G'),
                                zone_rates.get('H')
                            ]
                            rates_data.append(rate_entry)
            
            # Insert into database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.executemany('''
                INSERT INTO dhl_express_3rd_party_rates (
                    rate_type, document_type, weight_kg,
                    zone_a, zone_b, zone_c, zone_d, zone_e, zone_f, zone_g, zone_h
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', rates_data)
            
            conn.commit()
            conn.close()
            
            print(f"Loaded {len(rates_data)} rate entries")
            
            # Verify our test case (15kg, Zone E)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                SELECT zone_e FROM dhl_express_3rd_party_rates 
                WHERE weight_kg = 15 AND document_type = 'NON_DOCUMENT'
            ''')
            result = cursor.fetchone()
            conn.close()
            
            if result and result[0]:
                print(f"✓ 15kg Zone E rate: ${result[0]:.2f}")
            else:
                print("✗ Could not find 15kg Zone E rate")
            
            return len(rates_data)
            
        except Exception as e:
            print(f"Error loading rates: {str(e)}")
            return 0

    def load_all_data(self, excel_path: str) -> Dict:
        """Load all 3rd party data from Excel file"""
        
        print("=== Loading DHL Express 3rd Party Rate Card Data ===")
        
        self.create_3rd_party_tables()
        
        results = {
            'zones_loaded': self.load_zones_data(excel_path),
            'matrix_entries_loaded': self.load_matrix_data(excel_path),
            'rate_entries_loaded': self.load_rates_data(excel_path)
        }
        
        print("\\n=== Load Summary ===")
        for key, value in results.items():
            print(f"{key}: {value}")
        
        return results

def main():
    """Main function"""
    
    excel_path = r"uploads\\ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx"
    
    loader = DHLExpress3rdPartyLoader()
    results = loader.load_all_data(excel_path)
    
    # Test the lookup process
    print("\\n=== Testing Lookup Process ===")
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Step 1: Get zones for JP and NZ
    cursor.execute('SELECT zone_number FROM dhl_express_3rd_party_zones WHERE country_code = ?', ('JP',))
    jp_zone = cursor.fetchone()
    cursor.execute('SELECT zone_number FROM dhl_express_3rd_party_zones WHERE country_code = ?', ('NZ',))
    nz_zone = cursor.fetchone()
    
    print(f"Japan (JP) → Zone {jp_zone[0] if jp_zone else 'Not found'}")
    print(f"New Zealand (NZ) → Zone {nz_zone[0] if nz_zone else 'Not found'}")
    
    # Step 2: Get matrix result
    if jp_zone and nz_zone:
        cursor.execute('''
            SELECT result_zone FROM dhl_express_3rd_party_matrix 
            WHERE origin_zone = ? AND destination_zone = ?
        ''', (jp_zone[0], nz_zone[0]))
        matrix_result = cursor.fetchone()
        
        result_zone = matrix_result[0] if matrix_result else 'Not found'
        print(f"Zone {jp_zone[0]} × Zone {nz_zone[0]} → Zone {result_zone}")
        
        # Step 3: Get rate for 15kg
        if matrix_result:
            zone_column = f'zone_{result_zone.lower()}'
            cursor.execute(f'''
                SELECT {zone_column} FROM dhl_express_3rd_party_rates 
                WHERE weight_kg = 15 AND document_type = 'NON_DOCUMENT'
            ''')
            rate_result = cursor.fetchone()
            
            if rate_result and rate_result[0]:
                print(f"15kg Zone {result_zone} rate: ${rate_result[0]:.2f}")
                print(f"Expected: $461.09")
                if abs(rate_result[0] - 461.09) < 0.01:
                    print("✅ PERFECT MATCH!")
                else:
                    print(f"❌ Difference: ${abs(rate_result[0] - 461.09):.2f}")
    
    conn.close()

if __name__ == "__main__":
    main()
