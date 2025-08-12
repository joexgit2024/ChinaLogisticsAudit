#!/usr/bin/env python3
"""
Complete DHL Express 3rd Party Charge Loader
===========================================

Loads all 3rd party data into database tables with verified logic.
"""

import sqlite3
import pandas as pd
import numpy as np


def create_3rd_party_tables(db_path: str):
    """Create the 3rd party tables"""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Drop existing tables if they exist
    cursor.execute("DROP TABLE IF EXISTS dhl_express_3rd_party_rates")
    cursor.execute("DROP TABLE IF EXISTS dhl_express_3rd_party_matrix")
    cursor.execute("DROP TABLE IF EXISTS dhl_express_3rd_party_zones")
    
    # Create zones table
    cursor.execute("""
        CREATE TABLE dhl_express_3rd_party_zones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            country_code TEXT NOT NULL,
            zone INTEGER NOT NULL,
            region TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(country_code)
        )
    """)
    
    # Create matrix table
    cursor.execute("""
        CREATE TABLE dhl_express_3rd_party_matrix (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin_zone INTEGER NOT NULL,
            destination_zone INTEGER NOT NULL,
            rate_zone TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(origin_zone, destination_zone)
        )
    """)
    
    # Create rates table
    cursor.execute("""
        CREATE TABLE dhl_express_3rd_party_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            weight_kg REAL NOT NULL,
            zone_a REAL,
            zone_b REAL,
            zone_c REAL,
            zone_d REAL,
            zone_e REAL,
            zone_f REAL,
            zone_g REAL,
            zone_h REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(weight_kg)
        )
    """)
    
    conn.commit()
    conn.close()
    print("✅ Created 3rd party tables")


def load_zone_mappings(excel_path: str, db_path: str):
    """Load country to zone mappings"""
    
    conn = sqlite3.connect(db_path)
    
    try:
        df = pd.read_excel(excel_path, sheet_name='AU Zones 3rdCty TD', 
                          header=None)
        
        zone_data = []
        
        # Find data starting from row with countries
        for i, row in df.iterrows():
            if i < 5:  # Skip header rows
                continue
                
            country_code = row.iloc[0]
            zone = row.iloc[1]
            region = row.iloc[2] if len(row) > 2 else None
            
            if (pd.notna(country_code) and pd.notna(zone) 
                and str(country_code).strip() and str(zone).strip()):
                
                try:
                    zone_int = int(float(str(zone).strip()))
                    zone_data.append({
                        'country_code': str(country_code).strip().upper(),
                        'zone': zone_int,
                        'region': str(region).strip() if pd.notna(region) else None
                    })
                except (ValueError, TypeError):
                    continue
        
        # Insert into database
        for data in zone_data:
            conn.execute("""
                INSERT OR REPLACE INTO dhl_express_3rd_party_zones 
                (country_code, zone, region) VALUES (?, ?, ?)
            """, (data['country_code'], data['zone'], data['region']))
        
        conn.commit()
        print(f"✅ Loaded {len(zone_data)} zone mappings")
        
        # Verify key mappings
        cursor = conn.cursor()
        cursor.execute("SELECT zone FROM dhl_express_3rd_party_zones WHERE country_code = 'JP'")
        jp_zone = cursor.fetchone()
        cursor.execute("SELECT zone FROM dhl_express_3rd_party_zones WHERE country_code = 'NZ'")
        nz_zone = cursor.fetchone()
        
        if jp_zone and nz_zone:
            print(f"✅ JP → Zone {jp_zone[0]}, NZ → Zone {nz_zone[0]}")
        
    except Exception as e:
        print(f"❌ Error loading zones: {e}")
    finally:
        conn.close()


def load_matrix_data(excel_path: str, db_path: str):
    """Load the zone matrix"""
    
    conn = sqlite3.connect(db_path)
    
    try:
        df = pd.read_excel(excel_path, sheet_name='AU Matrix TD 3rdCtry', 
                          header=None)
        
        # Get header row (destination zones)
        header_row = df.iloc[7]
        destination_zones = []
        for i, cell in enumerate(header_row):
            if pd.notna(cell) and str(cell).strip().isdigit():
                destination_zones.append((i, int(str(cell).strip())))
        
        print(f"Found destination zones at columns: {destination_zones}")
        
        matrix_data = []
        
        # Process origin zone rows (starting from row 10)
        for row_idx in range(10, min(20, len(df))):  # Zones 1-10
            row = df.iloc[row_idx]
            origin_zone_cell = row.iloc[1]
            
            if pd.notna(origin_zone_cell):
                try:
                    origin_zone = int(float(str(origin_zone_cell).strip()))
                    
                    # For each destination zone
                    for dest_col, dest_zone in destination_zones:
                        if dest_col < len(row):
                            rate_zone = row.iloc[dest_col]
                            if pd.notna(rate_zone):
                                rate_zone_str = str(rate_zone).strip().upper()
                                if rate_zone_str in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']:
                                    matrix_data.append({
                                        'origin_zone': origin_zone,
                                        'destination_zone': dest_zone,
                                        'rate_zone': rate_zone_str
                                    })
                                    
                except (ValueError, TypeError):
                    continue
        
        # Insert into database
        for data in matrix_data:
            conn.execute("""
                INSERT OR REPLACE INTO dhl_express_3rd_party_matrix 
                (origin_zone, destination_zone, rate_zone) VALUES (?, ?, ?)
            """, (data['origin_zone'], data['destination_zone'], data['rate_zone']))
        
        conn.commit()
        print(f"✅ Loaded {len(matrix_data)} matrix entries")
        
        # Verify key lookup
        cursor = conn.cursor()
        cursor.execute("""
            SELECT rate_zone FROM dhl_express_3rd_party_matrix 
            WHERE origin_zone = 4 AND destination_zone = 5
        """)
        result = cursor.fetchone()
        if result:
            print(f"✅ Zone 4 × Zone 5 = Zone {result[0]}")
        
    except Exception as e:
        print(f"❌ Error loading matrix: {e}")
    finally:
        conn.close()


def load_rate_data(excel_path: str, db_path: str):
    """Load the rate table"""
    
    conn = sqlite3.connect(db_path)
    
    try:
        df = pd.read_excel(excel_path, sheet_name='AU TD 3rdCty WW', header=None)
        
        rate_data = []
        
        # Find rate data (starting around row 10)
        for i, row in df.iterrows():
            if i < 10:  # Skip header rows
                continue
                
            weight = row.iloc[0]
            if pd.notna(weight):
                try:
                    weight_kg = float(str(weight).strip())
                    
                    # Extract zone rates (columns 2-9 for zones A-H)
                    rates = {}
                    zone_letters = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H']
                    
                    for j, zone_letter in enumerate(zone_letters):
                        rate_col = j + 2  # Start from column 2
                        if rate_col < len(row):
                            rate_value = row.iloc[rate_col]
                            if pd.notna(rate_value):
                                try:
                                    rates[f'zone_{zone_letter.lower()}'] = float(rate_value)
                                except (ValueError, TypeError):
                                    rates[f'zone_{zone_letter.lower()}'] = None
                            else:
                                rates[f'zone_{zone_letter.lower()}'] = None
                    
                    if rates:  # Only add if we have some rates
                        rate_entry = {'weight_kg': weight_kg}
                        rate_entry.update(rates)
                        rate_data.append(rate_entry)
                        
                except (ValueError, TypeError):
                    continue
        
        # Insert into database
        for data in rate_data:
            conn.execute("""
                INSERT OR REPLACE INTO dhl_express_3rd_party_rates 
                (weight_kg, zone_a, zone_b, zone_c, zone_d, zone_e, zone_f, zone_g, zone_h) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                data['weight_kg'],
                data.get('zone_a'),
                data.get('zone_b'), 
                data.get('zone_c'),
                data.get('zone_d'),
                data.get('zone_e'),
                data.get('zone_f'),
                data.get('zone_g'),
                data.get('zone_h')
            ))
        
        conn.commit()
        print(f"✅ Loaded {len(rate_data)} rate entries")
        
        # Verify 15kg Zone D rate
        cursor = conn.cursor()
        cursor.execute("SELECT zone_d FROM dhl_express_3rd_party_rates WHERE weight_kg = 15")
        result = cursor.fetchone()
        if result:
            print(f"✅ 15kg Zone D rate: ${result[0]:.2f}")
        
    except Exception as e:
        print(f"❌ Error loading rates: {e}")
    finally:
        conn.close()


def test_complete_lookup(db_path: str):
    """Test the complete 3rd party lookup process"""
    
    print("\n=== Testing Complete 3rd Party Lookup ===")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Test case: Tokyo to Auckland, 15kg
        origin_country = 'JP'
        dest_country = 'NZ'
        weight = 15.0
        
        # Step 1: Get origin zone
        cursor.execute("""
            SELECT zone FROM dhl_express_3rd_party_zones 
            WHERE country_code = ?
        """, (origin_country,))
        origin_result = cursor.fetchone()
        
        # Step 2: Get destination zone
        cursor.execute("""
            SELECT zone FROM dhl_express_3rd_party_zones 
            WHERE country_code = ?
        """, (dest_country,))
        dest_result = cursor.fetchone()
        
        if not origin_result or not dest_result:
            print("❌ Could not find zones for countries")
            return
        
        origin_zone = origin_result[0]
        dest_zone = dest_result[0]
        
        print(f"Step 1: {origin_country} → Zone {origin_zone}")
        print(f"Step 2: {dest_country} → Zone {dest_zone}")
        
        # Step 3: Get rate zone from matrix
        cursor.execute("""
            SELECT rate_zone FROM dhl_express_3rd_party_matrix 
            WHERE origin_zone = ? AND destination_zone = ?
        """, (origin_zone, dest_zone))
        matrix_result = cursor.fetchone()
        
        if not matrix_result:
            print("❌ No matrix entry found")
            return
        
        rate_zone = matrix_result[0]
        print(f"Step 3: Zone {origin_zone} × Zone {dest_zone} = Zone {rate_zone}")
        
        # Step 4: Get rate for weight and zone
        zone_column = f"zone_{rate_zone.lower()}"
        cursor.execute(f"""
            SELECT {zone_column} FROM dhl_express_3rd_party_rates 
            WHERE weight_kg = ?
        """, (weight,))
        rate_result = cursor.fetchone()
        
        if not rate_result or rate_result[0] is None:
            print("❌ No rate found")
            return
        
        final_rate = rate_result[0]
        print(f"Step 4: {weight}kg Zone {rate_zone} = ${final_rate:.2f}")
        
        # Compare with invoice
        invoice_amount = 461.09
        if abs(final_rate - invoice_amount) < 0.01:
            print(f"✅ SUCCESS: Rate ${final_rate:.2f} matches invoice ${invoice_amount:.2f}!")
        else:
            print(f"❌ MISMATCH: Rate ${final_rate:.2f} vs Invoice ${invoice_amount:.2f}")
            
    except Exception as e:
        print(f"❌ Error during lookup: {e}")
    finally:
        conn.close()


def main():
    """Main loader process"""
    
    excel_path = r"uploads\ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx"
    db_path = "dhl_audit.db"
    
    print("=== DHL Express 3rd Party Charge Loader ===")
    
    # Create tables
    create_3rd_party_tables(db_path)
    
    # Load all data
    load_zone_mappings(excel_path, db_path)
    load_matrix_data(excel_path, db_path)
    load_rate_data(excel_path, db_path)
    
    # Test the complete process
    test_complete_lookup(db_path)
    
    print("\n🎯 3rd Party Charge system is ready!")


if __name__ == "__main__":
    main()
