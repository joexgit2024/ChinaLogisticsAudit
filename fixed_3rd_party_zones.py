#!/usr/bin/env python3
"""
Fixed 3rd Party Zone Parser
==========================

Parse zones from multi-column layout in AU Zones 3rdCty TD sheet
"""

import sqlite3
import pandas as pd


def load_zone_mappings_fixed(excel_path: str, db_path: str):
    """Load country to zone mappings from multi-column layout"""
    
    conn = sqlite3.connect(db_path)
    
    try:
        df = pd.read_excel(excel_path, sheet_name='AU Zones 3rdCty TD', 
                          header=None)
        
        zone_data = []
        
        print("=== Parsing Multi-Column Zone Layout ===")
        
        # The data is in a multi-column layout
        # We need to search all cells for country patterns
        for i, row in df.iterrows():
            for j, cell in enumerate(row):
                if pd.notna(cell):
                    cell_str = str(cell).strip()
                    
                    # Look for country pattern: "Country Name (CC)"
                    if '(' in cell_str and ')' in cell_str and len(cell_str) > 5:
                        # Try to extract country and code
                        try:
                            if cell_str.endswith(')') and '(' in cell_str:
                                country_part = cell_str[:cell_str.rfind('(')].strip()
                                code_part = cell_str[cell_str.rfind('(')+1:cell_str.rfind(')')].strip()
                                
                                # Check if next cell has a zone number
                                if j + 1 < len(row):
                                    zone_cell = row.iloc[j + 1]
                                    if pd.notna(zone_cell):
                                        try:
                                            zone_int = int(float(str(zone_cell).strip()))
                                            
                                            zone_data.append({
                                                'country_code': code_part.upper(),
                                                'country_name': country_part,
                                                'zone': zone_int,
                                                'row': i,
                                                'col': j
                                            })
                                            
                                            print(f"Found: {country_part} ({code_part}) → Zone {zone_int} at row {i}, col {j}")
                                            
                                        except (ValueError, TypeError):
                                            pass
                        except Exception:
                            pass
        
        print(f"\nTotal countries found: {len(zone_data)}")
        
        # Clear existing data
        conn.execute("DELETE FROM dhl_express_3rd_party_zones")
        
        # Insert into database
        for data in zone_data:
            conn.execute("""
                INSERT INTO dhl_express_3rd_party_zones 
                (country_code, zone, region) VALUES (?, ?, ?)
            """, (data['country_code'], data['zone'], data['country_name']))
        
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
            return True
        else:
            print("❌ Could not find JP or NZ in loaded data")
            return False
        
    except Exception as e:
        print(f"❌ Error loading zones: {e}")
        return False
    finally:
        conn.close()


def test_fixed_lookup(db_path: str):
    """Test the lookup with fixed zone data"""
    
    print("\n=== Testing Fixed 3rd Party Lookup ===")
    
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
            return False
        
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
            return False
        
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
            return False
        
        final_rate = rate_result[0]
        print(f"Step 4: {weight}kg Zone {rate_zone} = ${final_rate:.2f}")
        
        # Compare with invoice
        invoice_amount = 461.09
        if abs(final_rate - invoice_amount) < 0.01:
            print(f"✅ SUCCESS: Rate ${final_rate:.2f} matches invoice ${invoice_amount:.2f}!")
            print("\n🎯 3rd Party Charge lookup is working perfectly!")
            return True
        else:
            print(f"❌ MISMATCH: Rate ${final_rate:.2f} vs Invoice ${invoice_amount:.2f}")
            return False
            
    except Exception as e:
        print(f"❌ Error during lookup: {e}")
        return False
    finally:
        conn.close()


def main():
    """Main fixed loader"""
    
    excel_path = r"uploads\ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx"
    db_path = "dhl_audit.db"
    
    print("=== DHL Express 3rd Party Zone Loader (Fixed) ===")
    
    # Load fixed zone mappings
    success = load_zone_mappings_fixed(excel_path, db_path)
    
    if success:
        # Test the complete process
        test_fixed_lookup(db_path)
    else:
        print("❌ Zone loading failed")


if __name__ == "__main__":
    main()
