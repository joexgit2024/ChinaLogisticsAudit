#!/usr/bin/env python3
"""
Create a comprehensive fixed loader that integrates into the main application
"""

import pandas as pd
import sqlite3
import re
import os
from datetime import datetime

class ComprehensiveFixedRateCardLoader:
    def __init__(self, database_path='dhl_audit.db'):
        self.database_path = database_path
        self.conn = sqlite3.connect(database_path)
        self.cursor = self.conn.cursor()

    def clean_all_duplicates(self):
        """Clean all existing duplicates before loading new data"""
        print("🧹 Cleaning all existing rate card duplicates...")
        
        # Find and remove duplicates, keeping only the latest
        self.cursor.execute('''
            DELETE FROM dhl_express_rate_cards 
            WHERE id NOT IN (
                SELECT MAX(id) 
                FROM dhl_express_rate_cards 
                GROUP BY service_type, rate_section, weight_from, weight_to, is_multiplier
            )
        ''')
        
        deleted_count = self.cursor.rowcount
        print(f"🗑️ Removed {deleted_count} duplicate entries")
        self.conn.commit()

    def load_rate_card_from_excel(self, excel_file, service_type, sheet_name):
        """Load rate card data from Excel with proper duplicate handling"""
        
        print(f"\n{'='*50}")
        print(f"LOADING {service_type.upper()} RATE CARD")
        print(f"File: {excel_file}")
        print(f"Sheet: {sheet_name}")
        print(f"{'='*50}")
        
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
            print(f"📊 Excel sheet has {len(df)} rows and {len(df.columns)} columns")
        except Exception as e:
            print(f"❌ Could not read sheet '{sheet_name}': {e}")
            return False
        
        # Clean existing data for this service type
        print(f"🧹 Cleaning existing {service_type} data...")
        self.cursor.execute('''
            DELETE FROM dhl_express_rate_cards 
            WHERE service_type = ?
        ''', (service_type,))
        deleted_count = self.cursor.rowcount
        print(f"🗑️ Removed {deleted_count} existing {service_type} entries")
        
        # Load rate cards (Documents and Non-Documents)
        self._load_rate_cards(df, service_type)
        
        # Load multiplier section  
        self._load_multipliers(df, service_type)
        
        self.conn.commit()
        return True

    def _load_rate_cards(self, df, service_type):
        """Load regular rate cards (Documents and Non-Documents sections) ONLY for this specific service type"""
        
        print(f"\n📋 Loading {service_type} rate cards from {service_type} sheet ONLY...")
        
        # Find rate card sections - Documents and Non-Documents should be in the same sheet
        documents_start = None
        non_docs_start = None
        
        for idx, row in df.iterrows():
            row_str = ' '.join([str(cell) for cell in row if pd.notna(cell)]).upper()
            if 'DOCUMENTS UP TO' in row_str and documents_start is None:
                documents_start = idx + 2  # Skip header row
                print(f"  📍 {service_type} Documents section starts at row {documents_start}")
            elif 'NON-DOCUMENTS FROM' in row_str and non_docs_start is None:
                non_docs_start = idx + 2  # Skip header row
                print(f"  📍 {service_type} Non-Documents section starts at row {non_docs_start}")
        
        loaded_count = 0
        max_zone_col = 11 if service_type == 'Export' else 21
        current_timestamp = datetime.now().isoformat()
        
        # Load Documents section ONLY from the current service_type sheet
        if documents_start:
            print(f"  📋 Loading {service_type} Documents from {service_type} sheet...")
            loaded_count += self._load_section(df, service_type, 'Documents', documents_start, max_zone_col, current_timestamp)
        
        # Load Non-Documents section ONLY from the current service_type sheet
        if non_docs_start:
            print(f"  📋 Loading {service_type} Non-documents from {service_type} sheet...")
            loaded_count += self._load_section(df, service_type, 'Non-documents', non_docs_start, max_zone_col, current_timestamp)
        
        print(f"  ✅ Loaded {loaded_count} {service_type} rate card entries")

    def _load_section(self, df, service_type, section_name, start_row, max_zone_col, timestamp):
        """Load a specific rate card section for the specified service type only"""
        
        print(f"    📦 Loading {service_type} {section_name} section starting at row {start_row}...")
        loaded_count = 0
        
        # Different logic for Documents vs Non-Documents
        if section_name == 'Documents':
            # Documents section: only load up to 2.0kg or 2.5kg
            max_weight = 2.5
            max_rows = 10
        else:
            # Non-Documents section: load everything up to multiplier section
            max_weight = 30
            max_rows = 100
        
        for offset in range(max_rows):
            idx = start_row + offset
            if idx >= len(df):
                break
                
            row = df.iloc[idx]
            first_col = row[0]
            
            if pd.notna(first_col):
                try:
                    weight_from = float(first_col)
                    
                    # Different stopping conditions for Documents vs Non-Documents
                    if section_name == 'Documents' and weight_from > max_weight:
                        print(f"      ⚠️ Stopped at {weight_from}kg (beyond Documents range)")
                        break
                    elif section_name != 'Documents' and weight_from > max_weight:
                        print(f"      ⚠️ Stopped at {weight_from}kg (multiplier section)")
                        break
                        
                    # Extract weight range
                    weight_to = weight_from + 0.5
                    
                    # Extract zone rates
                    zone_rates = []
                    for zone_col in range(2, max_zone_col):
                        try:
                            rate = float(row[zone_col]) if pd.notna(row[zone_col]) else None
                            zone_rates.append(rate)
                        except:
                            zone_rates.append(None)
                    
                    # Pad to 19 zones
                    while len(zone_rates) < 19:
                        zone_rates.append(None)
                    
                    # Insert rate card with proper service_type
                    self.cursor.execute('''
                        INSERT INTO dhl_express_rate_cards 
                        (service_type, rate_section, weight_from, weight_to, 
                         zone_1, zone_2, zone_3, zone_4, zone_5, zone_6, zone_7, zone_8, zone_9,
                         zone_10, zone_11, zone_12, zone_13, zone_14, zone_15, zone_16, zone_17, zone_18, zone_19,
                         is_multiplier, weight_range_from, weight_range_to, created_timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (service_type, section_name, weight_from, weight_to, *zone_rates, 0, weight_from, weight_to, timestamp))
                    
                    loaded_count += 1
                    if loaded_count <= 3:  # Show first few entries for verification
                        print(f"      💾 {service_type} {section_name} {weight_from}-{weight_to}kg: Zone5=${zone_rates[4] if len(zone_rates) > 4 and zone_rates[4] else 'N/A'}")
                    
                except Exception as e:
                    continue
            else:
                # Empty row - stop for Documents section, continue for Non-Documents
                if section_name == 'Documents':
                    print(f"      ℹ️ Empty row at {idx}, stopping Documents section")
                    break
        
        print(f"    ✅ Loaded {loaded_count} {service_type} {section_name} entries")
        return loaded_count

    def _load_multipliers(self, df, service_type):
        """Load multiplier section using row 76 structure for the specified service type only"""
        
        print(f"\n⚡ Loading {service_type} multipliers from {service_type} sheet...")
        
        # Find multiplier section (row 75 header structure)
        multiplier_start = None
        
        # Check row 75 for multiplier header
        if len(df) > 75:
            row_75_text = ' '.join([str(cell) for cell in df.iloc[75] if pd.notna(cell)]).upper()
            if any(keyword in row_75_text for keyword in ['ADDER RATE', 'MULTIPLIER RATE', 'FROM 30.1', 'PER 0.5']):
                multiplier_start = 77  # Data starts at row 77 (after column headers at row 76)
                print(f"  📍 Found {service_type} multiplier header at row 75: {row_75_text[:50]}...")
        
        if multiplier_start is None:
            print(f"  ⚠️ {service_type} multiplier section not found")
            return
        
        # Load multiplier data
        max_multiplier_rows = 5 if service_type == 'Import' else 3
        max_zone_col = 11 if service_type == 'Export' else 21
        current_timestamp = datetime.now().isoformat()
        loaded_count = 0
        
        for offset in range(max_multiplier_rows):
            idx = multiplier_start + offset
            if idx >= len(df):
                break
                
            row = df.iloc[idx]
            first_col = row[0]
            
            if pd.notna(first_col):
                try:
                    weight_from = float(first_col)
                    if weight_from < 30:
                        break
                    
                    # Extract weight_to from column 1
                    try:
                        weight_to = float(row[1]) if pd.notna(row[1]) else 99999
                    except:
                        weight_to = 99999
                    
                    # Extract zone rates
                    zone_rates = []
                    for zone_col in range(2, max_zone_col):
                        try:
                            rate = float(row[zone_col]) if pd.notna(row[zone_col]) else None
                            zone_rates.append(rate)
                        except:
                            zone_rates.append(None)
                    
                    # Pad to 19 zones
                    while len(zone_rates) < 19:
                        zone_rates.append(None)
                    
                    # Insert multiplier with proper service_type
                    self.cursor.execute('''
                        INSERT INTO dhl_express_rate_cards 
                        (service_type, rate_section, weight_from, weight_to, 
                         zone_1, zone_2, zone_3, zone_4, zone_5, zone_6, zone_7, zone_8, zone_9,
                         zone_10, zone_11, zone_12, zone_13, zone_14, zone_15, zone_16, zone_17, zone_18, zone_19,
                         is_multiplier, weight_range_from, weight_range_to, created_timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (service_type, 'Multiplier', weight_from, weight_to, *zone_rates, 1, weight_from, weight_to, current_timestamp))
                    
                    loaded_count += 1
                    print(f"    📦 {service_type} {weight_from}-{weight_to}kg: Zone5=${zone_rates[4] if len(zone_rates) > 4 and zone_rates[4] else 'N/A'}")
                    
                except Exception as e:
                    print(f"    ⚠️ Error processing {service_type} row {idx}: {e}")
                    break
        
        print(f"  ✅ Loaded {loaded_count} {service_type} multiplier entries")

def main():
    """Main function to test the comprehensive loader"""
    
    print("=" * 60)
    print("COMPREHENSIVE FIXED RATE CARD LOADER")
    print("=" * 60)
    
    excel_file = 'uploads\\import and export.xlsx'
    if not os.path.exists(excel_file):
        print(f"❌ File not found: {excel_file}")
        return
    
    loader = ComprehensiveFixedRateCardLoader()
    
    try:
        # Clean existing duplicates first
        loader.clean_all_duplicates()
        
        # Load Export data
        success_export = loader.load_rate_card_from_excel(excel_file, 'Export', 'AU TD Exp WW')
        
        # Load Import data
        success_import = loader.load_rate_card_from_excel(excel_file, 'Import', 'AU TD Imp WW')
        
        if success_export and success_import:
            print("\n" + "="*60)
            print("FINAL VERIFICATION")
            print("="*60)
            
            # Count final results
            for service_type in ['Export', 'Import']:
                loader.cursor.execute('''
                    SELECT COUNT(*) FROM dhl_express_rate_cards 
                    WHERE service_type = ? AND is_multiplier = 0
                ''', (service_type,))
                rate_count = loader.cursor.fetchone()[0]
                
                loader.cursor.execute('''
                    SELECT COUNT(*) FROM dhl_express_rate_cards 
                    WHERE service_type = ? AND is_multiplier = 1
                ''', (service_type,))
                multiplier_count = loader.cursor.fetchone()[0]
                
                print(f"📊 {service_type}: {rate_count} rate cards, {multiplier_count} multipliers")
            
            # Check for any remaining duplicates
            loader.cursor.execute('''
                SELECT service_type, rate_section, weight_from, weight_to, COUNT(*) as count
                FROM dhl_express_rate_cards 
                GROUP BY service_type, rate_section, weight_from, weight_to
                HAVING COUNT(*) > 1
            ''')
            duplicates = loader.cursor.fetchall()
            
            if duplicates:
                print(f"⚠️ Found {len(duplicates)} remaining duplicates:")
                for dup in duplicates[:5]:
                    print(f"   {dup[0]} {dup[1]} {dup[2]}-{dup[3]}kg: {dup[4]} copies")
            else:
                print("✅ No duplicates found! Data loaded successfully.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        loader.conn.rollback()
    finally:
        loader.conn.close()

if __name__ == '__main__':
    main()
