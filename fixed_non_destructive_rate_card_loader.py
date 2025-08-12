#!/usr/bin/env python3
"""
Fixed version of the non-destructive rate card loader that:
1. Processes ALL multiplier weight ranges (not just first one)
2. Uses proper UPDATE/DELETE logic instead of INSERT OR REPLACE to avoid duplicates
3. Maintains non-destructive behavior for manually added data
"""

import pandas as pd
import sqlite3
import re
import os
from datetime import datetime

class FixedNonDestructiveRateCardLoader:
    def __init__(self, database_path='dhl_audit.db'):
        self.database_path = database_path
        self.conn = sqlite3.connect(database_path)
        self.cursor = self.conn.cursor()
        
    def _update_multiplier_section_fixed(self, df, service_type, cursor):
        """FIXED: Update multiplier section processing ALL rows, not just first one"""
        
        # Check for existing multiplier ranges > 70kg (preserve manual data)
        cursor.execute('''
            SELECT weight_from, weight_to, zone_5 
            FROM dhl_express_rate_cards 
            WHERE service_type = ? AND is_multiplier = 1 
            AND weight_from > 70
            ORDER BY weight_from
        ''', (service_type,))
        
        existing_ranges = cursor.fetchall()
        preserved_count = len(existing_ranges)
        
        if existing_ranges:
            print(f"🔍 Found {preserved_count} existing multiplier ranges to preserve:")
            for range_data in existing_ranges:
                print(f"    {range_data[0]}-{range_data[1]}kg: Zone 5 = ${range_data[2]}")
        
        # Find multiplier section - both Import and Export start at row 76
        multiplier_start = None
        
        # Method 1: Check row 76 for multiplier header (consistent structure)
        if len(df) > 76:
            row_76_text = ' '.join([str(cell) for cell in df.iloc[76] if pd.notna(cell)]).upper()
            if any(keyword in row_76_text for keyword in ['ADDER RATE', 'MULTIPLIER RATE', 'FROM 30.1', 'PER 0.5']):
                multiplier_start = 78  # Data starts at row 78 (after column headers at row 77)
                print(f"  📍 Found multiplier header at row 76: {row_76_text[:80]}...")
        
        # Method 2: Fallback - look for explicit multiplier headers 
        if multiplier_start is None:
            for idx, row in df.iterrows():
                if any('MULTIPLIER' in str(cell).upper() or 'ADDER RATE' in str(cell).upper() or 'PER 0.5' in str(cell).upper() 
                       for cell in row if pd.notna(cell)):
                    # Check if next row has column headers (From, To, Zone...)
                    if idx + 1 < len(df):
                        next_row_text = ' '.join([str(cell) for cell in df.iloc[idx + 1] if pd.notna(cell)]).upper()
                        if 'FROM' in next_row_text and 'TO' in next_row_text and 'ZONE' in next_row_text:
                            multiplier_start = idx + 2  # Skip header row
                            print(f"  📍 Found multiplier header at row {idx}, data starts at row {idx + 2}")
                        else:
                            multiplier_start = idx + 1
                            print(f"  📍 Found multiplier header at row {idx}, data starts at row {idx + 1}")
                    break
        
        # Method 3: Look for weight jump pattern (backup method)
        if multiplier_start is None:
            print(f"  🔍 No multiplier header found, scanning for weight patterns...")
            for idx in range(len(df) - 1):
                try:
                    current_weight = float(df.iloc[idx, 0]) if pd.notna(df.iloc[idx, 0]) else None
                    next_weight = float(df.iloc[idx + 1, 0]) if pd.notna(df.iloc[idx + 1, 0]) else None
                    
                    # Look for the pattern where we jump from 30.0 to 30.1+ (multiplier section)
                    if (current_weight is not None and next_weight is not None and 
                        current_weight == 30.0 and next_weight > 30.0):
                        multiplier_start = idx + 1
                        print(f"  📍 Found multiplier section by weight pattern at row {idx + 1} ({next_weight}kg)")
                        break
                except:
                    continue
        
        if multiplier_start is None:
            print("  ⚠️ Multiplier section not found in Excel")
            return 0, preserved_count
        
        # FIXED: Process ALL multiplier rows from the known structure
        multiplier_rows = []
        
        # Based on user input: Export has 3 rows, Import has 5 rows of multiplier data
        max_multiplier_rows = 5 if service_type == 'Import' else 3
        
        for offset in range(max_multiplier_rows):
            idx = multiplier_start + offset
            if idx < len(df):
                first_col = df.iloc[idx, 0]
                if pd.notna(first_col):
                    try:
                        weight_val = float(first_col)
                        if weight_val >= 30:
                            multiplier_rows.append((idx, df.iloc[idx], weight_val))
                            print(f"    📍 Row {idx}: {weight_val}kg multiplier")
                        else:
                            print(f"    ⚠️ Row {idx}: Unexpected weight {weight_val}kg < 30")
                            break
                    except Exception as e:
                        print(f"    ⚠️ Row {idx}: Could not parse weight: {e}")
                        break
                else:
                    print(f"    ⚠️ Row {idx}: Empty weight cell")
                    break
            else:
                print(f"    ⚠️ Row {idx}: Beyond end of sheet")
                break
        
        if not multiplier_rows:
            print("  ⚠️ No multiplier rate rows found in Excel")
            return 0, preserved_count
        
        print(f"  ✅ Found {len(multiplier_rows)} multiplier weight ranges in Excel:")
        
        # FIXED: Delete existing Excel-sourced multiplier ranges first (but preserve manual ones > 70kg)
        # However, we need to be smarter about which ranges to preserve vs. replace
        
        # Get the weight ranges we're about to load from Excel
        excel_weight_ranges = [weight_val for idx, multiplier_row, weight_val in multiplier_rows]
        min_excel_weight = min(excel_weight_ranges) if excel_weight_ranges else 30
        max_excel_weight = max(excel_weight_ranges) if excel_weight_ranges else 100
        
        print(f"  📊 Excel covers weight range: {min_excel_weight}kg - {max_excel_weight}kg")
        
        # Delete existing ranges that overlap with what we're loading from Excel
        cursor.execute('''
            DELETE FROM dhl_express_rate_cards 
            WHERE service_type = ? AND is_multiplier = 1 
            AND weight_from >= ? AND weight_from <= ?
        ''', (service_type, min_excel_weight, max_excel_weight))
        
        deleted_count = cursor.rowcount
        print(f"  🗑️ Cleaned up {deleted_count} overlapping multiplier ranges")
        
        # Process all multiplier rows
        loaded_count = 0
        max_zone_col = 11 if service_type == 'Export' else 21
        current_timestamp = datetime.now().isoformat()
        
        for idx, multiplier_row, actual_weight in multiplier_rows:
            # Extract zone rates - Import has more zones than Export
            zone_rates = []
            for zone_col in range(2, max_zone_col):
                try:
                    rate = float(multiplier_row[zone_col]) if pd.notna(multiplier_row[zone_col]) else None
                    zone_rates.append(rate)
                except:
                    zone_rates.append(None)
            
            # Pad to 19 zones
            while len(zone_rates) < 19:
                zone_rates.append(None)
            
            # Extract weight_to from column 1 (To column) if available, otherwise calculate
            try:
                weight_to = float(multiplier_row[1]) if pd.notna(multiplier_row[1]) else actual_weight + 0.5
            except:
                weight_to = actual_weight + 0.5
            
            # Insert the new multiplier range
            cursor.execute('''
                INSERT INTO dhl_express_rate_cards 
                (service_type, rate_section, weight_from, weight_to, 
                 zone_1, zone_2, zone_3, zone_4, zone_5, zone_6, zone_7, zone_8, zone_9,
                 zone_10, zone_11, zone_12, zone_13, zone_14, zone_15, zone_16, zone_17, zone_18, zone_19,
                 is_multiplier, weight_range_from, weight_range_to, created_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (service_type, 'Multiplier', actual_weight, weight_to, *zone_rates, 1, actual_weight, weight_to, current_timestamp))
            
            loaded_count += 1
            print(f"    📦 {actual_weight}-{weight_to}kg: Zone5=${zone_rates[4] if len(zone_rates) > 4 and zone_rates[4] else 'N/A'}")
        
        print(f"  ⚡ Loaded {loaded_count} multiplier ranges from Excel")
        if preserved_count > 0:
            print(f"  🔒 Preserved {preserved_count} manually added multiplier ranges")
        
        return loaded_count, preserved_count
    
    def _update_rate_cards_fixed(self, df, service_type, cursor):
        """FIXED: Update rate cards using proper DELETE/INSERT to avoid duplicates"""
        
        # Check for existing manual ranges > 30kg (preserve manual data)
        cursor.execute('''
            SELECT weight_from, weight_to, zone_5 
            FROM dhl_express_rate_cards 
            WHERE service_type = ? AND is_multiplier = 0 
            AND weight_from > 30
            ORDER BY weight_from
        ''', (service_type,))
        
        manual_ranges = cursor.fetchall()
        preserved_count = len(manual_ranges)
        
        if manual_ranges:
            print(f"🔍 Found {preserved_count} manual rate card ranges to preserve:")
            for range_data in manual_ranges[:5]:  # Show first 5
                print(f"    {range_data[0]}-{range_data[1]}kg: Zone 5 = ${range_data[2]}")
            if preserved_count > 5:
                print(f"    ... and {preserved_count - 5} more")
        
        # Find rate card section
        documents_start = None
        non_docs_start = None
        
        for idx, row in df.iterrows():
            row_str = ' '.join([str(cell) for cell in row if pd.notna(cell)]).upper()
            if 'DOCUMENTS' in row_str and documents_start is None:
                documents_start = idx + 1
            elif 'NON-DOCUMENTS' in row_str and non_docs_start is None:
                non_docs_start = idx + 1
        
        if documents_start is None and non_docs_start is None:
            print("  ⚠️ Rate card sections not found in Excel")
            return 0, preserved_count
        
        # FIXED: Delete existing Excel-sourced rate cards first (but preserve manual ones > 30kg)
        # However, we need to be smarter about which ranges to preserve vs. replace
        
        # Find the weight ranges we'll be loading
        rate_card_weight_ranges = []
        
        # Scan documents section
        if documents_start:
            for idx in range(documents_start, min(documents_start + 20, len(df))):
                if idx < len(df):
                    first_col = df.iloc[idx, 0]
                    if pd.notna(first_col):
                        try:
                            weight_from = float(first_col)
                            if weight_from <= 30:
                                rate_card_weight_ranges.append(weight_from)
                        except:
                            continue
        
        # Scan non-documents section
        if non_docs_start:
            for idx in range(non_docs_start, min(non_docs_start + 100, len(df))):
                if idx < len(df):
                    first_col = df.iloc[idx, 0]
                    if pd.notna(first_col):
                        try:
                            weight_from = float(first_col)
                            if weight_from <= 30:
                                rate_card_weight_ranges.append(weight_from)
                        except:
                            continue
        
        if rate_card_weight_ranges:
            min_weight = min(rate_card_weight_ranges)
            max_weight = max(rate_card_weight_ranges)
            print(f"  📊 Excel covers weight range: {min_weight}kg - {max_weight}kg")
            
            # Delete existing ranges that overlap with what we're loading from Excel
            cursor.execute('''
                DELETE FROM dhl_express_rate_cards 
                WHERE service_type = ? AND is_multiplier = 0 
                AND weight_from >= ? AND weight_from <= ?
            ''', (service_type, min_weight, max_weight))
        else:
            # Fallback: delete up to 30kg
            cursor.execute('''
                DELETE FROM dhl_express_rate_cards 
                WHERE service_type = ? AND is_multiplier = 0 
                AND weight_from <= 30
            ''', (service_type,))
        
        deleted_count = cursor.rowcount
        print(f"  🗑️ Cleaned up {deleted_count} overlapping rate card ranges")
        
        # Process rate card sections
        loaded_count = 0
        current_timestamp = datetime.now().isoformat()
        max_zone_col = 11 if service_type == 'Export' else 21
        
        # Process Documents section
        if documents_start:
            for idx in range(documents_start, min(documents_start + 20, len(df))):
                if idx < len(df):
                    row = df.iloc[idx]
                    first_col = row[0]
                    
                    if pd.notna(first_col):
                        try:
                            weight_from = float(first_col)
                            if weight_from > 30:  # Stop at multiplier section
                                break
                                
                            # Extract weight range
                            weight_to = weight_from + 0.5  # Default increment
                            
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
                            
                            # Insert rate card
                            cursor.execute('''
                                INSERT INTO dhl_express_rate_cards 
                                (service_type, rate_section, weight_from, weight_to, 
                                 zone_1, zone_2, zone_3, zone_4, zone_5, zone_6, zone_7, zone_8, zone_9,
                                 zone_10, zone_11, zone_12, zone_13, zone_14, zone_15, zone_16, zone_17, zone_18, zone_19,
                                 is_multiplier, weight_range_from, weight_range_to, created_timestamp)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (service_type, 'Documents', weight_from, weight_to, *zone_rates, 0, weight_from, weight_to, current_timestamp))
                            
                            loaded_count += 1
                            
                        except:
                            continue
        
        # Process Non-Documents section  
        if non_docs_start:
            for idx in range(non_docs_start, min(non_docs_start + 100, len(df))):
                if idx < len(df):
                    row = df.iloc[idx]
                    first_col = row[0]
                    
                    if pd.notna(first_col):
                        try:
                            weight_from = float(first_col)
                            if weight_from > 30:  # Stop at multiplier section
                                break
                                
                            # Extract weight range
                            weight_to = weight_from + 0.5  # Default increment
                            
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
                            
                            # Insert rate card
                            cursor.execute('''
                                INSERT INTO dhl_express_rate_cards 
                                (service_type, rate_section, weight_from, weight_to, 
                                 zone_1, zone_2, zone_3, zone_4, zone_5, zone_6, zone_7, zone_8, zone_9,
                                 zone_10, zone_11, zone_12, zone_13, zone_14, zone_15, zone_16, zone_17, zone_18, zone_19,
                                 is_multiplier, weight_range_from, weight_range_to, created_timestamp)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (service_type, 'Non-documents', weight_from, weight_to, *zone_rates, 0, weight_from, weight_to, current_timestamp))
                            
                            loaded_count += 1
                            
                        except:
                            continue
        
        print(f"  📋 Loaded {loaded_count} rate card ranges from Excel")
        if preserved_count > 0:
            print(f"  🔒 Preserved {preserved_count} manually added rate card ranges")
        
        return loaded_count, preserved_count

def test_fixed_loader():
    """Test the fixed loader with current Excel file"""
    
    print("=" * 60)
    print("TESTING FIXED NON-DESTRUCTIVE RATE CARD LOADER")
    print("=" * 60)
    
    # Use specific Excel files for testing
    test_files = [
        ('uploads\\import and export.xlsx', 'Export'),
        ('uploads\\import and export.xlsx', 'Import')
    ]
    
    # Check which files exist
    available_files = []
    excel_file = 'uploads\\import and export.xlsx'
    if os.path.exists(excel_file):
        available_files = [(excel_file, 'Export'), (excel_file, 'Import')]
        print(f"📁 Found combined rate card file: {excel_file}")
    else:
        print(f"⚠️ Missing rate card file: {excel_file}")
    
    if not available_files:
        print("❌ No Excel rate card files found in uploads folder")
        return
    
    # Initialize loader
    loader = FixedNonDestructiveRateCardLoader()
    
    total_multiplier_loaded = 0
    total_cards_loaded = 0
    
    try:
        for excel_file, service_type in available_files:
            print(f"\n{'='*50}")
            print(f"PROCESSING {service_type.upper()} RATE CARD")
            print(f"File: {excel_file}")
            print(f"{'='*50}")
            
            # FIXED: Read the correct sheet for each service type
            if service_type == 'Export':
                sheet_name = 'AU TD Exp WW'
            elif service_type == 'Import':
                sheet_name = 'AU TD Imp WW'
            else:
                sheet_name = 0  # Default to first sheet
            
            try:
                df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
                print(f"📊 Excel sheet '{sheet_name}' has {len(df)} rows and {len(df.columns)} columns")
            except Exception as e:
                print(f"⚠️ Could not read sheet '{sheet_name}', trying default sheet: {e}")
                df = pd.read_excel(excel_file, header=None)
                print(f"📊 Excel has {len(df)} rows (using default sheet)")
            
            # Test multiplier section fix
            print(f"\n{'-'*30}")
            print(f"MULTIPLIER SECTION - {service_type}")
            print(f"{'-'*30}")
            
            multiplier_loaded, multiplier_preserved = loader._update_multiplier_section_fixed(df, service_type, loader.cursor)
            total_multiplier_loaded += multiplier_loaded
            
            # Test rate cards fix
            print(f"\n{'-'*30}")
            print(f"RATE CARDS SECTION - {service_type}")
            print(f"{'-'*30}")
            
            cards_loaded, cards_preserved = loader._update_rate_cards_fixed(df, service_type, loader.cursor)
            total_cards_loaded += cards_loaded
            
            print(f"\n✅ {service_type} Results:")
            print(f"   Multiplier ranges loaded: {multiplier_loaded}")
            print(f"   Rate card ranges loaded: {cards_loaded}")
        
        # Commit all changes
        loader.conn.commit()
        
        print("\n" + "="*60)
        print("FINAL TEST RESULTS")
        print("="*60)
        print(f"✅ Total multiplier ranges loaded: {total_multiplier_loaded}")
        print(f"✅ Total rate card ranges loaded: {total_cards_loaded}")
        
        # Final verification
        for service_type in ['Export', 'Import']:
            loader.cursor.execute('''
                SELECT COUNT(*) FROM dhl_express_rate_cards 
                WHERE service_type = ? AND is_multiplier = 1
            ''', (service_type,))
            multiplier_count = loader.cursor.fetchone()[0]
            
            loader.cursor.execute('''
                SELECT COUNT(*) FROM dhl_express_rate_cards 
                WHERE service_type = ? AND is_multiplier = 0
            ''', (service_type,))
            rate_card_count = loader.cursor.fetchone()[0]
            
            print(f"📊 {service_type} - Multiplier ranges in DB: {multiplier_count}")
            print(f"📊 {service_type} - Rate card ranges in DB: {rate_card_count}")
        
        # Check for duplicates
        print(f"\n{'-'*40}")
        print("DUPLICATE CHECK")
        print(f"{'-'*40}")
        loader.cursor.execute('''
            SELECT service_type, rate_section, weight_from, weight_to, COUNT(*) as count
            FROM dhl_express_rate_cards 
            GROUP BY service_type, rate_section, weight_from, weight_to
            HAVING COUNT(*) > 1
            LIMIT 5
        ''')
        duplicates = loader.cursor.fetchall()
        
        if duplicates:
            print(f"⚠️ Found {len(duplicates)} duplicate weight ranges:")
            for dup in duplicates:
                print(f"   {dup[0]} {dup[1]} {dup[2]}-{dup[3]}kg: {dup[4]} copies")
        else:
            print("✅ No duplicates found!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        loader.conn.rollback()
    finally:
        loader.conn.close()

if __name__ == '__main__':
    test_fixed_loader()
