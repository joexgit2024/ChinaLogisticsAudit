#!/usr/bin/env python3
"""
DHL Express China Rate Card Loader
=================================

Loads DHL Express rate cards for China from Excel file:
- CN TD IMP WW (Import) - 3 sections
- CN TD EXP WW (Export) - 3 sections

Each with:
1. Documents up to 2.0 KG
2. Non-documents from 0.5 KG & Documents from 2.5 KG  
3. Multiplier rate per 0.5 KG from 30.1 KG

Rates are in CNY (Chinese Yuan) instead of AUD.
"""

import pandas as pd
import sqlite3
import re
from datetime import datetime
from typing import Dict, List, Tuple

class DHLExpressChinaRateCardLoader:
    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path
        
    def clear_existing_rates(self):
        """Clear existing DHL Express rate cards to replace with Chinese ones"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Clear existing rate cards
        cursor.execute('DELETE FROM dhl_express_rate_cards')
        cursor.execute('DELETE FROM dhl_express_rate_cards_backup WHERE 1=1')
        
        conn.commit()
        conn.close()
        print("✅ Cleared existing AU rate cards")
        
    def init_rate_card_table(self):
        """Initialize the DHL Express rate card table with proper structure for China"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if table exists and has the right structure
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dhl_express_rate_cards (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_type VARCHAR(20) NOT NULL,  -- 'Import' or 'Export'
                rate_section VARCHAR(50) NOT NULL,  -- 'Documents', 'Non-documents', 'Multiplier'
                weight_from DECIMAL(10,3) NOT NULL,
                weight_to DECIMAL(10,3) NOT NULL,
                zone_1 DECIMAL(10,2),
                zone_2 DECIMAL(10,2),
                zone_3 DECIMAL(10,2),
                zone_4 DECIMAL(10,2),
                zone_5 DECIMAL(10,2),
                zone_6 DECIMAL(10,2),
                zone_7 DECIMAL(10,2),
                zone_8 DECIMAL(10,2),
                zone_9 DECIMAL(10,2),
                zone_10 DECIMAL(10,2),
                zone_11 DECIMAL(10,2),
                zone_12 DECIMAL(10,2),
                zone_13 DECIMAL(10,2),
                zone_14 DECIMAL(10,2),
                zone_15 DECIMAL(10,2),
                zone_16 DECIMAL(10,2),
                zone_17 DECIMAL(10,2),
                zone_18 DECIMAL(10,2),
                zone_19 DECIMAL(10,2),
                is_multiplier BOOLEAN DEFAULT FALSE,
                weight_range_from DECIMAL(10,3),  -- For multiplier ranges
                weight_range_to DECIMAL(10,3),    -- For multiplier ranges
                created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        print("✅ Initialized DHL Express rate card table structure for China (CNY rates)")
    
    def load_complete_rate_cards(self, excel_path: str) -> Dict:
        """Load complete rate cards from Excel file - interface method for upload handler"""
        return self.load_china_rate_cards(excel_path)
    
    def load_china_rate_cards(self, excel_path: str) -> Dict:
        """Load Chinese rate cards from Excel file"""
        try:
            results = {
                'success': True,
                'import_rates_loaded': 0,
                'export_rates_loaded': 0,
                'sections_processed': 0,
                'errors': [],
                'currency': 'CNY'
            }
            
            # Clear existing AU rates first
            self.clear_existing_rates()
            
            # Initialize table
            self.init_rate_card_table()
            
            # Load Import rates (CN TD IMP WW) 
            import_count = self._load_service_rates(excel_path, 'CN TD Imp WW', 'Import')
            results['import_rates_loaded'] = import_count
            
            # Load Export rates (CN TD EXP WW)  
            export_count = self._load_service_rates(excel_path, 'CN TD Exp WW', 'Export')
            results['export_rates_loaded'] = export_count
            
            results['sections_processed'] = 6  # 3 sections each for Import and Export
            
            return results
            
        except Exception as e:
            return {
                'success': False,
                'error': f'Failed to load Chinese rate cards: {str(e)}',
                'import_rates_loaded': 0,
                'export_rates_loaded': 0
            }
    
    def _load_service_rates(self, excel_path: str, sheet_name: str, service_type: str) -> int:
        """Load rates for a specific service (Import or Export)"""
        print(f"Loading China {service_type} rates from sheet: {sheet_name}")
        
        # Read the Excel sheet
        df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)
        
        total_loaded = 0
        
        # Section 1: Documents up to 2.0 KG
        section1_count = self._load_documents_section(df, service_type)
        total_loaded += section1_count
        print(f"  Loaded {section1_count} Document rates (CNY)")
        
        # Section 2: Non-documents from 0.5 KG & Documents from 2.5 KG
        section2_count = self._load_non_documents_section(df, service_type)
        total_loaded += section2_count
        print(f"  Loaded {section2_count} Non-document rates (CNY)")
        
        # Section 3: Multiplier rate per 0.5 KG from 30.1 KG
        section3_count = self._load_multiplier_section(df, service_type)
        total_loaded += section3_count
        print(f"  Loaded {section3_count} Multiplier rates (CNY)")
        
        return total_loaded
    
    def _load_documents_section(self, df: pd.DataFrame, service_type: str) -> int:
        """Load Section 1: Documents up to 2.0 KG"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Find the "Documents up to 2.0 KG" section
        doc_section_row = None
        for idx, row in df.iterrows():
            if pd.notna(row[0]) and "Documents up to 2.0" in str(row[0]):
                doc_section_row = idx
                break
        
        if doc_section_row is None:
            print(f"  Warning: Documents section not found in {service_type}")
            conn.close()
            return 0
        
        # The header row should be next (KG, Zone 1, Zone 2, etc.)
        header_row = doc_section_row + 1
        
        # Define the weight points for documents section
        doc_weights = [0.5, 1.0, 1.5, 2.0]
        
        loaded_count = 0
        
        for i, weight in enumerate(doc_weights):
            data_row = header_row + 1 + i
            
            if data_row < len(df):
                row_data = df.iloc[data_row]
                
                # Extract zone rates - Chinese sheets may have different zone counts
                zone_rates = []
                max_zones = min(19, len(row_data) - 2)  # Up to 19 zones, starting from column 2
                
                for zone_col in range(2, 2 + max_zones):
                    if zone_col < len(row_data):
                        try:
                            rate = float(row_data[zone_col]) if pd.notna(row_data[zone_col]) else None
                            zone_rates.append(rate)
                        except:
                            zone_rates.append(None)
                    else:
                        zone_rates.append(None)
                
                # Pad or truncate to exactly 19 zones
                while len(zone_rates) < 19:
                    zone_rates.append(None)
                zone_rates = zone_rates[:19]
                
                # Insert the rate
                weight_to = weight if weight < 2.0 else 2.0
                
                cursor.execute('''
                    INSERT INTO dhl_express_rate_cards 
                    (service_type, rate_section, weight_from, weight_to, 
                     zone_1, zone_2, zone_3, zone_4, zone_5, zone_6, zone_7, zone_8, zone_9, zone_10,
                     zone_11, zone_12, zone_13, zone_14, zone_15, zone_16, zone_17, zone_18, zone_19)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (service_type, 'Documents', weight, weight_to, *zone_rates))
                
                loaded_count += 1
        
        conn.commit()
        conn.close()
        return loaded_count
    
    def _load_non_documents_section(self, df: pd.DataFrame, service_type: str) -> int:
        """Load Section 2: Non-documents from 0.5 KG & Documents from 2.5 KG"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Find the "Non-documents from 0.5 KG" section
        non_doc_section_row = None
        for idx, row in df.iterrows():
            if pd.notna(row[0]) and "Non-documents from 0.5" in str(row[0]):
                non_doc_section_row = idx
                break
        
        if non_doc_section_row is None:
            print(f"  Warning: Non-documents section not found in {service_type}")
            conn.close()
            return 0
        
        # The header row should be next
        header_row = non_doc_section_row + 1
        
        loaded_count = 0
        
        # Read until we hit the multiplier section or end of data
        data_start_row = header_row + 1
        
        for row_idx in range(data_start_row, len(df)):
            row_data = df.iloc[row_idx]
            
            # Check if we've hit the multiplier section
            if pd.notna(row_data[0]) and "Multiplier" in str(row_data[0]):
                break
            
            # Skip empty rows
            if pd.isna(row_data[0]):
                continue
                
            try:
                weight = float(row_data[0])
                
                # Extract zone rates - handle variable zone counts
                zone_rates = []
                max_zones = min(19, len(row_data) - 2)
                
                for zone_col in range(2, 2 + max_zones):
                    if zone_col < len(row_data):
                        try:
                            rate = float(row_data[zone_col]) if pd.notna(row_data[zone_col]) else None
                            zone_rates.append(rate)
                        except:
                            zone_rates.append(None)
                    else:
                        zone_rates.append(None)
                
                # Pad or truncate to exactly 19 zones
                while len(zone_rates) < 19:
                    zone_rates.append(None)
                zone_rates = zone_rates[:19]
                
                # Determine weight_to (next weight or same for last entry)
                weight_to = weight + 0.5 if weight < 30.0 else 30.0
                
                cursor.execute('''
                    INSERT INTO dhl_express_rate_cards 
                    (service_type, rate_section, weight_from, weight_to, 
                     zone_1, zone_2, zone_3, zone_4, zone_5, zone_6, zone_7, zone_8, zone_9, zone_10,
                     zone_11, zone_12, zone_13, zone_14, zone_15, zone_16, zone_17, zone_18, zone_19)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (service_type, 'Non-documents', weight, weight_to, *zone_rates))
                
                loaded_count += 1
                
            except ValueError:
                # Skip non-numeric weight values
                continue
        
        conn.commit()
        conn.close()
        return loaded_count
    
    def _load_multiplier_section(self, df: pd.DataFrame, service_type: str) -> int:
        """Load Section 3: Multiplier rate per 0.5 KG from 30.1 KG"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        loaded_count = 0
        
        if service_type == 'Export':
            # Find the "Multiplier rate per 0.5 KG" section
            mult_section_row = None
            for idx, row in df.iterrows():
                if pd.notna(row[0]) and "Multiplier rate per 0.5" in str(row[0]):
                    mult_section_row = idx
                    break
            
            if mult_section_row is None:
                print(f"  Warning: Multiplier section not found in {service_type}")
                conn.close()
                return 0
            
            # Find the header row (From, To, Zone 1, etc.)
            header_row = mult_section_row + 1
            data_start_row = header_row + 1
            
            for row_idx in range(data_start_row, data_start_row + 10):
                if row_idx >= len(df):
                    break
                    
                row_data = df.iloc[row_idx]
                
                # Skip empty rows
                if pd.isna(row_data[0]):
                    continue
                
                try:
                    weight_from = float(row_data[0])
                    weight_to = float(row_data[1]) if pd.notna(row_data[1]) else 99999
                    
                    # Extract zone rates (multiplier rates)
                    zone_rates = []
                    max_zones = min(19, len(row_data) - 2)
                    
                    for zone_col in range(2, 2 + max_zones):
                        if zone_col < len(row_data):
                            try:
                                rate = float(row_data[zone_col]) if pd.notna(row_data[zone_col]) else None
                                zone_rates.append(rate)
                            except:
                                zone_rates.append(None)
                        else:
                            zone_rates.append(None)
                    
                    # Pad or truncate to exactly 19 zones
                    while len(zone_rates) < 19:
                        zone_rates.append(None)
                    zone_rates = zone_rates[:19]
                    
                    cursor.execute('''
                        INSERT INTO dhl_express_rate_cards 
                        (service_type, rate_section, weight_from, weight_to, 
                         zone_1, zone_2, zone_3, zone_4, zone_5, zone_6, zone_7, zone_8, zone_9, zone_10,
                         zone_11, zone_12, zone_13, zone_14, zone_15, zone_16, zone_17, zone_18, zone_19,
                         is_multiplier, weight_range_from, weight_range_to)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (service_type, 'Multiplier', weight_from, weight_to, *zone_rates, 
                         True, weight_from, weight_to))
                    
                    loaded_count += 1
                    
                except ValueError:
                    # Stop when we hit non-numeric data (like Premium section)
                    break
        
        else:  # Import sheet
            # For Import, look for weight ranges that look like multipliers (> 30kg)
            for row_idx in range(len(df)):
                row_data = df.iloc[row_idx]
                
                # Skip empty rows
                if pd.isna(row_data[0]):
                    continue
                
                try:
                    weight_from = float(row_data[0])
                    
                    # Look for multiplier-style ranges (> 30kg and with decimal ranges)
                    if weight_from > 30 and '.' in str(row_data[0]):
                        # This looks like a multiplier range (e.g., 75.1, 100.1, 200.1)
                        if pd.notna(row_data[1]):
                            try:
                                weight_to = float(row_data[1])
                            except:
                                weight_to = 99999
                        else:
                            weight_to = 99999
                        
                        # Extract zone rates
                        zone_rates = []
                        max_zones = min(19, len(row_data) - 2)
                        
                        for zone_col in range(2, 2 + max_zones):
                            if zone_col < len(row_data):
                                try:
                                    rate = float(row_data[zone_col]) if pd.notna(row_data[zone_col]) else None
                                    zone_rates.append(rate)
                                except:
                                    zone_rates.append(None)
                            else:
                                zone_rates.append(None)
                        
                        # Pad or truncate to exactly 19 zones
                        while len(zone_rates) < 19:
                            zone_rates.append(None)
                        zone_rates = zone_rates[:19]
                        
                        cursor.execute('''
                            INSERT INTO dhl_express_rate_cards 
                            (service_type, rate_section, weight_from, weight_to, 
                             zone_1, zone_2, zone_3, zone_4, zone_5, zone_6, zone_7, zone_8, zone_9, zone_10,
                             zone_11, zone_12, zone_13, zone_14, zone_15, zone_16, zone_17, zone_18, zone_19,
                             is_multiplier, weight_range_from, weight_range_to)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (service_type, 'Multiplier', weight_from, weight_to, *zone_rates, 
                             True, weight_from, weight_to))
                        
                        loaded_count += 1
                        
                except ValueError:
                    continue
        
        conn.commit()
        conn.close()
        return loaded_count

def main():
    """Load Chinese DHL Express rate cards"""
    excel_path = r'c:\ChinaLogisticsAudit\uploads\ID_104249_01_AP_V01_Commscope_CN_20241113-074659-898.xlsx'
    
    loader = DHLExpressChinaRateCardLoader()
    
    print("🇨🇳 Loading DHL Express China rate cards...")
    print("=" * 50)
    print(f"📁 File: {excel_path}")
    
    # Check if file exists
    import os
    if not os.path.exists(excel_path):
        print(f"❌ File not found: {excel_path}")
        return
    
    # Check available sheets first
    try:
        excel_file = pd.ExcelFile(excel_path)
        available_sheets = excel_file.sheet_names
        print(f"📋 Available sheets: {available_sheets}")
        
        # Find CN sheets
        cn_sheets = [sheet for sheet in available_sheets if 'CN' in sheet.upper()]
        print(f"🔍 CN sheets found: {cn_sheets}")
        
    except Exception as e:
        print(f"❌ Error reading Excel file: {e}")
        return
    
    results = loader.load_china_rate_cards(excel_path)
    
    if results['success']:
        print(f"✅ Chinese rate card loading completed successfully!")
        print(f"   💰 Currency: {results['currency']}")
        print(f"   📊 Import rates loaded: {results['import_rates_loaded']}")
        print(f"   📊 Export rates loaded: {results['export_rates_loaded']}")
        print(f"   📊 Sections processed: {results['sections_processed']}")
        
        # Verify the loaded data
        print(f"\n📋 Verification:")
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM dhl_express_rate_cards")
        total_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT service_type, rate_section, COUNT(*) FROM dhl_express_rate_cards GROUP BY service_type, rate_section")
        breakdown = cursor.fetchall()
        
        print(f"   📊 Total rate entries: {total_count}")
        for service, section, count in breakdown:
            print(f"   📋 {service} - {section}: {count} entries")
        
        conn.close()
        
        print(f"\n🇨🇳 DHL Express China rate cards are now active for billing audit")
    else:
        print(f"❌ Error loading Chinese rate cards: {results['error']}")

if __name__ == "__main__":
    main()
