#!/usr/bin/env python3
"""
AU DHL Express Domestic Rate Card Loader
Loads zones, matrix, and rates from Excel file into database
"""

import pandas as pd
import sqlite3
import openpyxl
from datetime import datetime
import re

class AUDomesticRateCardLoader:
    def __init__(self):
        self.db_path = 'dhl_audit.db'
        self.conn = None
        
    def connect_db(self):
        """Connect to database"""
        self.conn = sqlite3.connect(self.db_path)
        
    def disconnect_db(self):
        """Disconnect from database"""
        if self.conn:
            self.conn.close()
            
    def load_rate_card(self, file_path: str, uploaded_by: str = "system") -> dict:
        """Load complete AU domestic rate card from Excel file"""
        results = {
            'zones_loaded': 0,
            'matrix_loaded': 0, 
            'rates_loaded': 0,
            'status': 'success',
            'errors': []
        }
        
        try:
            self.connect_db()
            cursor = self.conn.cursor()
            
            # Record upload
            upload_id = self._record_upload(file_path, uploaded_by)
            
            print(f"🔄 Loading AU Domestic Rate Card: {file_path}")
            print("=" * 60)
            
            # 1. Load Zones
            zones_count = self._load_zones(file_path)
            results['zones_loaded'] = zones_count
            print(f"✅ Zones loaded: {zones_count}")
            
            # 2. Load Matrix  
            matrix_count = self._load_matrix(file_path)
            results['matrix_loaded'] = matrix_count
            print(f"✅ Matrix loaded: {matrix_count}")
            
            # 3. Load Rates
            rates_count = self._load_rates(file_path)
            results['rates_loaded'] = rates_count
            print(f"✅ Rates loaded: {rates_count}")
            
            # Update upload record
            self._update_upload_status(upload_id, 'completed', results)
            
            self.conn.commit()
            print(f"\n🎯 AU Domestic Rate Card loaded successfully!")
            
        except Exception as e:
            results['status'] = 'error'
            results['errors'].append(str(e))
            print(f"❌ Error loading rate card: {e}")
            if self.conn:
                self.conn.rollback()
                
        finally:
            self.disconnect_db()
            
        return results
    
    def _record_upload(self, file_path: str, uploaded_by: str) -> int:
        """Record the upload in database"""
        cursor = self.conn.cursor()
        filename = file_path.split('\\')[-1]
        
        cursor.execute('''
            INSERT INTO dhl_express_au_domestic_uploads 
            (filename, uploaded_by) VALUES (?, ?)
        ''', (filename, uploaded_by))
        
        return cursor.lastrowid
    
    def _update_upload_status(self, upload_id: int, status: str, results: dict):
        """Update upload status"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE dhl_express_au_domestic_uploads 
            SET processed_date = CURRENT_TIMESTAMP,
                status = ?,
                zones_loaded = ?,
                matrix_loaded = ?,
                rates_loaded = ?
            WHERE id = ?
        ''', (status, results['zones_loaded'], results['matrix_loaded'], 
              results['rates_loaded'], upload_id))
    
    def _load_zones(self, file_path: str) -> int:
        """Load zone mappings from AU Zones TD Dom sheet"""
        df = pd.read_excel(file_path, sheet_name='AU Zones TD Dom', header=None)
        cursor = self.conn.cursor()
        count = 0
        
        # Find zone mapping data starting from row that contains "Countries & Territories"
        zone_start_row = None
        service_area_start_row = None
        
        for i, row in df.iterrows():
            if any('Countries & Territories' in str(cell) for cell in row if pd.notna(cell)):
                if zone_start_row is None:
                    zone_start_row = i
                elif service_area_start_row is None:
                    service_area_start_row = i
                    break
                
        # Load zone mappings (Australia *1 = Zone 1, etc.)
        if zone_start_row is not None:
            for i in range(zone_start_row + 1, service_area_start_row or len(df)):
                row = df.iloc[i]
                for col_idx in range(0, len(row), 3):  # Every 3 columns: country, zone, blank
                    country = row.iloc[col_idx] if col_idx < len(row) else None
                    zone = row.iloc[col_idx + 1] if col_idx + 1 < len(row) else None
                    
                    if pd.notna(country) and pd.notna(zone) and 'Australia (AU)' in str(country):
                        # Extract zone number from "Australia (AU) *1" format
                        zone_match = re.search(r'\*(\d+)', str(country))
                        if zone_match:
                            zone_number = int(zone_match.group(1))
                            
                            # Use INSERT OR REPLACE pattern (just create the zone record)
                            cursor.execute('''
                                INSERT OR IGNORE INTO dhl_express_au_domestic_zones 
                                (zone_number, updated_timestamp) VALUES (?, CURRENT_TIMESTAMP)
                            ''', (zone_number,))
                            count += 1
        
        # Load service area mappings (Melbourne = Zone 1, etc.)
        if service_area_start_row is not None:
            for i in range(service_area_start_row + 1, len(df)):
                row = df.iloc[i]
                country = row.iloc[0] if len(row) > 0 else None
                service_area = row.iloc[1] if len(row) > 1 else None
                
                if pd.notna(country) and pd.notna(service_area) and 'Australia (AU)' in str(country):
                    # Extract zone number from "Australia (AU) *1"
                    zone_match = re.search(r'\*(\d+)', str(country))
                    if zone_match:
                        zone_number = int(zone_match.group(1))
                        
                        # Parse service area "Melbourne (MEL)" or "Rest of Australia (AU)"
                        city_name = None
                        city_code = None
                        
                        service_area_str = str(service_area)
                        if '(' in service_area_str and ')' in service_area_str:
                            city_name = service_area_str.split('(')[0].strip()
                            city_code = service_area_str.split('(')[1].split(')')[0].strip()
                        else:
                            city_name = service_area_str.strip()
                        
                        # Use UPDATE to add service area details to existing zone
                        cursor.execute('''
                            UPDATE dhl_express_au_domestic_zones 
                            SET city_name = ?, city_code = ?, service_area = ?, updated_timestamp = CURRENT_TIMESTAMP
                            WHERE zone_number = ?
                        ''', (city_name, city_code, service_area_str, zone_number))
        
        # Commit the transaction
        self.conn.commit()
        return count
    
    def _load_matrix(self, file_path: str) -> int:
        """Load zone matrix from AU Matrix TD Dom sheet"""
        df = pd.read_excel(file_path, sheet_name='AU Matrix TD Dom', header=None)
        cursor = self.conn.cursor()
        count = 0
        
        # Find matrix data - look for "Destination zone" header
        matrix_start_row = None
        for i, row in df.iterrows():
            if any('Destination zone' in str(cell) for cell in row if pd.notna(cell)):
                matrix_start_row = i
                break
        
        if matrix_start_row is not None:
            # Get destination zone numbers from the row AFTER the header row
            dest_zones = []
            if matrix_start_row + 1 < len(df):
                header_next_row = df.iloc[matrix_start_row + 1]
                for cell in header_next_row[2:]:  # Skip first 2 columns
                    if pd.notna(cell) and str(cell).isdigit():
                        dest_zones.append(int(cell))
            
            # Process matrix data rows (start from matrix_start_row + 2 to skip header and zone numbers row)
            for i in range(matrix_start_row + 2, len(df)):
                row = df.iloc[i]
                origin_zone = row.iloc[1] if len(row) > 1 else None
                
                if pd.notna(origin_zone) and str(origin_zone).replace('.0', '').isdigit():
                    origin_zone = int(float(origin_zone))
                    
                    # Process each destination zone
                    for j, dest_zone in enumerate(dest_zones):
                        rate_zone_idx = j + 2  # Skip first 2 columns
                        if rate_zone_idx < len(row):
                            rate_zone = row.iloc[rate_zone_idx]
                            if pd.notna(rate_zone):
                                rate_zone = str(rate_zone).strip()
                                
                                # Use INSERT OR REPLACE pattern
                                cursor.execute('''
                                    INSERT OR REPLACE INTO dhl_express_au_domestic_matrix 
                                    (origin_zone, destination_zone, rate_zone, updated_timestamp) 
                                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                                ''', (origin_zone, dest_zone, rate_zone))
                                count += 1
        
        # Commit the transaction
        self.conn.commit()
        return count
    
    def _load_rates(self, file_path: str) -> int:
        """Load rates from AU TD Dom sheet"""
        df = pd.read_excel(file_path, sheet_name='AU TD Dom', header=None)
        cursor = self.conn.cursor()
        count = 0
        
        # Find rate table - look for "KG" and zone headers
        rate_start_row = None
        for i, row in df.iterrows():
            row_values = [str(cell) for cell in row if pd.notna(cell)]
            if any('KG' in val for val in row_values) and any('Zone' in val for val in row_values):
                rate_start_row = i
                break
        
        if rate_start_row is not None:
            # Get zone column mapping from header
            header_row = df.iloc[rate_start_row]
            zone_columns = {}
            for j, cell in enumerate(header_row):
                if pd.notna(cell) and 'Zone' in str(cell):
                    zone_letter = str(cell).split()[-1]  # Get A, B, C, etc.
                    zone_columns[f'zone_{zone_letter.lower()}'] = j
            
            # Process rate rows
            for i in range(rate_start_row + 1, len(df)):
                row = df.iloc[i]
                weight = row.iloc[0] if len(row) > 0 else None
                
                if pd.notna(weight) and self._is_valid_weight(weight):
                    weight_kg = float(weight)
                    
                    # Extract rates for each zone
                    rate_data = {'weight_kg': weight_kg}
                    for zone_col, col_idx in zone_columns.items():
                        if col_idx < len(row):
                            rate = row.iloc[col_idx]
                            if pd.notna(rate) and self._is_valid_rate(rate):
                                rate_data[zone_col] = float(rate)
                    
                    # Check for above 25kg multiplier (usually around row 60)
                    if i > 55:  # Look in later rows for multiplier
                        multiplier_text = str(row.iloc[0]) if len(row) > 0 else ""
                        if 'above 25kg' in multiplier_text.lower() or 'multiplier' in multiplier_text.lower():
                            # Look for multiplier value in subsequent columns
                            for j in range(1, len(row)):
                                if pd.notna(row.iloc[j]) and self._is_valid_rate(row.iloc[j]):
                                    rate_data['above_25kg_multiplier'] = float(row.iloc[j])
                                    break
                    
                    # Use UPDATE OR INSERT pattern
                    if len(rate_data) > 1:  # More than just weight
                        # Build dynamic update query
                        update_fields = []
                        update_values = []
                        for field, value in rate_data.items():
                            if field != 'weight_kg':
                                update_fields.append(f"{field} = ?")
                                update_values.append(value)
                        
                        if update_fields:
                            update_values.extend([weight_kg, datetime.now().date()])
                            cursor.execute(f'''
                                UPDATE dhl_express_au_domestic_rates 
                                SET {', '.join(update_fields)}, updated_timestamp = CURRENT_TIMESTAMP
                                WHERE weight_kg = ? AND effective_date = ?
                            ''', update_values)
                            
                            if cursor.rowcount == 0:
                                # Insert new record
                                fields = ['weight_kg', 'effective_date'] + list(rate_data.keys())[1:]
                                placeholders = ', '.join(['?'] * len(fields))
                                values = [weight_kg, datetime.now().date()] + list(rate_data.values())[1:]
                                
                                cursor.execute(f'''
                                    INSERT INTO dhl_express_au_domestic_rates 
                                    ({', '.join(fields)}) VALUES ({placeholders})
                                ''', values)
                            count += 1
        
        # Commit the transaction
        self.conn.commit()
        return count
    
    def _is_valid_weight(self, value) -> bool:
        """Check if value is a valid weight"""
        try:
            float_val = float(value)
            return 0 < float_val <= 1000  # Reasonable weight range
        except:
            return False
    
    def _is_valid_rate(self, value) -> bool:
        """Check if value is a valid rate"""
        try:
            float_val = float(value)
            return 0 < float_val <= 10000  # Reasonable rate range
        except:
            return False

def main():
    """Test the loader"""
    loader = AUDomesticRateCardLoader()
    file_path = r'uploads\DHL EXPRESS AU Domestic Cards.xlsx'
    
    results = loader.load_rate_card(file_path, uploaded_by='test_user')
    print(f"\nResults: {results}")

if __name__ == "__main__":
    main()
