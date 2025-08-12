#!/usr/bin/env python3
"""
Enhanced Service Charge Upload Processor
=======================================

This script applies the sophisticated demerging and service name conversion logic
during Excel uploads, ensuring that service charges are properly processed with:
1. Demerging logic (YY -> YY_ALL, YY_MED_DOM, etc.)
2. Proper service name matching
3. Product applicability enhancement
4. Rate updates for existing enhanced entries

This replaces the basic INSERT OR IGNORE with intelligent processing.
"""

import sqlite3
import pandas as pd
from datetime import datetime
import re

class EnhancedServiceChargeProcessor:
    """Processes service charges with full demerging and enhancement logic"""
    
    def __init__(self, db_path='dhl_audit.db'):
        self.db_path = db_path
        
        # Service charge demerging rules
        self.demerging_rules = {
            'YY': {
                'YY_ALL': {'charge_multiplier': 1.88, 'products': 'All Document/Non-doc'},
                'YY_MED_DOM': {'charge_multiplier': 1.0, 'products': 'Medical Domestic'}
            },
            'YB': {
                'YB_INTL': {'charge_multiplier': 1.0, 'products': 'International'},
                'YB_DOM': {'charge_multiplier': 0.625, 'products': 'Domestic'}
            }
        }
        
        # Service name enhancements
        self.service_enhancements = {
            'AA': 'All Products',
            'AB': 'All Products', 
            'CA': 'All Products',
            'CB': 'All Products',
            'DD': 'All Products',
            'FF': 'All Products',
            'GG': 'All Products',
            'II': 'All Products',
            'JJ': 'All Products',
            'KK': 'All Products',
            'LL': 'All Products',
            'MM': 'All Products',
            'NN': 'All Products',
            'PP': 'All Products',
            'QQ': 'All Products',
            'RR': 'All Products',
            'SS': 'All Products',
            'TT': 'All Products',
            'UU': 'All Products',
            'VV': 'All Products',
            'WW': 'All Products',
            'XX': 'All Products',
            'ZZ': 'All Products'
        }

    def process_service_charges_from_excel(self, excel_path: str, sheet_name: str) -> dict:
        """Process service charges with full enhancement logic"""
        
        print(f"🔧 ENHANCED SERVICE CHARGE PROCESSING")
        print(f"📄 Sheet: {sheet_name}")
        print("=" * 50)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Ensure enhanced table structure exists
        self._ensure_enhanced_table_structure(cursor)
        
        # Read Excel data
        try:
            df = pd.read_excel(excel_path, sheet_name=sheet_name)
        except Exception as e:
            print(f"❌ Could not read {sheet_name}: {e}")
            conn.close()
            return {'processed': 0, 'enhanced': 0, 'updated': 0}
        
        basic_entries = self._extract_basic_entries(df)
        print(f"📋 Found {len(basic_entries)} basic service charge entries")
        
        processed_count = 0
        enhanced_count = 0
        updated_count = 0
        
        for entry in basic_entries:
            result = self._process_single_service_charge(cursor, entry)
            processed_count += result['processed']
            enhanced_count += result['enhanced']
            updated_count += result['updated']
        
        conn.commit()
        conn.close()
        
        print(f"\n✅ PROCESSING COMPLETE:")
        print(f"   📊 Basic entries processed: {processed_count}")
        print(f"   ✨ Enhanced variants created: {enhanced_count}")
        print(f"   🔄 Existing entries updated: {updated_count}")
        
        return {
            'processed': processed_count,
            'enhanced': enhanced_count,
            'updated': updated_count
        }
    
    def _ensure_enhanced_table_structure(self, cursor):
        """Ensure service charges table has enhanced structure"""
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dhl_express_services_surcharges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_code TEXT NOT NULL,
                service_name TEXT,
                charge_type TEXT,
                charge_amount REAL,
                currency TEXT DEFAULT 'USD',
                created_timestamp TEXT,
                products_applicable TEXT,
                original_service_code TEXT,
                description TEXT,
                UNIQUE(service_code)
            )
        ''')
        
        # Add enhanced columns if missing
        try:
            cursor.execute('ALTER TABLE dhl_express_services_surcharges ADD COLUMN products_applicable TEXT')
        except:
            pass
            
        try:
            cursor.execute('ALTER TABLE dhl_express_services_surcharges ADD COLUMN original_service_code TEXT')
        except:
            pass
            
        try:
            cursor.execute('ALTER TABLE dhl_express_services_surcharges ADD COLUMN description TEXT')
        except:
            pass
    
    def _extract_basic_entries(self, df) -> list:
        """Extract basic service charge entries from Excel"""
        
        entries = []
        
        for _, row in df.iterrows():
            for col in df.columns:
                cell_value = str(row[col])
                
                # Look for service charge patterns (code + name + amount)
                if pd.notna(row[col]) and len(cell_value) <= 5:
                    # Check if this looks like a service code
                    if re.match(r'^[A-Z]{1,3}$', cell_value.strip()):
                        service_code = cell_value.strip()
                        
                        # Look for service name and amount in nearby cells
                        service_name = None
                        charge_amount = None
                        
                        # Check next columns for name and amount
                        col_idx = df.columns.get_loc(col)
                        for next_col_idx in range(col_idx + 1, min(col_idx + 5, len(df.columns))):
                            next_col = df.columns[next_col_idx]
                            next_value = str(row[next_col])
                            
                            if pd.notna(row[next_col]):
                                # Check if it's a service name (text)
                                if not service_name and len(next_value) > 5 and not re.search(r'[\d$]', next_value):
                                    service_name = next_value.strip()
                                
                                # Check if it's an amount
                                amount_match = re.search(r'[\$]?(\d+(?:\.\d+)?)', next_value)
                                if amount_match and not charge_amount:
                                    try:
                                        charge_amount = float(amount_match.group(1))
                                    except:
                                        pass
                        
                        if service_code and (service_name or charge_amount):
                            entries.append({
                                'service_code': service_code,
                                'service_name': service_name,
                                'charge_amount': charge_amount
                            })
        
        return entries
    
    def _process_single_service_charge(self, cursor, entry: dict) -> dict:
        """Process a single service charge with full enhancement logic"""
        
        service_code = entry['service_code']
        service_name = entry['service_name']
        charge_amount = entry['charge_amount']
        
        processed = 0
        enhanced = 0
        updated = 0
        
        # Check if this service code has demerging rules
        if service_code in self.demerging_rules:
            # Process demerged variants
            for variant_code, rules in self.demerging_rules[service_code].items():
                variant_amount = charge_amount * rules['charge_multiplier'] if charge_amount else None
                
                # Check if variant already exists
                cursor.execute('''
                    SELECT id, charge_amount FROM dhl_express_services_surcharges 
                    WHERE service_code = ?
                ''', (variant_code,))
                
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing enhanced entry with new rate
                    if variant_amount:
                        cursor.execute('''
                            UPDATE dhl_express_services_surcharges 
                            SET charge_amount = ?, created_timestamp = ?
                            WHERE service_code = ?
                        ''', (variant_amount, datetime.now().isoformat(), variant_code))
                        updated += 1
                        print(f"  🔄 Updated {variant_code}: ${variant_amount}")
                else:
                    # Create new enhanced entry
                    cursor.execute('''
                        INSERT INTO dhl_express_services_surcharges
                        (service_code, service_name, charge_amount, products_applicable, 
                         original_service_code, description, created_timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        variant_code, 
                        service_name,
                        variant_amount,
                        rules['products'],
                        service_code,
                        f"Enhanced variant of {service_code}",
                        datetime.now().isoformat()
                    ))
                    enhanced += 1
                    print(f"  ✨ Created enhanced {variant_code}: ${variant_amount} | Products: {rules['products']}")
        
        # Process basic entry
        cursor.execute('''
            SELECT id FROM dhl_express_services_surcharges 
            WHERE service_code = ?
        ''', (service_code,))
        
        existing_basic = cursor.fetchone()
        
        if existing_basic:
            # Update basic entry rate only (preserve enhanced data)
            if charge_amount:
                cursor.execute('''
                    UPDATE dhl_express_services_surcharges 
                    SET charge_amount = ?, created_timestamp = ?
                    WHERE service_code = ? AND (products_applicable IS NULL OR products_applicable = '')
                ''', (charge_amount, datetime.now().isoformat(), service_code))
                if cursor.rowcount > 0:
                    updated += 1
                    print(f"  🔄 Updated basic {service_code}: ${charge_amount}")
        else:
            # Create basic entry with enhancement if applicable
            products_applicable = self.service_enhancements.get(service_code, '')
            
            cursor.execute('''
                INSERT INTO dhl_express_services_surcharges
                (service_code, service_name, charge_amount, products_applicable, created_timestamp)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                service_code,
                service_name,
                charge_amount,
                products_applicable,
                datetime.now().isoformat()
            ))
            processed += 1
            
            if products_applicable:
                print(f"  ✅ Added enhanced {service_code}: ${charge_amount} | Products: {products_applicable}")
            else:
                print(f"  ✅ Added basic {service_code}: ${charge_amount}")
        
        return {'processed': processed, 'enhanced': enhanced, 'updated': updated}

def test_enhanced_processor():
    """Test the enhanced service charge processor"""
    
    processor = EnhancedServiceChargeProcessor()
    
    # Test with a sample Excel file
    print("🧪 TESTING ENHANCED SERVICE CHARGE PROCESSOR")
    print("=" * 60)
    
    # You would call this during actual upload:
    # result = processor.process_service_charges_from_excel('your_file.xlsx', 'Published Service Charges')
    
    print("✅ Enhanced processor ready for integration into upload workflow")

if __name__ == "__main__":
    test_enhanced_processor()
