#!/usr/bin/env python3
"""
Enhanced Auto-convert S&S Published tab with support for merged entries
Handles service charges that have multiple rates for different product categories
"""

import pandas as pd
import sqlite3
import re
from datetime import datetime

def parse_net_charge(net_charge_str):
    """Parse the net charge string to extract amount, minimum, and percentage"""
    if pd.isna(net_charge_str) or net_charge_str == 'Variable':
        return None, None, None
    
    net_charge_str = str(net_charge_str).strip()
    
    # Handle percentage with minimum (e.g., "2.00 % with minimum of 30.00 AUD")
    pct_min_match = re.search(r'(\d+\.?\d*)\s*%\s*with\s+minimum\s+of\s+(\d+\.?\d*)\s*AUD', net_charge_str, re.IGNORECASE)
    if pct_min_match:
        percentage = float(pct_min_match.group(1))
        minimum = float(pct_min_match.group(2))
        return None, minimum, percentage
    
    # Handle simple percentage (e.g., "2.5%")
    pct_match = re.search(r'(\d+\.?\d*)\s*%', net_charge_str, re.IGNORECASE)
    if pct_match:
        percentage = float(pct_match.group(1))
        return None, None, percentage
    
    # Handle amount with minimum (e.g., "1.00 AUD per kg with minimum of 35.00 AUD")
    amt_min_match = re.search(r'(\d+\.?\d*)\s*AUD.*?minimum\s+of\s+(\d+\.?\d*)\s*AUD', net_charge_str, re.IGNORECASE)
    if amt_min_match:
        amount = float(amt_min_match.group(1))
        minimum = float(amt_min_match.group(2))
        return amount, minimum, None
    
    # Handle simple amount (e.g., "65.00 AUD", "1.00 AUD per kg")
    amt_match = re.search(r'(\d+\.?\d*)\s*AUD', net_charge_str, re.IGNORECASE)
    if amt_match:
        amount = float(amt_match.group(1))
        return amount, None, None
    
    return None, None, None

def determine_charge_type(price_mechanism, net_charge_str):
    """Determine the charge type based on price mechanism and net charge description"""
    if pd.isna(price_mechanism):
        return 'rate per shipment'
    
    price_mechanism = str(price_mechanism).lower().strip()
    net_charge_str = str(net_charge_str).lower() if not pd.isna(net_charge_str) else ''
    
    if 'per kg' in price_mechanism or 'per kg' in net_charge_str:
        return 'rate per kg'
    elif 'per piece' in price_mechanism or 'per piece' in net_charge_str:
        return 'rate per piece'
    elif '% of' in price_mechanism or 'percentage' in price_mechanism:
        return '% of fiscal charges' if 'fiscal' in price_mechanism else '% of insured value'
    elif 'per shipment' in price_mechanism:
        return 'rate per shipment'
    else:
        return 'rate per shipment'  # Default

def convert_ss_published_to_service_charges_enhanced(excel_file_path, output_csv=None):
    """Enhanced converter that handles merged entries with multiple rates per service code"""
    
    print("=== Enhanced S&S Published Converter (Handles Merged Entries) ===")
    print(f"Reading from: {excel_file_path}")
    
    try:
        # Read the S&S Published tab without headers first
        df = pd.read_excel(excel_file_path, sheet_name='S&S Published', header=None)
        print(f"Loaded S&S Published tab: {df.shape}")
        
        # Find the header row
        header_row = None
        for idx, row in df.iterrows():
            if str(row.iloc[0]).strip() == 'Code':
                header_row = idx
                break
        
        if header_row is None:
            print("❌ Could not find 'Code' header row")
            return None
        
        print(f"Found header row at index: {header_row}")
        
        # Extract column names from header row
        headers = ['Code', 'Name', 'Description', 'Unknown', 'Price_Mechanism', 'Net_Charge', 'Products_Applicable']
        
        # Get data rows (after header)
        data_rows = df.iloc[header_row + 1:]
        
        # Process each row and handle merged entries
        service_charges = []
        i = 0
        
        while i < len(data_rows):
            row = data_rows.iloc[i]
            
            # Check if this is a main service code row
            service_code = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ''
            
            if len(service_code) == 2 and service_code.isalnum():
                # This is a main service code row
                service_name = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ''
                description = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else ''
                price_mechanism = str(row.iloc[4]).strip() if pd.notna(row.iloc[4]) else ''
                
                # Collect all rate/product combinations for this service code
                rate_combinations = []
                
                # First combination from the main row
                net_charge = str(row.iloc[5]).strip() if pd.notna(row.iloc[5]) else ''
                products_applicable = str(row.iloc[6]).strip() if pd.notna(row.iloc[6]) else ''
                
                if net_charge and net_charge != 'nan':
                    rate_combinations.append({
                        'net_charge': net_charge,
                        'products_applicable': products_applicable
                    })
                
                # Look for continuation rows
                j = i + 1
                while j < len(data_rows):
                    next_row = data_rows.iloc[j]
                    next_code = str(next_row.iloc[0]).strip() if pd.notna(next_row.iloc[0]) else ''
                    
                    # If next row has empty or NaN in first column, it's a continuation
                    if next_code == '' or next_code == 'nan':
                        continuation_net_charge = str(next_row.iloc[5]).strip() if pd.notna(next_row.iloc[5]) else ''
                        continuation_products = str(next_row.iloc[6]).strip() if pd.notna(next_row.iloc[6]) else ''
                        
                        if continuation_net_charge and continuation_net_charge != 'nan':
                            rate_combinations.append({
                                'net_charge': continuation_net_charge,
                                'products_applicable': continuation_products
                            })
                        j += 1
                    else:
                        # Next row is a new service code, stop looking for continuations
                        break
                
                # Create separate database entries for each rate/product combination
                for combo in rate_combinations:
                    # Parse net charge to extract components
                    charge_amount, minimum_charge, percentage_rate = parse_net_charge(combo['net_charge'])
                    
                    # Determine charge type
                    charge_type = determine_charge_type(price_mechanism, combo['net_charge'])
                    
                    # Create unique service code for database (append suffix if multiple rates)
                    if len(rate_combinations) == 1:
                        db_service_code = service_code
                    else:
                        # Create unique identifiers for multiple rates
                        suffix_map = {
                            'International': '_INTL',
                            'Domestic': '_DOM',
                            'All Products': '_ALL',
                            'MEDICAL EXPRESS (domestic), JETLINE (domestic), EXPRESS DOMESTIC, EXPRESS DOMESTIC 9:00, EXPRESS DOMESTIC 12:00': '_MED_DOM'
                        }
                        
                        suffix = suffix_map.get(combo['products_applicable'], f"_{len(service_charges) + 1}")
                        db_service_code = f"{service_code}{suffix}"
                    
                    service_charge = {
                        'service_code': db_service_code,
                        'service_name': service_name,
                        'description': description,
                        'charge_type': charge_type,
                        'charge_amount': charge_amount,
                        'minimum_charge': minimum_charge,
                        'percentage_rate': percentage_rate,
                        'products_applicable': combo['products_applicable'],
                        'price_mechanism': price_mechanism,
                        'net_charge_original': combo['net_charge'],
                        'created_timestamp': datetime.now().isoformat(),
                        'original_service_code': service_code  # Keep original for reference
                    }
                    
                    service_charges.append(service_charge)
                
                # Skip to next service code (j is already positioned correctly)
                i = j
                
            else:
                # Skip non-service code rows
                i += 1
        
        print(f"\\n✅ Extracted {len(service_charges)} service charge entries (including merged variations)")
        
        # Show sample results including merged entries
        print("\\n=== Sample Extracted Service Charges (Enhanced) ===")
        for i, charge in enumerate(service_charges[:10]):
            original_code = charge.get('original_service_code', charge['service_code'])
            print(f"{i+1}. {charge['service_code']} (orig: {original_code}): {charge['service_name']}")
            print(f"   Products: {charge['products_applicable']}")
            print(f"   Type: {charge['charge_type']}")
            if charge['charge_amount']:
                print(f"   Amount: ${charge['charge_amount']}")
            if charge['minimum_charge']:
                print(f"   Minimum: ${charge['minimum_charge']}")
            if charge['percentage_rate']:
                print(f"   Percentage: {charge['percentage_rate']}%")
            print(f"   Original: {charge['net_charge_original']}")
            print()
        
        # Save to CSV if requested
        if output_csv:
            df_charges = pd.DataFrame(service_charges)
            df_charges.to_csv(output_csv, index=False)
            print(f"💾 Saved to CSV: {output_csv}")
        
        return service_charges
        
    except Exception as e:
        print(f"❌ Error processing file: {e}")
        import traceback
        traceback.print_exc()
        return None

def load_service_charges_to_database_enhanced(service_charges, db_path='dhl_audit.db'):
    """Load the converted service charges into the database with enhanced structure"""
    
    if not service_charges:
        print("❌ No service charges to load")
        return False
    
    print(f"\\n=== Loading {len(service_charges)} Enhanced Service Charges to Database ===")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create enhanced table structure if needed
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dhl_express_services_surcharges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_code VARCHAR(15) UNIQUE,  -- Extended for suffixes
                service_name VARCHAR(100),
                description TEXT,
                charge_type VARCHAR(50),
                charge_amount DECIMAL(10,2),
                minimum_charge DECIMAL(10,2),
                percentage_rate DECIMAL(5,2),
                products_applicable TEXT,
                original_service_code VARCHAR(10),  -- New field for original code
                created_timestamp DATETIME
            )
        ''')
        
        # Load each service charge using INSERT OR REPLACE to be non-destructive
        loaded_count = 0
        updated_count = 0
        
        for charge in service_charges:
            # Check if service code already exists
            cursor.execute('SELECT COUNT(*) FROM dhl_express_services_surcharges WHERE service_code = ?', 
                          (charge['service_code'],))
            exists = cursor.fetchone()[0] > 0
            
            cursor.execute('''
                INSERT OR REPLACE INTO dhl_express_services_surcharges 
                (service_code, service_name, description, charge_type, charge_amount, 
                 minimum_charge, percentage_rate, products_applicable, original_service_code, created_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                charge['service_code'],
                charge['service_name'],
                charge['description'],
                charge['charge_type'],
                charge['charge_amount'],
                charge['minimum_charge'],
                charge['percentage_rate'],
                charge['products_applicable'],
                charge.get('original_service_code', charge['service_code']),
                charge['created_timestamp']
            ))
            
            if exists:
                updated_count += 1
            else:
                loaded_count += 1
        
        conn.commit()
        conn.close()
        
        print(f"✅ Successfully loaded {loaded_count} new service charge entries")
        print(f"✅ Successfully updated {updated_count} existing service charge entries")
        return True
        
    except Exception as e:
        print(f"❌ Error loading to database: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function to convert and load enhanced service charges"""
    
    excel_file = 'uploads/ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx'
    
    # Convert S&S Published tab to service charges format with enhanced handling
    service_charges = convert_ss_published_to_service_charges_enhanced(excel_file)
    
    if service_charges:
        # Load to database
        load_service_charges_to_database_enhanced(service_charges)
        
        # Save to CSV for inspection
        csv_file = 'enhanced_converted_service_charges.csv'
        pd.DataFrame(service_charges).to_csv(csv_file, index=False)
        print(f"\\n💾 Also saved to CSV for inspection: {csv_file}")
    
    print("\\n=== Enhanced Conversion Complete ===")

if __name__ == '__main__':
    main()
