#!/usr/bin/env python3
"""
Auto-convert S&S Published tab from rate card Excel to service charges database format
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

def convert_ss_published_to_service_charges(excel_file_path, output_csv=None):
    """Convert S&S Published tab to service charges format"""
    
    print("=== Converting S&S Published to Service Charges Format ===")
    print(f"Reading from: {excel_file_path}")
    
    try:
        # Read the S&S Published tab
        df = pd.read_excel(excel_file_path, sheet_name='S&S Published')
        print(f"Loaded S&S Published tab: {df.shape}")
        
        # The actual data starts around row 5 (0-indexed), after headers
        # Look for the "Code" column header
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
        headers = []
        for col in df.iloc[header_row]:
            if pd.notna(col):
                headers.append(str(col).strip())
            else:
                headers.append('Unknown')
        
        print(f"Headers: {headers}")
        
        # Get data rows (after header)
        data_rows = df.iloc[header_row + 1:]
        
        # Create new dataframe with proper headers
        service_charges = []
        
        for idx, row in data_rows.iterrows():
            # Skip empty rows
            if pd.isna(row.iloc[0]) or str(row.iloc[0]).strip() == '':
                continue
            
            service_code = str(row.iloc[0]).strip()
            service_name = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ''
            description = str(row.iloc[2]).strip() if pd.notna(row.iloc[2]) else ''
            price_mechanism = str(row.iloc[4]).strip() if pd.notna(row.iloc[4]) else ''
            net_charge = str(row.iloc[5]).strip() if pd.notna(row.iloc[5]) else ''
            products_applicable = str(row.iloc[6]).strip() if pd.notna(row.iloc[6]) else ''
            
            # Skip if no valid service code (likely a section header)
            if len(service_code) != 2 or not service_code.isalnum():
                continue
            
            # Parse net charge to extract components
            charge_amount, minimum_charge, percentage_rate = parse_net_charge(net_charge)
            
            # Determine charge type
            charge_type = determine_charge_type(price_mechanism, net_charge)
            
            service_charge = {
                'service_code': service_code,
                'service_name': service_name,
                'description': description,
                'charge_type': charge_type,
                'charge_amount': charge_amount,
                'minimum_charge': minimum_charge,
                'percentage_rate': percentage_rate,
                'products_applicable': products_applicable,
                'price_mechanism': price_mechanism,
                'net_charge_original': net_charge,
                'created_timestamp': datetime.now().isoformat()
            }
            
            service_charges.append(service_charge)
        
        print(f"\\n✅ Extracted {len(service_charges)} service charges")
        
        # Convert to DataFrame for easier handling
        df_charges = pd.DataFrame(service_charges)
        
        # Show sample results
        print("\\n=== Sample Extracted Service Charges ===")
        for i, charge in enumerate(service_charges[:5]):
            print(f"{i+1}. {charge['service_code']}: {charge['service_name']}")
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
            df_charges.to_csv(output_csv, index=False)
            print(f"💾 Saved to CSV: {output_csv}")
        
        return service_charges
        
    except Exception as e:
        print(f"❌ Error processing file: {e}")
        import traceback
        traceback.print_exc()
        return None

def load_service_charges_to_database(service_charges, db_path='dhl_audit.db'):
    """Load the converted service charges into the database"""
    
    if not service_charges:
        print("❌ No service charges to load")
        return False
    
    print(f"\\n=== Loading {len(service_charges)} Service Charges to Database ===")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Create table if it doesn't exist (should already exist)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dhl_express_services_surcharges (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                service_code VARCHAR(10) UNIQUE,
                service_name VARCHAR(100),
                description TEXT,
                charge_type VARCHAR(50),
                charge_amount DECIMAL(10,2),
                minimum_charge DECIMAL(10,2),
                percentage_rate DECIMAL(5,2),
                products_applicable TEXT,
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
                 minimum_charge, percentage_rate, products_applicable, created_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                charge['service_code'],
                charge['service_name'],
                charge['description'],
                charge['charge_type'],
                charge['charge_amount'],
                charge['minimum_charge'],
                charge['percentage_rate'],
                charge['products_applicable'],
                charge['created_timestamp']
            ))
            
            if exists:
                updated_count += 1
            else:
                loaded_count += 1
        
        conn.commit()
        conn.close()
        
        print(f"✅ Successfully loaded {loaded_count} new service charges")
        print(f"✅ Successfully updated {updated_count} existing service charges")
        return True
        
    except Exception as e:
        print(f"❌ Error loading to database: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main function to convert and load service charges"""
    
    excel_file = 'uploads/ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx'
    
    # Convert S&S Published tab to service charges format
    service_charges = convert_ss_published_to_service_charges(excel_file)
    
    if service_charges:
        # Load to database
        load_service_charges_to_database(service_charges)
        
        # Optional: Save to CSV for inspection
        csv_file = 'converted_service_charges.csv'
        pd.DataFrame(service_charges).to_csv(csv_file, index=False)
        print(f"\\n💾 Also saved to CSV for inspection: {csv_file}")
    
    print("\\n=== Conversion Complete ===")

if __name__ == '__main__':
    main()
