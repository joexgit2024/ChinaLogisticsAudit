#!/usr/bin/env python3
"""
Load Services & Surcharges from demerged Excel file into database
"""

import pandas as pd
import sqlite3
import re
from datetime import datetime

def parse_net_charge(net_charge_str):
    """Parse the Net Charge string to extract rate and minimum values"""
    if pd.isna(net_charge_str) or str(net_charge_str).strip() == '':
        return None, None, None
    
    charge_str = str(net_charge_str).strip()
    
    # Pattern for charges like "1.00 AUD with minimum of 35.00 AUD"
    min_pattern = r'(\d+\.?\d*)\s*AUD\s*with\s*minimum\s*of\s*(\d+\.?\d*)\s*AUD'
    min_match = re.search(min_pattern, charge_str, re.IGNORECASE)
    
    if min_match:
        rate = float(min_match.group(1))
        minimum = float(min_match.group(2))
        return rate, minimum, 'per_kg_with_minimum'
    
    # Pattern for simple AUD amounts like "65.00 AUD"
    simple_pattern = r'(\d+\.?\d*)\s*AUD'
    simple_match = re.search(simple_pattern, charge_str, re.IGNORECASE)
    
    if simple_match:
        amount = float(simple_match.group(1))
        return amount, None, 'fixed_amount'
    
    # Pattern for percentage charges like "2.00 % with minimum of 30.00 AUD"
    pct_pattern = r'(\d+\.?\d*)\s*%\s*with\s*minimum\s*of\s*(\d+\.?\d*)\s*AUD'
    pct_match = re.search(pct_pattern, charge_str, re.IGNORECASE)
    
    if pct_match:
        percentage = float(pct_match.group(1))
        minimum = float(pct_match.group(2))
        return percentage, minimum, 'percentage_with_minimum'
    
    return None, None, None

def load_services_surcharges():
    """Load services and surcharges from demerged Excel into database"""
    
    try:
        # Read the demerged file
        file_path = 'uploads/ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898_demerged.xlsx'
        df = pd.read_excel(file_path, sheet_name='Services_Demerged')
        
        print(f"Loaded {len(df)} service entries from demerged file")
        
        # Connect to database
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Clear existing services (optional - uncomment if you want to replace all)
        # cursor.execute("DELETE FROM dhl_express_services_surcharges")
        
        # Insert or update services
        inserted_count = 0
        updated_count = 0
        
        for index, row in df.iterrows():
            code = row['Code']
            name = row['Name']
            description = row['Description']
            price_mechanism = row['Price Mechanism']
            net_charge = row['Net Charge']
            products_applicable = row['Products Applicable']
            
            # Skip invalid entries
            if pd.isna(code) or code == 'Code':  # Skip header rows
                continue
                
            # Parse the net charge
            rate, minimum, charge_type = parse_net_charge(net_charge)
            
            # For international vs domestic, we need to handle differently
            # For now, we'll use the minimum value as the charge_amount for simplicity
            if charge_type == 'per_kg_with_minimum':
                charge_amount = minimum  # Use minimum for audit purposes
            elif charge_type == 'fixed_amount':
                charge_amount = rate
            elif charge_type == 'percentage_with_minimum':
                charge_amount = minimum  # Use minimum for audit purposes
            else:
                charge_amount = None
            
            if charge_amount is not None:
                # Check if entry exists
                cursor.execute('''
                    SELECT id FROM dhl_express_services_surcharges 
                    WHERE service_code = ? AND service_name = ?
                ''', (code, name))
                
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing entry
                    cursor.execute('''
                        UPDATE dhl_express_services_surcharges 
                        SET charge_amount = ?
                        WHERE service_code = ? AND service_name = ?
                    ''', (charge_amount, code, name))
                    updated_count += 1
                    print(f"Updated {code} - {name}: ${charge_amount}")
                else:
                    # Insert new entry
                    cursor.execute('''
                        INSERT INTO dhl_express_services_surcharges (
                            service_type, service_code, service_name, charge_amount,
                            currency, is_special_agreement, created_timestamp
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        'Import',  # Default service type
                        code,
                        name,
                        charge_amount,
                        'AUD',
                        0,  # Not special agreement
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ))
                    inserted_count += 1
                    print(f"Inserted {code} - {name}: ${charge_amount}")
        
        conn.commit()
        print(f"\nSummary:")
        print(f"  Inserted: {inserted_count} new services")
        print(f"  Updated: {updated_count} existing services")
        
        # Verify REMOTE AREA DELIVERY specifically
        print(f"\nVerifying REMOTE AREA DELIVERY (OO) entries:")
        cursor.execute('''
            SELECT service_code, service_name, charge_amount 
            FROM dhl_express_services_surcharges 
            WHERE service_code = 'OO'
        ''')
        
        oo_results = cursor.fetchall()
        for result in oo_results:
            print(f"  {result[0]} - {result[1]}: ${result[2]}")
        
        conn.close()
        print(f"\nServices & Surcharges loaded successfully!")
        
    except Exception as e:
        print(f"Error loading services: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    load_services_surcharges()
