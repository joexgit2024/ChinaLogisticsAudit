#!/usr/bin/env python3
"""Update DHL Express rate cards with Import rates from Excel"""

import pandas as pd
import sqlite3
from datetime import datetime

def update_import_rates():
    """Load Import rates from Excel and update the database"""
    
    file_path = 'uploads/ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx'
    sheet_name = 'AU TD Imp WW'
    
    try:
        # Read the Import rates
        print("Loading Import rates from Excel...")
        df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=5)
        
        # Fix column names - Excel has: KG, [empty], Zone 1, Zone 2, ..., Zone 19
        # We want: KG, Zone_1, Zone_2, ..., Zone_19
        column_names = ['KG', 'Empty'] + [f'Zone_{i}' for i in range(1, 20)]
        df.columns = column_names[:len(df.columns)]
        
        # Filter out rows where KG is not numeric
        df_clean = df[pd.to_numeric(df['KG'], errors='coerce').notna()].copy()
        df_clean['KG'] = pd.to_numeric(df_clean['KG'])
        
        # The zone data is already in the correct position since we named columns correctly
        # Zone_1 data comes from the Excel "Zone 1" column (index 2)
        
        print(f"Loaded {len(df_clean)} weight brackets")
        
        # Connect to database
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Clear existing Import Non-documents rates
        print("Clearing existing Import Non-documents rates...")
        cursor.execute("""
            DELETE FROM dhl_express_rate_cards 
            WHERE service_type = 'Import' AND rate_section = 'Non-documents'
        """)
        
        # Insert new rates
        print("Inserting new Import rates...")
        inserted_count = 0
        
        for index, row in df_clean.iterrows():
            weight = row['KG']
            
            # Create weight ranges
            if weight <= 30:
                # For weights up to 30kg, use tight ranges
                # The 30kg entry should only cover 30-30kg (base rate)
                if weight == 30:
                    weight_to = weight  # 30kg is exactly 30kg
                elif index < len(df_clean) - 1:
                    next_weight = df_clean.iloc[index + 1]['KG']
                    # Only extend to next weight if it's also ≤30kg
                    if next_weight <= 30:
                        weight_to = next_weight
                    else:
                        weight_to = weight + 0.5
                else:
                    weight_to = weight + 0.5
                is_multiplier = 0
            else:
                # For weights > 30kg, these are adder rates per 0.5kg
                # Set tight weight ranges so audit engine picks adder rates
                weight_to = weight + 0.5
                is_multiplier = 1
            
            # Prepare zone values (zones 1-19)
            zone_values = []
            for i in range(1, 20):
                zone_col = f'Zone_{i}'
                zone_value = row[zone_col] if pd.notna(row[zone_col]) else None
                zone_values.append(zone_value)
            
            # Insert the rate card entry
            cursor.execute("""
                INSERT INTO dhl_express_rate_cards (
                    service_type, rate_section, weight_from, weight_to,
                    zone_1, zone_2, zone_3, zone_4, zone_5, zone_6, zone_7, zone_8, zone_9,
                    zone_10, zone_11, zone_12, zone_13, zone_14, zone_15, zone_16, zone_17, zone_18, zone_19,
                    is_multiplier, created_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                'Import', 'Non-documents', weight, weight_to
            ] + zone_values + [is_multiplier, datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
            
            inserted_count += 1
        
        conn.commit()
        print(f"Successfully inserted {inserted_count} rate card entries")
        
        # Verify the data
        print("\nVerifying Zone 11 rates for testing...")
        cursor.execute("""
            SELECT weight_from, weight_to, zone_11 
            FROM dhl_express_rate_cards 
            WHERE service_type = 'Import' 
            AND rate_section = 'Non-documents' 
            AND zone_11 IS NOT NULL
            ORDER BY weight_from
            LIMIT 10
        """)
        
        results = cursor.fetchall()
        for result in results:
            print(f"Weight {result[0]}-{result[1]}kg: Zone 11 = ${result[2]:.2f}")
        
        # Test the specific case we need (29kg)
        cursor.execute("""
            SELECT weight_from, weight_to, zone_11 
            FROM dhl_express_rate_cards 
            WHERE service_type = 'Import' 
            AND rate_section = 'Non-documents' 
            AND weight_from <= 29 
            AND weight_to > 29
        """)
        
        result = cursor.fetchone()
        if result:
            print(f"\nFor 29kg shipment: Weight range {result[0]}-{result[1]}kg, Zone 11 rate = ${result[2]:.2f}")
        else:
            print("\nNo rate found for 29kg shipment")
        
        conn.close()
        print("\nImport rate update completed successfully!")
        
    except Exception as e:
        print(f"Error updating Import rates: {e}")

if __name__ == "__main__":
    update_import_rates()
