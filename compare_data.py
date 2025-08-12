#!/usr/bin/env python3
"""Compare database vs Excel data to find the column mismatch"""

import sqlite3
import pandas as pd

def compare_data():
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()

    # Check the actual data that was loaded for 29kg
    cursor.execute('SELECT weight_from, weight_to, zone_10, zone_11, zone_12, zone_13 FROM dhl_express_rate_cards WHERE service_type = ? AND rate_section = ? AND weight_from = 29', ('Import', 'Non-documents'))
    db_rate = cursor.fetchone()

    print('Database entry for 29kg:')
    if db_rate:
        print(f'Weight: {db_rate[0]}-{db_rate[1]}kg')
        print(f'Zone 10: {db_rate[2]}')
        print(f'Zone 11: {db_rate[3]}')
        print(f'Zone 12: {db_rate[4]}')  
        print(f'Zone 13: {db_rate[5]}')
    else:
        print('No 29kg entry found')

    # Now check the Excel file again
    file_path = 'uploads/ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx'
    df = pd.read_excel(file_path, sheet_name='AU TD Imp WW', skiprows=5)
    df.columns = ['KG'] + [f'Zone_{i}' for i in range(1, 20)] + ['Zone_20']
    df_clean = df[pd.to_numeric(df['KG'], errors='coerce').notna()].copy()
    df_clean['KG'] = pd.to_numeric(df_clean['KG'])

    excel_29 = df_clean[df_clean['KG'] == 29]
    if not excel_29.empty:
        print('\nExcel file for 29kg:')
        print(f'Zone 10: {excel_29.iloc[0]["Zone_10"]}')
        print(f'Zone 11: {excel_29.iloc[0]["Zone_11"]}')
        print(f'Zone 12: {excel_29.iloc[0]["Zone_12"]}')
        print(f'Zone 13: {excel_29.iloc[0]["Zone_13"]}')
        
    print('\nComparison:')
    print('Expected from invoice: Zone 12 should be 328.90')
    if db_rate and excel_29 is not None and not excel_29.empty:
        print(f'Excel Zone 12: {excel_29.iloc[0]["Zone_12"]} vs DB Zone 12: {db_rate[4]}')

    conn.close()

if __name__ == "__main__":
    compare_data()
