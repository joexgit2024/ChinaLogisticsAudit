#!/usr/bin/env python3
"""Load Import rate data from Excel file"""

import pandas as pd
import sqlite3

def load_import_rates():
    file_path = 'uploads/ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx'
    sheet_name = 'AU TD Imp WW'
    
    try:
        # Read starting from row 5 (where the headers 'KG', 'Zone 1', etc. are)
        df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=5)
        
        # Set proper column names
        df.columns = ['KG'] + [f'Zone_{i}' for i in range(1, 20)] + ['Zone_20']
        
        print(f'After cleanup - Shape: {df.shape}')
        print(f'Columns: {list(df.columns)}')
        print('First 10 rows of weight-based rates:')
        
        # Filter out rows where KG is not numeric
        df_clean = df[pd.to_numeric(df['KG'], errors='coerce').notna()].copy()
        df_clean['KG'] = pd.to_numeric(df_clean['KG'])
        
        print(df_clean.head(10))
        
        weight_min = df_clean['KG'].min()
        weight_max = df_clean['KG'].max()
        print(f'Weight range: {weight_min:.1f}kg to {weight_max:.1f}kg')
        print(f'Total weight brackets: {len(df_clean)}')
        
        return df_clean
        
    except Exception as e:
        print(f'Error: {e}')
        return None

if __name__ == "__main__":
    load_import_rates()
