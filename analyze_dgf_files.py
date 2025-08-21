#!/usr/bin/env python3
"""
Analyze DGF Air and Sea freight files to understand structure
"""

import pandas as pd
import os

def analyze_dgf_files():
    # File paths
    air_file = r'c:\ChinaLogisticsAudit\uploads\DGF AIR\DGF Air.xlsx'
    sea_file = r'c:\ChinaLogisticsAudit\uploads\DGF SEA\DGF-CN10 billing.xlsx'
    
    print('=== DGF AIR FILE ANALYSIS ===')
    try:
        air_sheets = pd.ExcelFile(air_file).sheet_names
        print(f'Sheet names: {air_sheets}')
        
        for sheet in air_sheets:
            df = pd.read_excel(air_file, sheet_name=sheet, nrows=10)
            print(f'\nSheet: {sheet}')
            print(f'Columns: {list(df.columns)}')
            print(f'Shape: {df.shape}')
            print(df.head())
            
    except Exception as e:
        print(f'Error reading air file: {e}')

    print('\n\n=== DGF SEA FILE ANALYSIS ===')
    try:
        sea_sheets = pd.ExcelFile(sea_file).sheet_names
        print(f'Sheet names: {sea_sheets}')
        
        for sheet in sea_sheets:
            df = pd.read_excel(sea_file, sheet_name=sheet, nrows=10)
            print(f'\nSheet: {sheet}')
            print(f'Columns: {list(df.columns)}')
            print(f'Shape: {df.shape}')
            print(df.head())
            
    except Exception as e:
        print(f'Error reading sea file: {e}')

if __name__ == '__main__':
    analyze_dgf_files()
