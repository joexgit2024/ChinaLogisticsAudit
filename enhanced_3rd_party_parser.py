#!/usr/bin/env python3
"""        # Look for the matrix header and data
        matrix_start_row = None
        for i in range(len(df)):
            row = df.iloc[i]
            # Look for a row with destination zones (like 1, 2, 3, 4, 5...)
            if (pd.notna(row.iloc[2]) and str(row.iloc[2]).strip() == '1' and 
                pd.notna(row.iloc[3]) and str(row.iloc[3]).strip() == '2'):
                matrix_start_row = i
                print(f"Found matrix header at row {i}")
                breakd 3rd Party Parser with Better Matrix Parsing
===================================================

This script improves the matrix parsing and looks for the exact $461.09 rate.
"""

import pandas as pd
import re

def parse_matrix_sheet_improved(excel_path: str):
    """Improved matrix sheet parsing"""
    
    print("\n=== Enhanced Matrix Parsing (AU Matrix TD 3rdCtry) ===")
    
    try:
        df = pd.read_excel(excel_path, sheet_name='AU Matrix TD 3rdCtry', header=None)
        
        print(f"Sheet dimensions: {df.shape}")
        
        # Print first 15 rows to understand structure
        print("\nFirst 15 rows:")
        for i in range(min(15, len(df))):
            row_data = [str(cell) if pd.notna(cell) else 'NaN' for cell in df.iloc[i]]
            print(f"Row {i:2d}: {row_data}")
        
        # Look for the matrix header and data
        matrix_start_row = None
        for i in range(len(df)):
            row = df.iloc[i]
            # Look for a row with destination zones (like 1, 2, 3, 4, 5...)
            if pd.notna(row.iloc[1]) and str(row.iloc[1]).strip() == '1':
                matrix_start_row = i
                print(f"\nFound matrix header at row {i}")
                break
        
        if matrix_start_row is not None:
            # Extract matrix data
            matrix_data = {}
            
            # Get destination zone headers from the header row
            header_row = df.iloc[matrix_start_row]
            dest_zones = []
            for j in range(2, len(header_row)):  # Start from column 2
                cell = header_row.iloc[j]
                if pd.notna(cell) and str(cell).strip().isdigit():
                    dest_zones.append(int(str(cell).strip()))
            
            print(f"Destination zones: {dest_zones}")
            
            # Process data rows
            for i in range(matrix_start_row + 1, len(df)):
                row = df.iloc[i]
                origin_cell = row.iloc[1]  # Origin zone is in column 1
                
                if pd.notna(origin_cell) and str(origin_cell).strip().replace('.0', '').isdigit():
                    origin_zone = int(float(str(origin_cell).strip()))
                    
                    print(f"Origin zone {origin_zone}:")
                    
                    for j, dest_zone in enumerate(dest_zones):
                        if j + 2 < len(row):  # +2 because dest zones start at column 2
                            cell_value = row.iloc[j + 2]
                            if pd.notna(cell_value) and isinstance(cell_value, str):
                                result_zone = cell_value.strip().upper()
                                matrix_data[(origin_zone, dest_zone)] = result_zone
                                print(f"  → Zone {dest_zone} = {result_zone}")
            
            print(f"\nMatrix data found: {len(matrix_data)} entries")
            
            # Check our specific case
            result = matrix_data.get((4, 5), 'Not found')
            print(f"Zone 4 → Zone 5 = {result}")
            
            return matrix_data
        
    except Exception as e:
        print(f"Error parsing matrix: {str(e)}")
    
    return {}

def find_exact_rate(excel_path: str, target_rate: float = 461.09):
    """Find the exact rate in the rate sheet"""
    
    print(f"\n=== Searching for exact rate ${target_rate:.2f} ===")
    
    try:
        df = pd.read_excel(excel_path, sheet_name='AU TD 3rdCty WW', header=None)
        
        matches = []
        
        for i in range(len(df)):
            for j in range(len(df.columns)):
                cell = df.iloc[i, j]
                if pd.notna(cell) and isinstance(cell, (int, float)):
                    if abs(float(cell) - target_rate) < 0.01:  # Within 1 cent
                        matches.append((i, j, cell))
                        print(f"Found ${cell:.2f} at row {i}, column {j}")
                        
                        # Show context around this cell
                        print("Context:")
                        for context_i in range(max(0, i-2), min(len(df), i+3)):
                            row_data = []
                            for context_j in range(max(0, j-2), min(len(df.columns), j+3)):
                                cell_val = df.iloc[context_i, context_j]
                                if pd.notna(cell_val):
                                    row_data.append(str(cell_val)[:15])
                                else:
                                    row_data.append('NaN')
                            marker = '→' if context_i == i else ' '
                            print(f"  {marker} Row {context_i:2d}: {row_data}")
        
        if not matches:
            print(f"Exact rate ${target_rate:.2f} not found")
            
            # Look for close matches
            print("\nLooking for close matches (within $10):")
            for i in range(len(df)):
                for j in range(len(df.columns)):
                    cell = df.iloc[i, j]
                    if pd.notna(cell) and isinstance(cell, (int, float)):
                        if abs(float(cell) - target_rate) <= 10.0:
                            print(f"Close match: ${cell:.2f} at row {i}, column {j}")
        
        return matches
        
    except Exception as e:
        print(f"Error searching for rate: {str(e)}")
    
    return []

def analyze_weight_15kg_rates(excel_path: str):
    """Analyze all rates for 15kg weight specifically"""
    
    print(f"\n=== Analyzing all 15kg rates ===")
    
    try:
        df = pd.read_excel(excel_path, sheet_name='AU TD 3rdCty WW', header=None)
        
        # Find rows that mention "15" weight
        weight_15_rows = []
        
        for i in range(len(df)):
            cell = df.iloc[i, 0]
            if pd.notna(cell):
                cell_str = str(cell).strip()
                if cell_str == '15' or cell_str == '15.0':
                    weight_15_rows.append(i)
                    print(f"Found 15kg row at {i}: {cell_str}")
                    
                    # Show all rates in this row
                    row = df.iloc[i]
                    for j in range(1, min(10, len(row))):
                        rate_cell = row.iloc[j]
                        if pd.notna(rate_cell) and isinstance(rate_cell, (int, float)):
                            zone_letter = chr(ord('A') + j - 1)
                            print(f"  Zone {zone_letter}: ${rate_cell:.2f}")
        
        return weight_15_rows
        
    except Exception as e:
        print(f"Error analyzing 15kg rates: {str(e)}")
    
    return []

def main():
    """Enhanced main function"""
    
    excel_path = r"uploads\ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx"
    
    print("=== Enhanced 3rd Party Rate Card Analysis ===")
    
    # Parse matrix with improved logic
    matrix_data = parse_matrix_sheet_improved(excel_path)
    
    # Find exact target rate
    find_exact_rate(excel_path, 461.09)
    
    # Analyze 15kg specifically
    analyze_weight_15kg_rates(excel_path)
    
    print(f"\n=== Analysis Summary ===")
    print(f"• Zone mapping: Japan (JP) → Zone 4, New Zealand (NZ) → Zone 5")
    print(f"• Matrix lookup: Zone 4 × Zone 5 → Zone {matrix_data.get((4, 5), 'Not found')}")
    print(f"• Target rate: $461.09 (from invoice MELIR00819599)")
    print(f"• Investigation: Need to check if rate varies by document type or other factors")

if __name__ == "__main__":
    main()
