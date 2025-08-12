#!/usr/bin/env python3
"""Find Japan and New Zealand in all sheets"""

import pandas as pd

excel_path = r"uploads\ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx"

print("=== Searching All Sheets for Japan and New Zealand ===")

# Get all sheet names
xl_file = pd.ExcelFile(excel_path)
sheet_names = xl_file.sheet_names

print(f"Total sheets: {len(sheet_names)}")

for sheet_name in sheet_names:
    print(f"\n--- Checking sheet: {sheet_name} ---")
    
    try:
        df = pd.read_excel(excel_path, sheet_name=sheet_name, header=None)
        
        japan_found = False
        nz_found = False
        
        for i, row in df.iterrows():
            for j, cell in enumerate(row):
                if pd.notna(cell):
                    cell_str = str(cell).upper()
                    if 'JAPAN' in cell_str or 'JP' in cell_str:
                        print(f"  JAPAN found at row {i}, col {j}: '{cell}'")
                        japan_found = True
                        # Show surrounding data
                        if j < len(row) - 1:
                            zone = row.iloc[j+1]
                            print(f"    Next cell (possible zone): '{zone}'")
                    
                    if 'NEW ZEALAND' in cell_str or 'NZ' in cell_str:
                        print(f"  NEW ZEALAND found at row {i}, col {j}: '{cell}'")
                        nz_found = True
                        # Show surrounding data
                        if j < len(row) - 1:
                            zone = row.iloc[j+1]
                            print(f"    Next cell (possible zone): '{zone}'")
        
        if not japan_found and not nz_found:
            print("  No Japan or New Zealand found in this sheet")
            
    except Exception as e:
        print(f"  Error reading sheet: {e}")

print("\n=== Summary ===")
print("If Japan and New Zealand are not found in any sheet,")
print("we may need to use a different approach or the rate card")
print("might not include these countries for 3rd party charges.")
