#!/usr/bin/env python3
"""
Analyze Chinese DHL Invoice Structure
===================================
"""
import pandas as pd
import sys

def analyze_dhl_china_invoice():
    file_path = 'c:/ChinaLogisticsAudit/uploads/DHL Bill.xlsx'
    
    try:
        # Load Excel file
        xl = pd.ExcelFile(file_path)
        print(f"📄 File: {file_path}")
        print(f"📊 Sheets: {xl.sheet_names}")
        
        # Analyze first sheet
        df = pd.read_excel(file_path, sheet_name='Table1')
        print(f"\n📋 Sheet 'Table1' Analysis:")
        print(f"   Rows: {len(df)}")
        print(f"   Columns: {len(df.columns)}")
        
        print(f"\n🏷️ Column Names:")
        for i, col in enumerate(df.columns, 1):
            print(f"   {i:2d}. {col}")
        
        print(f"\n📝 Sample Data (first 3 rows):")
        print(df.head(3).to_string())
        
        print(f"\n🔍 Data Types:")
        for col in df.columns:
            dtype = df[col].dtype
            non_null = df[col].notna().sum()
            print(f"   {col}: {dtype} ({non_null}/{len(df)} non-null)")
        
        return df
        
    except Exception as e:
        print(f"❌ Error analyzing file: {e}")
        return None

if __name__ == '__main__':
    analyze_dhl_china_invoice()
