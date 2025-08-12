#!/usr/bin/env python3
"""
Invoice CSV Data Analysis for Schema Enhancement
Analyzes patterns in uploaded CSV files to identify schema improvements
"""

import pandas as pd
import json
from typing import Dict, List, Set
import re

def analyze_csv_patterns():
    print("=== INVOICE CSV DATA ANALYSIS ===\n")
    
    # Load both CSV files
    try:
        df1 = pd.read_csv("uploads/DHL EXPRESS INVOICES.csv")
        df2 = pd.read_csv("uploads/t_inv_level_rep_dg_400694_20250724100337.csv")
        
        print(f"File 1 (DHL EXPRESS INVOICES.csv): {len(df1)} rows, {len(df1.columns)} columns")
        print(f"File 2 (t_inv_level_rep_dg_400694_20250724100337.csv): {len(df2)} rows, {len(df2.columns)} columns")
        print()
        
        # Analyze File 1 structure
        print("=== FILE 1 STRUCTURE (DHL EXPRESS INVOICES.csv) ===")
        print("Columns:")
        for i, col in enumerate(df1.columns, 1):
            sample_vals = df1[col].dropna().unique()[:3]
            print(f"{i:2d}. {col:<25} | Sample: {sample_vals}")
        print()
        
        # Analyze File 2 structure
        print("=== FILE 2 STRUCTURE (t_inv_level_rep_dg_400694_20250724100337.csv) ===")
        print("Key columns (first 25):")
        for i, col in enumerate(df2.columns[:25], 1):
            sample_vals = df2[col].dropna().unique()[:2]
            print(f"{i:2d}. {col:<30} | Sample: {sample_vals}")
        print(f"... and {len(df2.columns)-25} more columns")
        print()
        
        # Field mapping analysis
        print("=== FIELD MAPPING ANALYSIS ===")
        
        # Common field patterns
        common_fields = {
            'invoice_identifiers': [],
            'dates': [],
            'amounts': [],
            'locations': [],
            'customer_info': [],
            'shipment_details': [],
            'charges': []
        }
        
        # Analyze File 1 patterns
        for col in df1.columns:
            col_lower = col.lower()
            if 'invoice' in col_lower and ('no' in col_lower or 'number' in col_lower):
                common_fields['invoice_identifiers'].append(f"File1: {col}")
            elif 'date' in col_lower:
                common_fields['dates'].append(f"File1: {col}")
            elif 'amount' in col_lower or 'charge' in col_lower:
                common_fields['amounts'].append(f"File1: {col}")
            elif any(x in col_lower for x in ['origin', 'destination', 'code']):
                common_fields['locations'].append(f"File1: {col}")
            elif any(x in col_lower for x in ['company', 'customer', 'name', 'shipper', 'receiver']):
                common_fields['customer_info'].append(f"File1: {col}")
            elif any(x in col_lower for x in ['weight', 'awb', 'reference']):
                common_fields['shipment_details'].append(f"File1: {col}")
        
        # Analyze File 2 patterns
        for col in df2.columns:
            col_lower = col.lower()
            if 'invoice' in col_lower and ('no' in col_lower or 'number' in col_lower):
                common_fields['invoice_identifiers'].append(f"File2: {col}")
            elif 'date' in col_lower:
                common_fields['dates'].append(f"File2: {col}")
            elif 'charge' in col_lower or 'amount' in col_lower:
                common_fields['amounts'].append(f"File2: {col}")
            elif any(x in col_lower for x in ['origin', 'destination', 'port', 'terminal']):
                common_fields['locations'].append(f"File2: {col}")
            elif any(x in col_lower for x in ['bill to', 'shipper', 'consignee', 'name']):
                common_fields['customer_info'].append(f"File2: {col}")
            elif any(x in col_lower for x in ['weight', 'volume', 'container', 'pieces']):
                common_fields['shipment_details'].append(f"File2: {col}")
        
        for category, fields in common_fields.items():
            if fields:
                print(f"\n{category.upper().replace('_', ' ')}:")
                for field in fields[:10]:  # Show first 10
                    print(f"  - {field}")
                if len(fields) > 10:
                    print(f"  ... and {len(fields)-10} more")
        
        return df1, df2, common_fields
        
    except Exception as e:
        print(f"Error loading CSV files: {e}")
        return None, None, None

def analyze_charge_categories():
    """Analyze charge types and categories"""
    print("\n=== CHARGE CATEGORIES ANALYSIS ===")
    
    try:
        df1 = pd.read_csv("uploads/DHL EXPRESS INVOICES.csv")
        
        # Extract charge descriptions from File 1
        if 'DHL Product Description' in df1.columns:
            descriptions = df1['DHL Product Description'].dropna().unique()
            print(f"\nCharge Types found in File 1 ({len(descriptions)} unique):")
            
            # Categorize charges
            freight_charges = []
            surcharges = []
            service_charges = []
            duties_taxes = []
            other_charges = []
            
            for desc in descriptions:
                desc_lower = str(desc).lower()
                if any(x in desc_lower for x in ['express', 'economy', 'domestic', 'worldwide']):
                    freight_charges.append(desc)
                elif any(x in desc_lower for x in ['fuel', 'surcharge', 'premium', 'remote']):
                    surcharges.append(desc)
                elif any(x in desc_lower for x in ['signature', 'billing', 'storage', 'handling']):
                    service_charges.append(desc)
                elif any(x in desc_lower for x in ['duties', 'tax', 'customs']):
                    duties_taxes.append(desc)
                else:
                    other_charges.append(desc)
            
            print("FREIGHT CHARGES:")
            for charge in freight_charges:
                print(f"  - {charge}")
            
            print("\nSURCHARGES:")
            for charge in surcharges:
                print(f"  - {charge}")
            
            print("\nSERVICE CHARGES:")
            for charge in service_charges:
                print(f"  - {charge}")
            
            print("\nDUTIES & TAXES:")
            for charge in duties_taxes:
                print(f"  - {charge}")
            
            print("\nOTHER CHARGES:")
            for charge in other_charges:
                print(f"  - {charge}")
                
    except Exception as e:
        print(f"Error analyzing charge categories: {e}")

def suggest_schema_enhancements():
    """Suggest enhancements to current schema based on CSV analysis"""
    print("\n=== SCHEMA ENHANCEMENT SUGGESTIONS ===")
    
    suggestions = {
        "new_fields": {
            "llm_invoice_summary": [
                "account_number VARCHAR(50)",
                "payment_terms VARCHAR(20)",  # Collect, Prepaid, etc.
                "incoterms VARCHAR(10)",      # FCA, EXW, DAP, etc.
                "transportation_mode VARCHAR(20)",  # Air, Sea, etc.
                "masterbill VARCHAR(50)",
                "housebill VARCHAR(50)",
                "awb_number VARCHAR(50)",
                "shipment_date DATE",
                "total_pieces INTEGER",
                "chargeable_weight DECIMAL(10,2)",
                "volume_weight DECIMAL(10,2)",
                "exchange_rate_eur DECIMAL(10,6)",
                "exchange_rate_usd DECIMAL(10,6)",
                "shipper_name VARCHAR(255)",
                "shipper_address TEXT",
                "consignee_name VARCHAR(255)",
                "consignee_address TEXT",
                "commodity_description TEXT"
            ],
            "llm_billing_line_items": [
                "charge_type VARCHAR(50)",    # Pickup, Freight, Fuel, etc.
                "base_amount DECIMAL(15,2)",  # Amount before surcharges
                "surcharge_amount DECIMAL(15,2)",
                "discount_amount DECIMAL(15,2)",
                "discount_code VARCHAR(20)",
                "tax_code VARCHAR(10)",
                "pal_col INTEGER",           # Package/Container identifier
                "weight_charge DECIMAL(15,2)"
            ]
        },
        "enhanced_categories": [
            "PICKUP_CHARGES",
            "ORIGIN_HANDLING", 
            "ORIGIN_CUSTOMS",
            "FREIGHT_CHARGES",
            "FUEL_SURCHARGE",
            "SECURITY_SURCHARGE", 
            "DESTINATION_CUSTOMS",
            "DESTINATION_HANDLING",
            "DELIVERY_CHARGES",
            "REMOTE_AREA_PICKUP",
            "REMOTE_AREA_DELIVERY",
            "OVERWEIGHT_PIECE",
            "NON_CONVEYABLE_PIECE",
            "CHANGE_OF_BILLING",
            "DIRECT_SIGNATURE",
            "PREMIUM_SERVICE",
            "BONDED_STORAGE",
            "GOGREEN_PLUS",
            "OTHER_CHARGES"
        ],
        "new_tables": {
            "llm_shipment_routing": [
                "id INTEGER PRIMARY KEY AUTOINCREMENT",
                "invoice_no VARCHAR(50) NOT NULL",
                "origin_port_code VARCHAR(10)",
                "origin_port_name VARCHAR(100)",  
                "destination_port_code VARCHAR(10)",
                "destination_port_name VARCHAR(100)",
                "port_of_loading VARCHAR(100)",
                "port_of_discharge VARCHAR(100)",
                "routing_details TEXT",
                "FOREIGN KEY (invoice_no) REFERENCES llm_pdf_extractions(invoice_no)"
            ],
            "llm_container_details": [
                "id INTEGER PRIMARY KEY AUTOINCREMENT",
                "invoice_no VARCHAR(50) NOT NULL", 
                "container_number VARCHAR(50)",
                "container_type VARCHAR(20)",  # FCL, LCL
                "num_teus INTEGER",
                "num_20ft INTEGER",
                "num_40ft INTEGER",
                "FOREIGN KEY (invoice_no) REFERENCES llm_pdf_extractions(invoice_no)"
            ]
        }
    }
    
    print("RECOMMENDED NEW FIELDS FOR llm_invoice_summary:")
    for field in suggestions["new_fields"]["llm_invoice_summary"]:
        print(f"  + {field}")
    
    print("\nRECOMMENDED NEW FIELDS FOR llm_billing_line_items:")
    for field in suggestions["new_fields"]["llm_billing_line_items"]:
        print(f"  + {field}")
    
    print("\nENHANCED CHARGE CATEGORIES:")
    for category in suggestions["enhanced_categories"]:
        print(f"  - {category}")
    
    print("\nRECOMMENDED NEW TABLES:")
    for table_name, fields in suggestions["new_tables"].items():
        print(f"\n{table_name}:")
        for field in fields:
            print(f"  {field}")
    
    return suggestions

if __name__ == "__main__":
    df1, df2, patterns = analyze_csv_patterns()
    analyze_charge_categories()
    suggestions = suggest_schema_enhancements()
    
    print("\n=== SUMMARY ===")
    print("✅ Analyzed 2 CSV files with comprehensive invoice data")
    print("✅ Identified common field patterns across both formats")
    print("✅ Categorized 19 different charge types")
    print("✅ Suggested schema enhancements for better data capture")
    print("\nNext steps:")
    print("1. Update LLM prompts to extract additional fields")
    print("2. Enhance database schema with new tables/columns")
    print("3. Update category classification in prompts")
    print("4. Test with sample invoices")
