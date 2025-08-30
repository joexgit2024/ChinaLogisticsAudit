#!/usr/bin/env python3
"""
DGF Quote Template Generator
Creates Excel templates for AIR, FCL, and LCL quotes
"""

import pandas as pd
import os
from datetime import datetime, timedelta

def create_quote_templates():
    """Create Excel templates for DGF quotes."""
    
    # Sample data for templates
    sample_date = datetime.now()
    validity_start = sample_date
    validity_end = sample_date + timedelta(days=30)
    
    # AIR Quote Template
    air_data = {
        'quote_id': ['AIR_SAMPLE_001', 'AIR_SAMPLE_002'],
        'quote_date': [sample_date, sample_date],
        'validity_start': [validity_start, validity_start],
        'validity_end': [validity_end, validity_end],
        'origin_country': ['China', 'China'],
        'origin_city': ['Shanghai', 'Beijing'],
        'origin_airport_code': ['PVG', 'PEK'],
        'destination_country': ['USA', 'Germany'],
        'destination_city': ['Los Angeles', 'Frankfurt'],
        'destination_airport_code': ['LAX', 'FRA'],
        'rate_per_kg': [8.50, 9.20],
        'min_weight_kg': [45, 45],
        'max_weight_kg': [1000, 1000],
        'currency': ['USD', 'USD'],
        'transit_time_days': [3, 5],
        'service_type': ['Express', 'Standard'],
        'fuel_surcharge_pct': [15.5, 18.0],
        'security_surcharge': [25.00, 25.00],
        'handling_fee': [50.00, 55.00],
        'documentation_fee': [30.00, 30.00],
        'customs_clearance_fee': [45.00, 45.00],
        'pickup_fee': [35.00, 35.00],
        'delivery_fee': [40.00, 40.00],
        'other_charges': [0.00, 0.00],
        'other_charges_description': ['', ''],
        'incoterms': ['FOB', 'CIF'],
        'payment_terms': ['Prepaid', 'Collect'],
        'special_instructions': ['Handle with care', 'Temperature controlled']
    }
    
    # FCL Quote Template
    fcl_data = {
        'quote_id': ['FCL_SAMPLE_001', 'FCL_SAMPLE_002'],
        'quote_date': [sample_date, sample_date],
        'validity_start': [validity_start, validity_start],
        'validity_end': [validity_end, validity_end],
        'origin_country': ['China', 'China'],
        'origin_port': ['Shanghai', 'Ningbo'],
        'origin_port_code': ['CNSHA', 'CNNGB'],
        'destination_country': ['USA', 'Germany'],
        'destination_port': ['Long Beach', 'Hamburg'],
        'destination_port_code': ['USLGB', 'DEHAM'],
        'container_type': ['40HQ', '20GP'],
        'container_size': [40, 20],
        'container_height': ['High Cube', 'Standard'],
        'rate_per_container': [2500.00, 1800.00],
        'currency': ['USD', 'USD'],
        'transit_time_days': [18, 25],
        'service_type': ['Direct', 'Transshipment'],
        'vessel_operator': ['COSCO', 'MSC'],
        'origin_terminal_handling': [180.00, 150.00],
        'origin_documentation': [50.00, 50.00],
        'origin_customs_clearance': [120.00, 120.00],
        'origin_trucking': [250.00, 200.00],
        'origin_other_charges': [0.00, 0.00],
        'origin_charges_currency': ['CNY', 'CNY'],
        'dest_terminal_handling': [300.00, 250.00],
        'dest_documentation': [75.00, 75.00],
        'dest_customs_clearance': [150.00, 150.00],
        'dest_trucking': [350.00, 280.00],
        'dest_other_charges': [0.00, 0.00],
        'dest_charges_currency': ['USD', 'USD'],
        'bunker_adjustment_factor': [12.5, 10.0],
        'currency_adjustment_factor': [2.0, 2.0],
        'equipment_imbalance_surcharge': [100.00, 80.00],
        'incoterms': ['FOB', 'CIF'],
        'payment_terms': ['Prepaid', 'Collect'],
        'free_time_days': [7, 5],
        'demurrage_rate': [80.00, 60.00],
        'detention_rate': [60.00, 50.00],
        'special_instructions': ['Hazmat cargo', 'Oversized cargo']
    }
    
    # LCL Quote Template
    lcl_data = {
        'quote_id': ['LCL_SAMPLE_001', 'LCL_SAMPLE_002'],
        'quote_date': [sample_date, sample_date],
        'validity_start': [validity_start, validity_start],
        'validity_end': [validity_end, validity_end],
        'origin_country': ['China', 'China'],
        'origin_port': ['Shanghai', 'Shenzhen'],
        'origin_port_code': ['CNSHA', 'CNSZN'],
        'destination_country': ['USA', 'Australia'],
        'destination_port': ['Los Angeles', 'Sydney'],
        'destination_port_code': ['USLAX', 'AUSYD'],
        'rate_per_cbm': [125.00, 140.00],
        'rate_per_ton': [95.00, 110.00],
        'min_charge_cbm': [1.0, 1.0],
        'min_charge_ton': [1.0, 1.0],
        'currency': ['USD', 'USD'],
        'weight_measure_ratio': [1000, 1000],
        'transit_time_days': [20, 25],
        'service_type': ['Standard', 'Express'],
        'consolidation_port': ['Shanghai', 'Hong Kong'],
        'origin_handling_fee': [45.00, 50.00],
        'origin_documentation': [35.00, 35.00],
        'origin_customs_clearance': [60.00, 60.00],
        'origin_pickup_fee': [80.00, 75.00],
        'origin_other_charges': [0.00, 0.00],
        'origin_charges_currency': ['CNY', 'CNY'],
        'dest_handling_fee': [65.00, 70.00],
        'dest_documentation': [45.00, 45.00],
        'dest_customs_clearance': [85.00, 85.00],
        'dest_delivery_fee': [120.00, 130.00],
        'dest_other_charges': [0.00, 0.00],
        'dest_charges_currency': ['USD', 'AUD'],
        'bunker_adjustment_factor': [8.5, 10.0],
        'currency_adjustment_factor': [1.5, 2.0],
        'consolidation_fee': [25.00, 30.00],
        'deconsolidation_fee': [35.00, 40.00],
        'incoterms': ['FOB', 'CIF'],
        'payment_terms': ['Prepaid', 'Collect'],
        'free_time_days': [5, 7],
        'storage_rate': [15.00, 20.00],
        'special_instructions': ['Fragile goods', 'Electronics']
    }
    
    # Create Excel file with multiple sheets
    template_path = os.path.join('uploads', 'DGF_Quote_Template.xlsx')
    
    with pd.ExcelWriter(template_path, engine='openpyxl') as writer:
        # Write AIR quotes
        air_df = pd.DataFrame(air_data)
        air_df.to_excel(writer, sheet_name='AIR', index=False)
        
        # Write FCL quotes
        fcl_df = pd.DataFrame(fcl_data)
        fcl_df.to_excel(writer, sheet_name='FCL', index=False)
        
        # Write LCL quotes
        lcl_df = pd.DataFrame(lcl_data)
        lcl_df.to_excel(writer, sheet_name='LCL', index=False)
        
        # Create instructions sheet
        instructions = {
            'Sheet Name': ['AIR', 'FCL', 'LCL'],
            'Description': [
                'Air freight quotes with rates per kg',
                'Full Container Load quotes with rates per container',
                'Less than Container Load quotes with rates per CBM/TON'
            ],
            'Required Columns': [
                'quote_id, origin_airport_code, destination_airport_code, rate_per_kg, currency',
                'quote_id, origin_port_code, destination_port_code, container_type, rate_per_container, currency',
                'quote_id, origin_port_code, destination_port_code, rate_per_cbm, rate_per_ton, currency'
            ]
        }
        
        instructions_df = pd.DataFrame(instructions)
        instructions_df.to_excel(writer, sheet_name='Instructions', index=False)
    
    print(f"DGF Quote template created: {template_path}")
    return template_path

if __name__ == "__main__":
    # Ensure upload directory exists
    os.makedirs('uploads', exist_ok=True)
    
    # Create the template
    template_path = create_quote_templates()
    print(f"Template ready for download: {template_path}")
