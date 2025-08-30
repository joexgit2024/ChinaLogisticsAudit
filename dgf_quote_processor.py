#!/usr/bin/env python3
"""
DGF Quote Upload and Processing System
Handles Excel file uploads for AIR, FCL, and LCL quotes
"""

import sqlite3
import pandas as pd
import os
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging
import openpyxl

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DGFQuoteProcessor:
    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path
    
    def process_quote_file(self, file_path: str, uploaded_by: str = 'system') -> Dict:
        """
        Process a quote Excel file containing AIR, FCL, and LCL sheets.
        Returns a summary of the processing results.
        """
        results = {
            'air': {'success': 0, 'errors': 0, 'messages': []},
            'fcl': {'success': 0, 'errors': 0, 'messages': []},
            'lcl': {'success': 0, 'errors': 0, 'messages': []}
        }
        
        try:
            # Read Excel file
            excel_file = pd.ExcelFile(file_path)
            
            # Process each sheet type
            for sheet_name in excel_file.sheet_names:
                sheet_name_lower = sheet_name.lower()
                
                if 'air' in sheet_name_lower:
                    result = self.process_air_quotes(excel_file, sheet_name, file_path, uploaded_by)
                    results['air'] = result
                elif 'fcl' in sheet_name_lower or 'full' in sheet_name_lower:
                    result = self.process_fcl_quotes(excel_file, sheet_name, file_path, uploaded_by)
                    results['fcl'] = result
                elif 'lcl' in sheet_name_lower or 'less' in sheet_name_lower:
                    result = self.process_lcl_quotes(excel_file, sheet_name, file_path, uploaded_by)
                    results['lcl'] = result
                else:
                    logger.warning(f"Unknown sheet type: {sheet_name}")
            
        except Exception as e:
            logger.error(f"Error processing quote file: {str(e)}")
            for quote_type in results:
                results[quote_type]['errors'] += 1
                results[quote_type]['messages'].append(f"File processing error: {str(e)}")
        
        return results
    
    def process_air_quotes(self, excel_file: pd.ExcelFile, sheet_name: str, file_path: str, uploaded_by: str) -> Dict:
        """Process AIR quotes from Excel sheet."""
        result = {'success': 0, 'errors': 0, 'messages': []}
        
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for idx, row in df.iterrows():
                try:
                    # Generate quote_id if not provided
                    quote_id = row.get('quote_id') or f"AIR_{datetime.now().strftime('%Y%m%d')}_{idx+1:03d}"
                    
                    # Parse dates
                    quote_date = self.parse_date(row.get('quote_date'))
                    validity_start = self.parse_date(row.get('validity_start'))
                    validity_end = self.parse_date(row.get('validity_end'))
                    
                    # Insert air quote
                    cursor.execute('''
                        INSERT OR REPLACE INTO dgf_air_quotes (
                            quote_id, quote_date, validity_start, validity_end,
                            origin_country, origin_city, origin_airport_code,
                            destination_country, destination_city, destination_airport_code,
                            rate_per_kg, min_weight_kg, max_weight_kg, currency,
                            transit_time_days, service_type,
                            fuel_surcharge_pct, security_surcharge, handling_fee,
                            documentation_fee, customs_clearance_fee, pickup_fee,
                            delivery_fee, other_charges, other_charges_description,
                            incoterms, payment_terms, special_instructions,
                            file_name, sheet_name, row_number, uploaded_by
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        quote_id, quote_date, validity_start, validity_end,
                        self.safe_get(row, 'origin_country'),
                        self.safe_get(row, 'origin_city'),
                        self.safe_get(row, 'origin_airport_code'),
                        self.safe_get(row, 'destination_country'),
                        self.safe_get(row, 'destination_city'),
                        self.safe_get(row, 'destination_airport_code'),
                        self.safe_float(row, 'rate_per_kg'),
                        self.safe_float(row, 'min_weight_kg'),
                        self.safe_float(row, 'max_weight_kg'),
                        self.safe_get(row, 'currency', 'USD'),
                        self.safe_int(row, 'transit_time_days'),
                        self.safe_get(row, 'service_type'),
                        self.safe_float(row, 'fuel_surcharge_pct'),
                        self.safe_float(row, 'security_surcharge'),
                        self.safe_float(row, 'handling_fee'),
                        self.safe_float(row, 'documentation_fee'),
                        self.safe_float(row, 'customs_clearance_fee'),
                        self.safe_float(row, 'pickup_fee'),
                        self.safe_float(row, 'delivery_fee'),
                        self.safe_float(row, 'other_charges'),
                        self.safe_get(row, 'other_charges_description'),
                        self.safe_get(row, 'incoterms'),
                        self.safe_get(row, 'payment_terms'),
                        self.safe_get(row, 'special_instructions'),
                        os.path.basename(file_path), sheet_name, idx + 1, uploaded_by
                    ))
                    result['success'] += 1
                    
                except Exception as e:
                    result['errors'] += 1
                    result['messages'].append(f"Row {idx+1}: {str(e)}")
                    logger.error(f"Error processing air quote row {idx+1}: {str(e)}")
            
            conn.commit()
            conn.close()
            result['messages'].append(f"Successfully processed {result['success']} air quotes")
            
        except Exception as e:
            result['errors'] += 1
            result['messages'].append(f"Error processing air quotes: {str(e)}")
            logger.error(f"Error processing air quotes: {str(e)}")
        
        return result
    
    def process_fcl_quotes(self, excel_file: pd.ExcelFile, sheet_name: str, file_path: str, uploaded_by: str) -> Dict:
        """Process FCL quotes from Excel sheet."""
        result = {'success': 0, 'errors': 0, 'messages': []}
        
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for idx, row in df.iterrows():
                try:
                    # Generate quote_id if not provided
                    quote_id = row.get('quote_id') or f"FCL_{datetime.now().strftime('%Y%m%d')}_{idx+1:03d}"
                    
                    # Parse dates
                    quote_date = self.parse_date(row.get('quote_date'))
                    validity_start = self.parse_date(row.get('validity_start'))
                    validity_end = self.parse_date(row.get('validity_end'))
                    
                    # Insert FCL quote
                    cursor.execute('''
                        INSERT OR REPLACE INTO dgf_fcl_quotes (
                            quote_id, quote_date, validity_start, validity_end,
                            origin_country, origin_port, origin_port_code,
                            destination_country, destination_port, destination_port_code,
                            container_type, container_size, container_height,
                            rate_per_container, currency,
                            transit_time_days, service_type, vessel_operator,
                            origin_terminal_handling, origin_documentation, origin_customs_clearance,
                            origin_trucking, origin_other_charges, origin_charges_currency,
                            dest_terminal_handling, dest_documentation, dest_customs_clearance,
                            dest_trucking, dest_other_charges, dest_charges_currency,
                            bunker_adjustment_factor, currency_adjustment_factor, equipment_imbalance_surcharge,
                            incoterms, payment_terms, free_time_days, demurrage_rate, detention_rate,
                            special_instructions, file_name, sheet_name, row_number, uploaded_by
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        quote_id, quote_date, validity_start, validity_end,
                        self.safe_get(row, 'origin_country'),
                        self.safe_get(row, 'origin_port'),
                        self.safe_get(row, 'origin_port_code'),
                        self.safe_get(row, 'destination_country'),
                        self.safe_get(row, 'destination_port'),
                        self.safe_get(row, 'destination_port_code'),
                        self.safe_get(row, 'container_type'),
                        self.safe_int(row, 'container_size'),
                        self.safe_get(row, 'container_height'),
                        self.safe_float(row, 'rate_per_container'),
                        self.safe_get(row, 'currency', 'USD'),
                        self.safe_int(row, 'transit_time_days'),
                        self.safe_get(row, 'service_type'),
                        self.safe_get(row, 'vessel_operator'),
                        self.safe_float(row, 'origin_terminal_handling'),
                        self.safe_float(row, 'origin_documentation'),
                        self.safe_float(row, 'origin_customs_clearance'),
                        self.safe_float(row, 'origin_trucking'),
                        self.safe_float(row, 'origin_other_charges'),
                        self.safe_get(row, 'origin_charges_currency', 'CNY'),
                        self.safe_float(row, 'dest_terminal_handling'),
                        self.safe_float(row, 'dest_documentation'),
                        self.safe_float(row, 'dest_customs_clearance'),
                        self.safe_float(row, 'dest_trucking'),
                        self.safe_float(row, 'dest_other_charges'),
                        self.safe_get(row, 'dest_charges_currency', 'USD'),
                        self.safe_float(row, 'bunker_adjustment_factor'),
                        self.safe_float(row, 'currency_adjustment_factor'),
                        self.safe_float(row, 'equipment_imbalance_surcharge'),
                        self.safe_get(row, 'incoterms'),
                        self.safe_get(row, 'payment_terms'),
                        self.safe_int(row, 'free_time_days'),
                        self.safe_float(row, 'demurrage_rate'),
                        self.safe_float(row, 'detention_rate'),
                        self.safe_get(row, 'special_instructions'),
                        os.path.basename(file_path), sheet_name, idx + 1, uploaded_by
                    ))
                    result['success'] += 1
                    
                except Exception as e:
                    result['errors'] += 1
                    result['messages'].append(f"Row {idx+1}: {str(e)}")
                    logger.error(f"Error processing FCL quote row {idx+1}: {str(e)}")
            
            conn.commit()
            conn.close()
            result['messages'].append(f"Successfully processed {result['success']} FCL quotes")
            
        except Exception as e:
            result['errors'] += 1
            result['messages'].append(f"Error processing FCL quotes: {str(e)}")
            logger.error(f"Error processing FCL quotes: {str(e)}")
        
        return result
    
    def process_lcl_quotes(self, excel_file: pd.ExcelFile, sheet_name: str, file_path: str, uploaded_by: str) -> Dict:
        """Process LCL quotes from Excel sheet."""
        result = {'success': 0, 'errors': 0, 'messages': []}
        
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for idx, row in df.iterrows():
                try:
                    # Generate quote_id if not provided
                    quote_id = row.get('quote_id') or f"LCL_{datetime.now().strftime('%Y%m%d')}_{idx+1:03d}"
                    
                    # Parse dates
                    quote_date = self.parse_date(row.get('quote_date'))
                    validity_start = self.parse_date(row.get('validity_start'))
                    validity_end = self.parse_date(row.get('validity_end'))
                    
                    # Insert LCL quote
                    cursor.execute('''
                        INSERT OR REPLACE INTO dgf_lcl_quotes (
                            quote_id, quote_date, validity_start, validity_end,
                            origin_country, origin_port, origin_port_code,
                            destination_country, destination_port, destination_port_code,
                            rate_per_cbm, rate_per_ton, min_charge_cbm, min_charge_ton, currency,
                            weight_measure_ratio, transit_time_days, service_type, consolidation_port,
                            origin_handling_fee, origin_documentation, origin_customs_clearance,
                            origin_pickup_fee, origin_other_charges, origin_charges_currency,
                            dest_handling_fee, dest_documentation, dest_customs_clearance,
                            dest_delivery_fee, dest_other_charges, dest_charges_currency,
                            bunker_adjustment_factor, currency_adjustment_factor,
                            consolidation_fee, deconsolidation_fee,
                            incoterms, payment_terms, free_time_days, storage_rate,
                            special_instructions, file_name, sheet_name, row_number, uploaded_by
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        quote_id, quote_date, validity_start, validity_end,
                        self.safe_get(row, 'origin_country'),
                        self.safe_get(row, 'origin_port'),
                        self.safe_get(row, 'origin_port_code'),
                        self.safe_get(row, 'destination_country'),
                        self.safe_get(row, 'destination_port'),
                        self.safe_get(row, 'destination_port_code'),
                        self.safe_float(row, 'rate_per_cbm'),
                        self.safe_float(row, 'rate_per_ton'),
                        self.safe_float(row, 'min_charge_cbm'),
                        self.safe_float(row, 'min_charge_ton'),
                        self.safe_get(row, 'currency', 'USD'),
                        self.safe_float(row, 'weight_measure_ratio', 1000),
                        self.safe_int(row, 'transit_time_days'),
                        self.safe_get(row, 'service_type'),
                        self.safe_get(row, 'consolidation_port'),
                        self.safe_float(row, 'origin_handling_fee'),
                        self.safe_float(row, 'origin_documentation'),
                        self.safe_float(row, 'origin_customs_clearance'),
                        self.safe_float(row, 'origin_pickup_fee'),
                        self.safe_float(row, 'origin_other_charges'),
                        self.safe_get(row, 'origin_charges_currency', 'CNY'),
                        self.safe_float(row, 'dest_handling_fee'),
                        self.safe_float(row, 'dest_documentation'),
                        self.safe_float(row, 'dest_customs_clearance'),
                        self.safe_float(row, 'dest_delivery_fee'),
                        self.safe_float(row, 'dest_other_charges'),
                        self.safe_get(row, 'dest_charges_currency', 'USD'),
                        self.safe_float(row, 'bunker_adjustment_factor'),
                        self.safe_float(row, 'currency_adjustment_factor'),
                        self.safe_float(row, 'consolidation_fee'),
                        self.safe_float(row, 'deconsolidation_fee'),
                        self.safe_get(row, 'incoterms'),
                        self.safe_get(row, 'payment_terms'),
                        self.safe_int(row, 'free_time_days'),
                        self.safe_float(row, 'storage_rate'),
                        self.safe_get(row, 'special_instructions'),
                        os.path.basename(file_path), sheet_name, idx + 1, uploaded_by
                    ))
                    result['success'] += 1
                    
                except Exception as e:
                    result['errors'] += 1
                    result['messages'].append(f"Row {idx+1}: {str(e)}")
                    logger.error(f"Error processing LCL quote row {idx+1}: {str(e)}")
            
            conn.commit()
            conn.close()
            result['messages'].append(f"Successfully processed {result['success']} LCL quotes")
            
        except Exception as e:
            result['errors'] += 1
            result['messages'].append(f"Error processing LCL quotes: {str(e)}")
            logger.error(f"Error processing LCL quotes: {str(e)}")
        
        return result
    
    def safe_get(self, row, key, default=None):
        """Safely get value from pandas row."""
        try:
            value = row.get(key, default)
            if pd.isna(value):
                return default
            return str(value).strip() if value is not None else default
        except:
            return default
    
    def safe_float(self, row, key, default=None):
        """Safely convert value to float."""
        try:
            value = row.get(key, default)
            if pd.isna(value) or value is None:
                return default
            return float(value)
        except:
            return default
    
    def safe_int(self, row, key, default=None):
        """Safely convert value to int."""
        try:
            value = row.get(key, default)
            if pd.isna(value) or value is None:
                return default
            return int(float(value))
        except:
            return default
    
    def parse_date(self, date_value):
        """Parse date from various formats."""
        if pd.isna(date_value) or date_value is None:
            return None
        
        try:
            if isinstance(date_value, datetime):
                return date_value.strftime('%Y-%m-%d')
            elif isinstance(date_value, str):
                # Try common date formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d']:
                    try:
                        return datetime.strptime(date_value, fmt).strftime('%Y-%m-%d')
                    except:
                        continue
        except:
            pass
        
        return None
    
    def get_quotes_summary(self) -> Dict:
        """Get summary of all quotes in the database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        summary = {}
        
        # AIR quotes summary
        cursor.execute('''
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) as active,
                MIN(validity_start) as earliest_validity,
                MAX(validity_end) as latest_validity
            FROM dgf_air_quotes
        ''')
        air_stats = cursor.fetchone()
        summary['air'] = {
            'total': air_stats[0],
            'active': air_stats[1],
            'earliest_validity': air_stats[2],
            'latest_validity': air_stats[3]
        }
        
        # FCL quotes summary
        cursor.execute('''
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) as active,
                MIN(validity_start) as earliest_validity,
                MAX(validity_end) as latest_validity
            FROM dgf_fcl_quotes
        ''')
        fcl_stats = cursor.fetchone()
        summary['fcl'] = {
            'total': fcl_stats[0],
            'active': fcl_stats[1],
            'earliest_validity': fcl_stats[2],
            'latest_validity': fcl_stats[3]
        }
        
        # LCL quotes summary
        cursor.execute('''
            SELECT 
                COUNT(*) as total,
                COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) as active,
                MIN(validity_start) as earliest_validity,
                MAX(validity_end) as latest_validity
            FROM dgf_lcl_quotes
        ''')
        lcl_stats = cursor.fetchone()
        summary['lcl'] = {
            'total': lcl_stats[0],
            'active': lcl_stats[1],
            'earliest_validity': lcl_stats[2],
            'latest_validity': lcl_stats[3]
        }
        
        conn.close()
        return summary

if __name__ == "__main__":
    # Test the processor
    processor = DGFQuoteProcessor()
    summary = processor.get_quotes_summary()
    print("Quote Summary:")
    for quote_type, stats in summary.items():
        print(f"{quote_type.upper()}: {stats['active']} active / {stats['total']} total")
