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
    
    def process_quote_file(self, file_path: str, uploaded_by: str = 'system', replace_existing: bool = True) -> Dict:
        """
        Process a quote Excel file containing AIR, FCL, and LCL sheets.
        Returns a summary of the processing results.
        
        Args:
            file_path: Path to the Excel file
            uploaded_by: Name or ID of the person uploading
            replace_existing: If True, replace quotes with same quote_reference_no
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
                    result = self.process_air_quotes(excel_file, sheet_name, file_path, uploaded_by, replace_existing)
                    results['air'] = result
                elif 'fcl' in sheet_name_lower or 'full' in sheet_name_lower:
                    result = self.process_fcl_quotes(excel_file, sheet_name, file_path, uploaded_by, replace_existing)
                    results['fcl'] = result
                elif 'lcl' in sheet_name_lower or 'less' in sheet_name_lower:
                    result = self.process_lcl_quotes(excel_file, sheet_name, file_path, uploaded_by, replace_existing)
                    results['lcl'] = result
                else:
                    logger.warning(f"Unknown sheet type: {sheet_name}")
            
        except Exception as e:
            logger.error(f"Error processing quote file: {str(e)}")
            for quote_type in results:
                results[quote_type]['errors'] += 1
                results[quote_type]['messages'].append(f"File processing error: {str(e)}")
        
        return results
    
    def process_air_quotes(self, excel_file: pd.ExcelFile, sheet_name: str, file_path: str, uploaded_by: str, replace_existing: bool = True) -> Dict:
        """Process AIR quotes from Excel sheet matching the actual air quote format."""
        result = {'success': 0, 'errors': 0, 'messages': []}
        
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Column mapping from Excel to database
            column_mapping = {
                'Fild': 'field_type',
                'Quote Reference No.': 'quote_reference_no',
                'Vendor Name': 'vendor_name', 
                'Validity Period': 'validity_period',
                'Origin': 'origin',
                'Destination': 'destination',
                'Service Type': 'service_type',
                'Incoterms': 'incoterms',
                'Transit Time': 'transit_time',
                'Currency': 'currency',
                'DTP Min Charge': 'dtp_min_charge',
                'DTP Freight Cost ': 'dtp_freight_cost',  # Note space in original
                'CUSTOMS CLEARANCE': 'customs_clearance',
                'Origin Min Charge ': 'origin_min_charge',  # Note space in original
                'Origin Fees \n(THC, ISS, Screening, etc.) ': 'origin_fees',
                'Per shpt charges': 'per_shipment_charges',
                'ATA Min Charge': 'ata_min_charge',
                'ATA Cost \nCharge': 'ata_cost_charge',
                'Destination Min Charge ': 'destination_min_charge',  # Note space in original
                'Destination Fees \n(THC, ISS, Screening, etc.) ': 'destination_fees',
                'Total charges': 'total_charges',
                'Remarks': 'remarks'
            }
            
            for idx, row in df.iterrows():
                try:
                    # Extract and validate quote reference number
                    quote_ref = self.safe_get(row, 'Quote Reference No.')
                    if not quote_ref:
                        quote_ref = f"AIR_{datetime.now().strftime('%Y%m%d')}_{idx+1:03d}"
                    
                    # Parse validity period (format: "8/25/2025-8/31/2025")
                    validity_start, validity_end = self.parse_validity_period(
                        self.safe_get(row, 'Validity Period', '')
                    )
                    
                    # Extract airport codes and countries from origin/destination
                    origin_airport, origin_country = self.parse_location(
                        self.safe_get(row, 'Origin', '')
                    )
                    dest_airport, dest_country = self.parse_location(
                        self.safe_get(row, 'Destination', '')
                    )
                    
                    # Parse transit time to extract days
                    transit_days = self.parse_transit_time(
                        self.safe_get(row, 'Transit Time', '')
                    )
                    
                    # Calculate main rate per kg (using DTP Freight Cost as primary rate)
                    rate_per_kg = self.safe_float(row, 'DTP Freight Cost ')
                    
                    # Check if quote already exists (when replace_existing is False)
                    if not replace_existing:
                        cursor.execute('SELECT id FROM dgf_air_quotes WHERE quote_reference_no = ?', (quote_ref,))
                        if cursor.fetchone():
                            result['messages'].append(f"Skipped existing quote: {quote_ref}")
                            continue
                    
                    # Use appropriate INSERT strategy based on replace_existing
                    insert_mode = "INSERT OR REPLACE" if replace_existing else "INSERT OR IGNORE"
                    
                    # Insert air quote with new structure
                    cursor.execute(f'''
                        {insert_mode} INTO dgf_air_quotes (
                            field_type, quote_reference_no, vendor_name, validity_period,
                            validity_start, validity_end, origin, destination,
                            origin_airport_code, destination_airport_code, 
                            origin_country, destination_country,
                            service_type, incoterms, transit_time, transit_time_days, currency,
                            dtp_min_charge, dtp_freight_cost, customs_clearance,
                            origin_min_charge, origin_fees, per_shipment_charges,
                            ata_min_charge, ata_cost_charge, destination_min_charge,
                            destination_fees, total_charges, remarks, rate_per_kg,
                            file_name, sheet_name, row_number, uploaded_by
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        self.safe_get(row, 'Fild'),
                        quote_ref,
                        self.safe_get(row, 'Vendor Name'),
                        self.safe_get(row, 'Validity Period'),
                        validity_start,
                        validity_end,
                        self.safe_get(row, 'Origin'),
                        self.safe_get(row, 'Destination'),
                        origin_airport,
                        dest_airport,
                        origin_country,
                        dest_country,
                        self.safe_get(row, 'Service Type'),
                        self.safe_get(row, 'Incoterms'),
                        self.safe_get(row, 'Transit Time'),
                        transit_days,
                        self.safe_get(row, 'Currency', 'USD'),
                        self.safe_float(row, 'DTP Min Charge'),
                        self.safe_float(row, 'DTP Freight Cost '),
                        self.safe_float(row, 'CUSTOMS CLEARANCE'),
                        self.safe_float(row, 'Origin Min Charge '),
                        self.safe_float(row, 'Origin Fees \n(THC, ISS, Screening, etc.) '),
                        self.safe_float(row, 'Per shpt charges'),
                        self.safe_float(row, 'ATA Min Charge'),
                        self.safe_float(row, 'ATA Cost \nCharge'),
                        self.safe_float(row, 'Destination Min Charge '),
                        self.safe_float(row, 'Destination Fees \n(THC, ISS, Screening, etc.) '),
                        self.safe_float(row, 'Total charges'),
                        self.safe_get(row, 'Remarks'),
                        rate_per_kg,
                        os.path.basename(file_path),
                        sheet_name,
                        idx + 1,
                        uploaded_by
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
    
    def process_fcl_quotes(self, excel_file: pd.ExcelFile, sheet_name: str, file_path: str, uploaded_by: str, replace_existing: bool = True) -> Dict:
        """Process FCL quotes from Excel sheet - Based on actual FCL quote format."""
        result = {'success': 0, 'errors': 0, 'messages': []}
        
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for idx, row in df.iterrows():
                try:
                    # Get quote reference number
                    quote_ref = self.safe_get(row, 'Quote Reference No.')
                    if not quote_ref:
                        result['errors'] += 1
                        result['messages'].append(f"Row {idx+1}: Missing Quote Reference No.")
                        continue
                    
                    # Parse validity period
                    validity_start, validity_end = self.parse_validity_period(
                        self.safe_get(row, 'Validity Period')
                    )
                    
                    # Parse locations
                    origin_port, origin_country = self.parse_location(self.safe_get(row, 'Origin'))
                    dest_port, dest_country = self.parse_location(self.safe_get(row, 'Destination'))
                    
                    # Parse transit time
                    transit_days = self.parse_transit_time(self.safe_get(row, 'Transit Time'))
                    
                    # Check if quote already exists (when replace_existing is False)
                    if not replace_existing:
                        cursor.execute('SELECT id FROM dgf_fcl_quotes WHERE quote_reference_no = ?', (quote_ref,))
                        if cursor.fetchone():
                            result['messages'].append(f"Skipped existing quote: {quote_ref}")
                            continue
                    
                    # Use appropriate INSERT strategy based on replace_existing
                    insert_mode = "INSERT OR REPLACE" if replace_existing else "INSERT OR IGNORE"
                    
                    # Insert FCL quote
                    cursor.execute(f'''
                        {insert_mode} INTO dgf_fcl_quotes (
                            field_type, quote_reference_no, vendor_name, validity_period,
                            validity_start, validity_end, origin, destination,
                            origin_port_code, destination_port_code, origin_country, destination_country,
                            service_type, incoterms, transit_time, transit_time_days, currency,
                            pickup_charges_20, pickup_charges_40, customs_clearance,
                            origin_handling_20, origin_handling_40, freight_rate_20, freight_rate_40,
                            per_shipment_charges, destination_handling, total_charges,
                            file_name, sheet_name, row_number, uploaded_by
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        self.safe_get(row, 'Fild'),
                        quote_ref,
                        self.safe_get(row, 'Vendor Name'),
                        self.safe_get(row, 'Validity Period'),
                        validity_start,
                        validity_end,
                        self.safe_get(row, 'Origin'),
                        self.safe_get(row, 'Destination'),
                        origin_port,
                        dest_port,
                        origin_country,
                        dest_country,
                        self.safe_get(row, 'Service Type'),
                        self.safe_get(row, 'Incoterms'),
                        self.safe_get(row, 'Transit Time'),
                        transit_days,
                        self.safe_get(row, 'Currency', 'USD'),
                        self.safe_float(row, "Pickup Charges 20' "),
                        self.safe_float(row, "Pickup Charges 40' "),
                        self.safe_float(row, 'CUSTOMS CLEARANCE'),
                        self.safe_float(row, "Origin Handling 20' "),
                        self.safe_float(row, "Origin Handling 40' "),
                        self.safe_float(row, "Freight Rate 20' "),
                        self.safe_float(row, "Freight Rate 40' "),
                        self.safe_float(row, 'Per Shipment Charges'),
                        self.safe_float(row, 'Destination Handling '),
                        self.safe_float(row, 'Total Charges'),
                        os.path.basename(file_path),
                        sheet_name,
                        idx + 1,
                        uploaded_by
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
    
    def process_lcl_quotes(self, excel_file: pd.ExcelFile, sheet_name: str, file_path: str, uploaded_by: str, replace_existing: bool = True) -> Dict:
        """Process LCL quotes from Excel sheet - Based on actual LCL quote format."""
        result = {'success': 0, 'errors': 0, 'messages': []}
        
        try:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for idx, row in df.iterrows():
                try:
                    # Get quote reference number
                    quote_ref = self.safe_get(row, 'Quote Reference No.')
                    if not quote_ref:
                        result['errors'] += 1
                        result['messages'].append(f"Row {idx+1}: Missing Quote Reference No.")
                        continue
                    
                    # Parse validity period
                    validity_start, validity_end = self.parse_validity_period(
                        self.safe_get(row, 'Validity Period')
                    )
                    
                    # Parse locations
                    origin_port, origin_country = self.parse_location(self.safe_get(row, 'Origin'))
                    dest_port, dest_country = self.parse_location(self.safe_get(row, 'Destination'))
                    
                    # Parse transit time
                    transit_days = self.parse_transit_time(self.safe_get(row, 'Transit Time'))
                    
                    # Check if quote already exists (when replace_existing is False)
                    if not replace_existing:
                        cursor.execute('SELECT id FROM dgf_lcl_quotes WHERE quote_reference_no = ?', (quote_ref,))
                        if cursor.fetchone():
                            result['messages'].append(f"Skipped existing quote: {quote_ref}")
                            continue
                    
                    # Use appropriate INSERT strategy based on replace_existing
                    insert_mode = "INSERT OR REPLACE" if replace_existing else "INSERT OR IGNORE"
                    
                    # Insert LCL quote
                    cursor.execute(f'''
                        {insert_mode} INTO dgf_lcl_quotes (
                            field_type, quote_reference_no, vendor_name, validity_period,
                            validity_start, validity_end, origin, destination,
                            origin_port_code, destination_port_code, origin_country, destination_country,
                            service_type, incoterms, transit_time, transit_time_days, currency,
                            lcl_pickup_charges_min, lcl_pickup_charges_rate, customs_clearance,
                            lcl_origin_handling_min, lcl_origin_handling, per_shipment_charges,
                            lcl_freight_min, lcl_freight_rate, lcl_destination_handling_min,
                            lcl_destination_handling_rate, dest_document_handover, total_charges,
                            file_name, sheet_name, row_number, uploaded_by
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        self.safe_get(row, 'Fild'),
                        quote_ref,
                        self.safe_get(row, 'Vendor Name'),
                        self.safe_get(row, 'Validity Period'),
                        validity_start,
                        validity_end,
                        self.safe_get(row, 'Origin'),
                        self.safe_get(row, 'Destination'),
                        origin_port,
                        dest_port,
                        origin_country,
                        dest_country,
                        self.safe_get(row, 'Service Type'),
                        self.safe_get(row, 'Incoterms'),
                        self.safe_get(row, 'Transit Time'),
                        transit_days,
                        self.safe_get(row, 'Currency', 'USD'),
                        self.safe_float(row, 'LCL Pickup Charges Min '),
                        self.safe_float(row, 'LCL Pickup Charges Rate '),
                        self.safe_float(row, 'CUSTOMS CLEARANCE'),
                        self.safe_float(row, 'LCL Origin Handling Min '),
                        self.safe_float(row, 'LCL Origin Handling '),
                        self.safe_float(row, 'Per Shipment Charges'),
                        self.safe_float(row, 'LCL Freight Min'),
                        self.safe_float(row, 'LCL Freight Rate '),
                        self.safe_float(row, 'LCL Destination Handling Min '),
                        self.safe_float(row, 'LCL Destination Handling Rate '),
                        self.safe_float(row, 'Dest. Document Handove'),
                        self.safe_float(row, 'Total Charges'),
                        os.path.basename(file_path),
                        sheet_name,
                        idx + 1,
                        uploaded_by
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
    
    def parse_validity_period(self, validity_period: str) -> Tuple[Optional[str], Optional[str]]:
        """Parse validity period in format '8/25/2025-8/31/2025' to start and end dates."""
        if not validity_period or pd.isna(validity_period):
            return None, None
        
        try:
            # Split on dash or hyphen
            parts = re.split(r'[-–—]', str(validity_period).strip())
            if len(parts) != 2:
                return None, None
            
            start_str = parts[0].strip()
            end_str = parts[1].strip()
            
            # Parse dates
            start_date = self.parse_date(start_str)
            end_date = self.parse_date(end_str)
            
            return start_date, end_date
            
        except Exception as e:
            logger.warning(f"Could not parse validity period '{validity_period}': {e}")
            return None, None
    
    def parse_location(self, location: str) -> Tuple[Optional[str], Optional[str]]:
        """Parse location like 'DALLAS (DFW), US' or 'HAIPHONG (VNHPH), VIETNAM' to extract code and country."""
        if not location or pd.isna(location):
            return None, None
        
        try:
            location = str(location).strip()
            
            # Extract code from parentheses (3-5 characters for ports/airports)
            code_match = re.search(r'\(([A-Z]{3,5})\)', location)
            code = code_match.group(1) if code_match else None
            
            # Extract country (usually after the last comma)
            country_match = re.search(r',\s*([^,]+)$', location)
            country = country_match.group(1).strip() if country_match else None
            
            return code, country
            
        except Exception as e:
            logger.warning(f"Could not parse location '{location}': {e}")
            return None, None
    
    def parse_transit_time(self, transit_time: str) -> Optional[int]:
        """Parse transit time like '6 days' to extract number of days."""
        if not transit_time or pd.isna(transit_time):
            return None
        
        try:
            # Extract number from transit time string
            match = re.search(r'(\d+)', str(transit_time))
            if match:
                return int(match.group(1))
            return None
            
        except Exception as e:
            logger.warning(f"Could not parse transit time '{transit_time}': {e}")
            return None

if __name__ == "__main__":
    # Test the processor
    processor = DGFQuoteProcessor()
    summary = processor.get_quotes_summary()
    print("Quote Summary:")
    for quote_type, stats in summary.items():
        print(f"{quote_type.upper()}: {stats['active']} active / {stats['total']} total")
