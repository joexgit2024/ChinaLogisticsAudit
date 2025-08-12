"""
DHL Ocean Rate Card Processor
Processes Excel files containing DHL Ocean freight rate cards
Similar to the air rate card system but for ocean freight
"""

import pandas as pd
import sqlite3
import logging
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import os

class OceanRateCardProcessor:
    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)
        
        # Column mappings from Excel to database fields
        self.main_columns = {
            'Contract Validity': 'contract_validity',
            'Rate Validity': 'rate_validity',
            'Award': 'award',
            'Lane ID': 'lane_id',
            'Lane Description': 'lane_description',
            'RouteID': 'route_id',
            'Service': 'service',
            'Origin Region': 'origin_region',
            'Origin Country': 'origin_country',
            'Lane Origin': 'lane_origin',
            'Cities Included in Origin Lane': 'cities_included_origin',
            'Port of Loading': 'port_of_loading',
            'Destination Region': 'destination_region',
            'Destinaiton Country': 'destination_country',  # Note: typo in Excel
            'Lane Destination': 'lane_destination',
            'Cities Included in Dest Lane': 'cities_included_destination',
            'Port of Discharge': 'port_of_discharge',
            'Route Requirements/Information': 'route_requirements',
            'Annual FEU Forecast': 'annual_feu_forecast',
            'Bidder Name': 'bidder_name',
            'Shipping Lines (SSLs) Quoted': 'shipping_lines_quoted',
            'Bidder Comments': 'bidder_comments',
            'Number of Transship Ports': 'number_transship_ports',
            'Pickup to Port Transit (Calendar Days)': 'pickup_to_port_transit_days',
            'Port to Port Transit (Calendar Days)': 'port_to_port_transit_days',
            'Port to Final Transit (Calendar Days)': 'port_to_final_transit_days',
            'DTD Transit (Days)': 'dtd_transit_days',
            'Origin Port Code\n(5 char LOCODE code)': 'origin_port_code',
            'Destination Port Code\n(5 char LOCODE code)': 'destination_port_code'
        }
        
        self.fcl_columns = {
            "20' Pickup Charges\n(USD/Container)": 'pickup_20ft',
            "40' Pickup Charges\n(USD/Container)": 'pickup_40ft',
            "40'HC Pickup Charges\n(USD/Container)": 'pickup_40hc',
            "Origin Handling 20'\n(USD/Container)": 'origin_handling_20ft',
            "Origin Handling 40'\n(USD/Container)": 'origin_handling_40ft',
            "Origin Handling 40'HC\n(USD/Container)": 'origin_handling_40hc',
            'Origin # of Free Days for Detention and Demurrrage ': 'origin_free_days',
            'Origin Detention and Demurrage Rate (USD/Container)': 'origin_detention_demurrage_rate',
            "Freight Rate 20'\n(USD/Container)": 'freight_rate_20ft',
            "Freight Rate 40'\n(USD/Container)": 'freight_rate_40ft',
            "Freight Rate 40'HC\n(USD/Container)": 'freight_rate_40hc',
            "PSS 20'\n(USD/Container)": 'pss_20ft',
            "PSS 40'\n(USD/Container)": 'pss_40ft',
            "PSS 40'HC\n(USD/Container)": 'pss_40hc',
            "20' ERS (USD/Container)": 'ers_20ft',
            "40' ERS (USD/Container)": 'ers_40ft',
            "40'HC ERS (USD/Container)": 'ers_40hc',
            "Bunker Rate 20' + ETS\n(USD/Container)": 'bunker_rate_20ft',
            "Bunker Rate 40' + ETS\n(USD/Container)": 'bunker_rate_40ft',
            "Bunker Rate 40'HC + ETS\n(USD/Container)": 'bunker_rate_40hc',
            "Destination Handling 20'\n(USD/Container)": 'dest_handling_20ft',
            "Destination Handling 40'\n(USD/Container)": 'dest_handling_40ft',
            "Destination Handling 40'HC\n(USD/Container)": 'dest_handling_40hc',
            "Delivery Charges 20'\n(USD/Container)": 'delivery_20ft',
            "Delivery Charges 40'\n(USD/Container)": 'delivery_40ft',
            "Delivery Charges 40'HC\n(USD/Container)": 'delivery_40hc',
            'Destination # of Free Days for Demurrage and Detention': 'dest_free_days',
            "Destination Detention and Demurrage Rate\n(USD/Container)": 'dest_detention_demurrage_rate',
            "Destination Detention and Demurrage Rate\n(USD/Container).1": 'dest_detention_demurrage_rate_alt',
            "20' Total (USD/Container": 'total_20ft',
            "40' Total (USD/Container": 'total_40ft',
            "40'HC Total (USD/Container)": 'total_40hc'
        }
        
        self.lcl_columns = {
            'LCL DTD Transit Time': 'lcl_dtd_transit_time',
            'LCL Transit Validation': 'lcl_transit_validation',
            'LCL Pickup Charges Min (USD)': 'lcl_pickup_min_usd',
            'LCL Pickup Charges Min (USD/CBM)': 'lcl_pickup_usd_per_cbm',
            'LCL Origin Handling Min (USD)': 'lcl_origin_handling_min_usd',
            'LCL Origin Handling (USD/CBM)': 'lcl_origin_handling_usd_per_cbm',
            "LCL Freight Min\n(USD)": 'lcl_freight_min_usd',
            'LCL Freight Rate (USD/CBM)': 'lcl_freight_usd_per_cbm',
            "PSS Min\n(USD)": 'lcl_pss_min_usd',
            'PSS (USD/CBM)': 'lcl_pss_usd_per_cbm',
            "LCL Destination Handling Min\n(USD)": 'lcl_dest_handling_min_usd',
            "LCL Destination Handling Rate\n(USD/CBM)": 'lcl_dest_handling_usd_per_cbm',
            "LCL Delivery Min \n(USD)": 'lcl_delivery_min_usd',
            "LCL Delivery Rate \n(USD/CBM)": 'lcl_delivery_usd_per_cbm',
            "LCL Total Min \n(USD)": 'lcl_total_min_usd',
            "LCL Total \n(USD/CBM)": 'lcl_total_usd_per_cbm'
        }

    def clean_numeric_value(self, value) -> Optional[float]:
        """Clean and convert numeric values"""
        if pd.isna(value) or value is None or value == '':
            return None
        
        try:
            if isinstance(value, str):
                # Remove currency symbols and whitespace
                value = value.strip().replace('$', '').replace(',', '')
                if value == '' or value.lower() in ['n/a', 'na', 'null']:
                    return None
            return float(value)
        except (ValueError, TypeError):
            return None

    def clean_text_value(self, value) -> Optional[str]:
        """Clean text values"""
        if pd.isna(value) or value is None:
            return None
        
        text = str(value).strip()
        if text.lower() in ['nan', 'null', 'n/a', 'na', '']:
            return None
        
        return text

    def process_excel_file(self, file_path: str) -> Dict:
        """Process ocean rate card Excel file"""
        upload_id = f"ocean_rate_card_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        try:
            self.logger.info(f"Processing ocean rate card file: {file_path}")
            
            # Read Excel file
            df = pd.read_excel(file_path)
            self.logger.info(f"Found {len(df)} rows in Excel file")
            
            # Track upload
            file_size = os.path.getsize(file_path)
            filename = os.path.basename(file_path)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Record upload
            cursor.execute('''
                INSERT INTO ocean_rate_card_uploads 
                (upload_id, filename, file_size, total_records)
                VALUES (?, ?, ?, ?)
            ''', (upload_id, filename, file_size, len(df)))
            
            processed_records = 0
            failed_records = 0
            errors = []
            
            for idx, row in df.iterrows():
                try:
                    # Process main rate card data
                    main_data = {'upload_id': upload_id}
                    for excel_col, db_col in self.main_columns.items():
                        if excel_col in df.columns:
                            if db_col in ['annual_feu_forecast']:
                                main_data[db_col] = self.clean_numeric_value(row[excel_col])
                            elif db_col in ['number_transship_ports', 'pickup_to_port_transit_days',
                                          'port_to_port_transit_days', 'port_to_final_transit_days', 
                                          'dtd_transit_days']:
                                val = self.clean_numeric_value(row[excel_col])
                                main_data[db_col] = int(val) if val is not None else None
                            else:
                                main_data[db_col] = self.clean_text_value(row[excel_col])
                    
                    # Insert main record
                    columns = list(main_data.keys())
                    placeholders = ', '.join(['?' for _ in columns])
                    query = f"INSERT INTO ocean_rate_cards ({', '.join(columns)}) VALUES ({placeholders})"
                    
                    cursor.execute(query, list(main_data.values()))
                    rate_card_id = cursor.lastrowid
                    
                    # Process FCL charges
                    fcl_data = {'rate_card_id': rate_card_id}
                    for excel_col, db_col in self.fcl_columns.items():
                        if excel_col in df.columns:
                            if db_col in ['origin_free_days', 'dest_free_days']:
                                val = self.clean_numeric_value(row[excel_col])
                                fcl_data[db_col] = int(val) if val is not None else None
                            elif db_col in ['origin_detention_demurrage_rate', 'dest_detention_demurrage_rate']:
                                val = self.clean_numeric_value(row[excel_col])
                                fcl_data[db_col] = int(val) if val is not None else None
                            else:
                                fcl_data[db_col] = self.clean_numeric_value(row[excel_col])
                    
                    fcl_columns = list(fcl_data.keys())
                    fcl_placeholders = ', '.join(['?' for _ in fcl_columns])
                    fcl_query = f"INSERT INTO ocean_fcl_charges ({', '.join(fcl_columns)}) VALUES ({fcl_placeholders})"
                    
                    cursor.execute(fcl_query, list(fcl_data.values()))
                    
                    # Process LCL rates
                    lcl_data = {'rate_card_id': rate_card_id}
                    for excel_col, db_col in self.lcl_columns.items():
                        if excel_col in df.columns:
                            if db_col == 'lcl_transit_validation':
                                lcl_data[db_col] = self.clean_text_value(row[excel_col])
                            else:
                                lcl_data[db_col] = self.clean_numeric_value(row[excel_col])
                    
                    lcl_columns = list(lcl_data.keys())
                    lcl_placeholders = ', '.join(['?' for _ in lcl_columns])
                    lcl_query = f"INSERT INTO ocean_lcl_rates ({', '.join(lcl_columns)}) VALUES ({lcl_placeholders})"
                    
                    cursor.execute(lcl_query, list(lcl_data.values()))
                    
                    processed_records += 1
                    
                    if processed_records % 50 == 0:
                        self.logger.info(f"Processed {processed_records}/{len(df)} records")
                        
                except Exception as e:
                    failed_records += 1
                    error_msg = f"Row {idx}: {str(e)}"
                    errors.append(error_msg)
                    self.logger.error(error_msg)
                    continue
            
            # Update upload status
            cursor.execute('''
                UPDATE ocean_rate_card_uploads 
                SET processed_date = ?, processed_records = ?, failed_records = ?, status = ?
                WHERE upload_id = ?
            ''', (datetime.now(), processed_records, failed_records, 
                  'completed' if failed_records == 0 else 'completed_with_errors', upload_id))
            
            conn.commit()
            conn.close()
            
            result = {
                'upload_id': upload_id,
                'total_records': len(df),
                'processed_records': processed_records,
                'failed_records': failed_records,
                'errors': errors[:10]  # Limit error messages
            }
            
            self.logger.info(f"Processing completed: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to process file: {str(e)}")
            return {
                'upload_id': upload_id,
                'total_records': 0,
                'processed_records': 0,
                'failed_records': 0,
                'errors': [str(e)]
            }

    def get_upload_summary(self, upload_id: str = None) -> List[Dict]:
        """Get upload summary information"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if upload_id:
            cursor.execute('''
                SELECT upload_id, filename, file_size, upload_date, processed_date,
                       total_records, processed_records, failed_records, status
                FROM ocean_rate_card_uploads 
                WHERE upload_id = ?
                ORDER BY upload_date DESC
            ''', (upload_id,))
        else:
            cursor.execute('''
                SELECT upload_id, filename, file_size, upload_date, processed_date,
                       total_records, processed_records, failed_records, status
                FROM ocean_rate_card_uploads 
                ORDER BY upload_date DESC
            ''')
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'upload_id': row[0],
                'filename': row[1],
                'file_size': row[2],
                'upload_date': row[3],
                'processed_date': row[4],
                'total_records': row[5],
                'processed_records': row[6],
                'failed_records': row[7],
                'status': row[8]
            })
        
        conn.close()
        return results

    def search_rates(self, origin: str = None, destination: str = None, 
                    service: str = None, container_type: str = None) -> List[Dict]:
        """Search ocean rates with filters"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        query = '''
            SELECT 
                r.lane_id, r.lane_description, r.service,
                r.origin_country, r.lane_origin, r.port_of_loading,
                r.destination_country, r.lane_destination, r.port_of_discharge,
                r.dtd_transit_days,
                f.total_20ft, f.total_40ft, f.total_40hc,
                l.lcl_total_min_usd, l.lcl_total_usd_per_cbm,
                r.rate_validity
            FROM ocean_rate_cards r
            LEFT JOIN ocean_fcl_charges f ON r.id = f.rate_card_id
            LEFT JOIN ocean_lcl_rates l ON r.id = l.rate_card_id
            WHERE 1=1
        '''
        
        params = []
        
        if origin:
            query += " AND (r.origin_country LIKE ? OR r.lane_origin LIKE ?)"
            params.extend([f"%{origin}%", f"%{origin}%"])
        
        if destination:
            query += " AND (r.destination_country LIKE ? OR r.lane_destination LIKE ?)"
            params.extend([f"%{destination}%", f"%{destination}%"])
        
        if service:
            query += " AND r.service LIKE ?"
            params.append(f"%{service}%")
        
        query += " ORDER BY r.dtd_transit_days, f.total_40ft"
        
        cursor.execute(query, params)
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'lane_id': row[0],
                'lane_description': row[1],
                'service': row[2],
                'origin_country': row[3],
                'lane_origin': row[4],
                'port_of_loading': row[5],
                'destination_country': row[6],
                'lane_destination': row[7],
                'port_of_discharge': row[8],
                'dtd_transit_days': row[9],
                'total_20ft': row[10],
                'total_40ft': row[11],
                'total_40hc': row[12],
                'lcl_total_min_usd': row[13],
                'lcl_total_usd_per_cbm': row[14],
                'rate_validity': row[15]
            })
        
        conn.close()
        return results

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    
    # Test with the sample file
    processor = OceanRateCardProcessor()
    result = processor.process_excel_file(r'C:\DGFaudit\uploads\DHL OCEAN RATE CARD.xlsx')
    print(f"Processing result: {result}")
