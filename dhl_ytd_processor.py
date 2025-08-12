#!/usr/bin/env python3
"""
DHL Year-to-Date Invoice Report Processor
========================================

This module handles the processing of DHL year-to-date invoice reports in CSV format.
Provides functionality to parse, validate, and store DHL YTD invoice data.
"""

import sqlite3
import csv
import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
from decimal import Decimal, InvalidOperation
import re

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DHLYTDProcessor:
    """Processes DHL Year-to-Date invoice reports"""
    
    def __init__(self, db_path="dhl_audit.db"):
        self.db_path = db_path
        self.create_tables()
        
    def create_tables(self):
        """Create necessary database tables for DHL YTD data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Create main DHL YTD table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dhl_ytd_invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_no TEXT UNIQUE NOT NULL,
                invoice_type TEXT,
                payment_terms TEXT,
                incoterms_code TEXT,
                invoice_creation_date TEXT,
                masterbill TEXT,
                housebill TEXT,
                housebill_origin TEXT,
                housebill_destination TEXT,
                cw1_shipment_number TEXT,
                movement_type TEXT,
                product_name TEXT,
                shipment_creation_date TEXT,
                transportation_mode TEXT,
                commodity TEXT,
                origin TEXT,
                origin_port_country_code TEXT,
                port_terminal_loading TEXT,
                port_terminal_loading_code TEXT,
                destination TEXT,
                destination_port_country_code TEXT,
                port_discharge TEXT,
                port_discharge_code TEXT,
                bill_to_account TEXT,
                bill_to_name TEXT,
                bill_to_parent TEXT,
                bill_to_division TEXT,
                bill_to_sub_division TEXT,
                bill_to_address TEXT,
                bill_to_zip_code TEXT,
                bill_to_city TEXT,
                bill_to_country_code TEXT,
                shipper_account TEXT,
                shipper_name TEXT,
                shipper_parent TEXT,
                shipper_division TEXT,
                shipper_sub_division TEXT,
                shipper_address TEXT,
                shipper_zip_code TEXT,
                shipper_city TEXT,
                shipper_country_code TEXT,
                shipper_reference TEXT,
                consignee_account TEXT,
                consignee_name TEXT,
                consignee_parent TEXT,
                consignee_division TEXT,
                consignee_sub_division TEXT,
                consignee_address TEXT,
                consignee_zip_code TEXT,
                consignee_city TEXT,
                consignee_country_code TEXT,
                consignee_reference TEXT,
                billing_branch TEXT,
                billing_company_code TEXT,
                invoice_currency TEXT,
                
                -- Original currency charges (AUD/EUR/USD etc)
                pickup_charges DECIMAL(10,2),
                origin_handling_charges DECIMAL(10,2),
                origin_demurrage_charges DECIMAL(10,2),
                origin_storage_charges DECIMAL(10,2),
                origin_customs_charges DECIMAL(10,2),
                freight_charges DECIMAL(10,2),
                fuel_surcharge DECIMAL(10,2),
                security_surcharge DECIMAL(10,2),
                destination_customs_charges DECIMAL(10,2),
                destination_storage_charges DECIMAL(10,2),
                destination_demurrage_charges DECIMAL(10,2),
                destination_handling_charges DECIMAL(10,2),
                delivery_charges DECIMAL(10,2),
                other_charges DECIMAL(10,2),
                duties_and_taxes DECIMAL(10,2),
                total_charges_without_duty_tax DECIMAL(10,2),
                total_charges_with_duty_tax DECIMAL(10,2),
                
                -- EUR currency charges
                pickup_charges_eur DECIMAL(10,2),
                origin_handling_charges_eur DECIMAL(10,2),
                origin_demurrage_charges_eur DECIMAL(10,2),
                origin_storage_charges_eur DECIMAL(10,2),
                origin_customs_charges_eur DECIMAL(10,2),
                freight_charges_eur DECIMAL(10,2),
                fuel_surcharges_eur DECIMAL(10,2),
                security_surcharges_eur DECIMAL(10,2),
                destination_customs_charges_eur DECIMAL(10,2),
                destination_storage_charges_eur DECIMAL(10,2),
                destination_demurrage_charges_eur DECIMAL(10,2),
                destination_handling_charges_eur DECIMAL(10,2),
                delivery_charges_eur DECIMAL(10,2),
                other_charges_eur DECIMAL(10,2),
                duties_and_taxes_eur DECIMAL(10,2),
                total_charges_without_duty_tax_eur DECIMAL(10,2),
                total_charges_with_duty_tax_eur DECIMAL(10,2),
                
                -- USD currency charges
                pickup_charges_usd DECIMAL(10,2),
                origin_handling_charges_usd DECIMAL(10,2),
                origin_demurrage_charges_usd DECIMAL(10,2),
                origin_storage_charges_usd DECIMAL(10,2),
                origin_customs_charges_usd DECIMAL(10,2),
                freight_charges_usd DECIMAL(10,2),
                fuel_surcharges_usd DECIMAL(10,2),
                security_surcharges_usd DECIMAL(10,2),
                destination_customs_charges_usd DECIMAL(10,2),
                destination_storage_charges_usd DECIMAL(10,2),
                destination_demurrage_charges_usd DECIMAL(10,2),
                destination_handling_charges_usd DECIMAL(10,2),
                delivery_charges_usd DECIMAL(10,2),
                other_charges_usd DECIMAL(10,2),
                duties_and_taxes_usd DECIMAL(10,2),
                total_charges_without_duty_tax_usd DECIMAL(10,2),
                total_charges_with_duty_tax_usd DECIMAL(10,2),
                
                -- Exchange rates and shipment details
                exchange_rate_eur DECIMAL(10,6),
                exchange_rate_usd DECIMAL(10,6),
                shipment_weight_kg DECIMAL(10,2),
                total_shipment_chargeable_weight_kg DECIMAL(10,2),
                total_shipment_volume_m3 DECIMAL(10,2),
                total_shipment_chargeable_volume_m3 DECIMAL(10,2),
                total_pieces INTEGER,
                fcl_lcl TEXT,
                number_of_teus INTEGER,
                nb_of_20ft_containers INTEGER,
                nb_of_40ft_containers INTEGER,
                container_numbers TEXT,
                purchase_order_number TEXT,
                invoice_compliance_number TEXT,
                shipment_cancelled TEXT,
                
                -- Processing metadata
                upload_batch_id TEXT,
                processed_date TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create index on invoice_no for fast lookups
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dhl_ytd_invoice_no 
            ON dhl_ytd_invoices(invoice_no)
        ''')
        
        # Create index on upload_batch_id
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_dhl_ytd_batch_id 
            ON dhl_ytd_invoices(upload_batch_id)
        ''')
        
        # Create upload tracking table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dhl_ytd_uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id TEXT UNIQUE NOT NULL,
                filename TEXT NOT NULL,
                file_size INTEGER,
                total_records INTEGER,
                processed_records INTEGER,
                failed_records INTEGER,
                duplicate_records INTEGER,
                processing_status TEXT DEFAULT 'pending',
                processing_start_time TEXT,
                processing_end_time TEXT,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("DHL YTD database tables created successfully")
    
    def validate_decimal(self, value, field_name=""):
        """Convert string value to decimal, handling empty strings and invalid values"""
        if value is None:
            return None
        
        # Handle NaN values from pandas
        import pandas as pd
        if pd.isna(value):
            return None
        
        if str(value).strip() == "":
            return None
        
        try:
            # Remove any comma separators and convert to decimal
            cleaned_value = str(value).replace(',', '')
            return float(cleaned_value)
        except (ValueError, InvalidOperation):
            logger.warning(f"Invalid decimal value for {field_name}: {value}")
            return None
    
    def validate_integer(self, value, field_name=""):
        """Convert string value to integer, handling empty strings and invalid values"""
        if value is None:
            return None
        
        # Handle NaN values from pandas
        import pandas as pd
        if pd.isna(value):
            return None
        
        if str(value).strip() == "":
            return None
        
        try:
            return int(float(value))  # Handle "0.0" format
        except (ValueError, TypeError):
            logger.warning(f"Invalid integer value for {field_name}: {value}")
            return None
    
    def validate_date(self, value, field_name=""):
        """Validate and format date strings"""
        if value is None or (hasattr(value, '__len__') and len(str(value).strip()) == 0):
            return None
        
        # Handle NaN values from pandas
        import pandas as pd
        if pd.isna(value):
            return None
        
        try:
            # Try to parse the date and return ISO format
            if "00:00:00.0" in str(value):
                # Format: "2025-05-30 00:00:00.0"
                date_part = str(value).split(' ')[0]
                return date_part
            return str(value).strip()
        except Exception:
            logger.warning(f"Invalid date format for {field_name}: {value}")
            return None
    
    def process_csv_file(self, file_path, batch_id=None):
        """Process a DHL YTD CSV file"""
        if not batch_id:
            batch_id = f"dhl_ytd_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        logger.info(f"Processing DHL YTD file: {file_path}")
        
        # Create upload record
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO dhl_ytd_uploads 
            (batch_id, filename, file_size, processing_status, processing_start_time)
            VALUES (?, ?, ?, ?, ?)
        ''', (batch_id, file_path.name, file_path.stat().st_size, 'processing', 
              datetime.now().isoformat()))
        conn.commit()
        
        total_records = 0
        processed_records = 0
        failed_records = 0
        duplicate_records = 0
        errors = []
        
        try:
            # Read CSV file
            df = pd.read_csv(file_path, encoding='utf-8')
            total_records = len(df)
            
            logger.info(f"Found {total_records} records in CSV file")
            
            # Process each row
            for index, row in df.iterrows():
                try:
                    # Check if invoice already exists
                    invoice_no = row.get('Invoice No', '').strip()
                    if not invoice_no:
                        failed_records += 1
                        errors.append(f"Row {index + 2}: Missing Invoice No")
                        continue
                    
                    # Check for existing invoice
                    cursor.execute('SELECT id FROM dhl_ytd_invoices WHERE invoice_no = ?', (invoice_no,))
                    if cursor.fetchone():
                        duplicate_records += 1
                        logger.warning(f"Duplicate invoice skipped: {invoice_no}")
                        continue
                    
                    # Prepare data for insertion
                    data = self.prepare_row_data(row, batch_id)
                    
                    # Insert into database
                    placeholders = ', '.join(['?'] * len(data))
                    columns = ', '.join(data.keys())
                    
                    cursor.execute(f'''
                        INSERT INTO dhl_ytd_invoices ({columns})
                        VALUES ({placeholders})
                    ''', list(data.values()))
                    
                    processed_records += 1
                    
                    if processed_records % 100 == 0:
                        logger.info(f"Processed {processed_records}/{total_records} records")
                        conn.commit()
                
                except Exception as e:
                    failed_records += 1
                    error_msg = f"Row {index + 2}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg)
            
            # Final commit
            conn.commit()
            
            # Update upload record
            cursor.execute('''
                UPDATE dhl_ytd_uploads 
                SET total_records = ?, processed_records = ?, failed_records = ?, 
                    duplicate_records = ?, processing_status = ?, processing_end_time = ?
                WHERE batch_id = ?
            ''', (total_records, processed_records, failed_records, duplicate_records,
                  'completed', datetime.now().isoformat(), batch_id))
            conn.commit()
            
        except Exception as e:
            error_msg = f"Failed to process file: {str(e)}"
            logger.error(error_msg)
            
            cursor.execute('''
                UPDATE dhl_ytd_uploads 
                SET processing_status = ?, error_message = ?, processing_end_time = ?
                WHERE batch_id = ?
            ''', ('failed', error_msg, datetime.now().isoformat(), batch_id))
            conn.commit()
            raise
        
        finally:
            conn.close()
        
        result = {
            'batch_id': batch_id,
            'total_records': total_records,
            'processed_records': processed_records,
            'failed_records': failed_records,
            'duplicate_records': duplicate_records,
            'errors': errors[:10]  # Return first 10 errors
        }
        
        logger.info(f"Processing completed: {result}")
        return result
    
    def prepare_row_data(self, row, batch_id):
        """Prepare a single row of data for database insertion"""
        data = {}
        
        def safe_str(value):
            """Safely convert value to string and strip whitespace"""
            if value is None:
                return ''
            # Handle NaN values from pandas
            import pandas as pd
            if pd.isna(value):
                return ''
            return str(value).strip()
        
        # Basic invoice information
        data['invoice_no'] = safe_str(row.get('Invoice No', ''))
        data['invoice_type'] = safe_str(row.get('Invoice Type', ''))
        data['payment_terms'] = safe_str(row.get('Payment Terms', ''))
        data['incoterms_code'] = safe_str(row.get('Incoterms Code', ''))
        data['invoice_creation_date'] = self.validate_date(row.get('Invoice Creation Date'))
        
        # Shipment details
        data['masterbill'] = safe_str(row.get('MasterBill', ''))
        data['housebill'] = safe_str(row.get('HouseBill', ''))
        data['housebill_origin'] = safe_str(row.get('HouseBill Origin', ''))
        data['housebill_destination'] = safe_str(row.get('HouseBill Destination', ''))
        data['cw1_shipment_number'] = safe_str(row.get('CW1 Shipment Number', ''))
        data['movement_type'] = safe_str(row.get('Movement Type', ''))
        data['product_name'] = safe_str(row.get('Product Name', ''))
        data['shipment_creation_date'] = self.validate_date(row.get('Shipment Creation Date'))
        data['transportation_mode'] = safe_str(row.get('Transportation Mode', ''))
        data['commodity'] = safe_str(row.get('Commodity', ''))
        
        # Location information
        data['origin'] = safe_str(row.get('Origin', ''))
        data['origin_port_country_code'] = safe_str(row.get('Origin Port Country/Region Code', ''))
        data['port_terminal_loading'] = safe_str(row.get('Port/ Terminal of Loading', ''))
        data['port_terminal_loading_code'] = safe_str(row.get('Port/ Terminal of Loading Code', ''))
        data['destination'] = safe_str(row.get('Destination', ''))
        data['destination_port_country_code'] = safe_str(row.get('Destination Port Country/Region Code', ''))
        data['port_discharge'] = safe_str(row.get('Port of Discharge', ''))
        data['port_discharge_code'] = safe_str(row.get('Port of Discharge Code', ''))
        
        # Bill To information
        data['bill_to_account'] = safe_str(row.get('Bill To - Account', ''))
        data['bill_to_name'] = safe_str(row.get('Bill To - Name', ''))
        data['bill_to_parent'] = safe_str(row.get('Bill To - Parent', ''))
        data['bill_to_division'] = safe_str(row.get('Bill To - Division', ''))
        data['bill_to_sub_division'] = safe_str(row.get('Bill To - Sub Division', ''))
        data['bill_to_address'] = safe_str(row.get('Bill To - Address', ''))
        data['bill_to_zip_code'] = safe_str(row.get('Bill To - ZIP Code', ''))
        data['bill_to_city'] = safe_str(row.get('Bill To - City', ''))
        data['bill_to_country_code'] = safe_str(row.get('Bill To - Country/Region Code', ''))
        
        # Shipper information
        data['shipper_account'] = safe_str(row.get('Shipper - Account', ''))
        data['shipper_name'] = safe_str(row.get('Shipper Name', ''))
        data['shipper_parent'] = safe_str(row.get('Shipper - Parent', ''))
        data['shipper_division'] = safe_str(row.get('Shipper - Division', ''))
        data['shipper_sub_division'] = safe_str(row.get('Shipper - Sub Division', ''))
        data['shipper_address'] = safe_str(row.get('Shipper Address', ''))
        data['shipper_zip_code'] = safe_str(row.get('Shipper - ZIP Code', ''))
        data['shipper_city'] = safe_str(row.get('Shipper City', ''))
        data['shipper_country_code'] = safe_str(row.get('Shipper - Country/Region Code', ''))
        data['shipper_reference'] = safe_str(row.get('Shipper Reference', ''))
        
        # Consignee information
        data['consignee_account'] = safe_str(row.get('Consignee - Account', ''))
        data['consignee_name'] = safe_str(row.get('Consignee Name', ''))
        data['consignee_parent'] = safe_str(row.get('Consignee - Parent', ''))
        data['consignee_division'] = safe_str(row.get('Consignee - Division', ''))
        data['consignee_sub_division'] = safe_str(row.get('Consignee - Sub Division', ''))
        data['consignee_address'] = safe_str(row.get('Consignee Address', ''))
        data['consignee_zip_code'] = safe_str(row.get('Consignee - ZIP Code', ''))
        data['consignee_city'] = safe_str(row.get('Consignee City', ''))
        data['consignee_country_code'] = safe_str(row.get('Consignee - Country/Region Code', ''))
        data['consignee_reference'] = safe_str(row.get('Consignee Reference', ''))
        
        # Billing information
        data['billing_branch'] = safe_str(row.get('Billing Branch', ''))
        data['billing_company_code'] = safe_str(row.get('Billing Company Code', ''))
        data['invoice_currency'] = safe_str(row.get('Invoice Currency', ''))
        
        # Original currency charges
        data['pickup_charges'] = self.validate_decimal(row.get('Pickup Charges'))
        data['origin_handling_charges'] = self.validate_decimal(row.get('Origin Handling Charges'))
        data['origin_demurrage_charges'] = self.validate_decimal(row.get('Origin Demurrage Charges'))
        data['origin_storage_charges'] = self.validate_decimal(row.get('Origin Storage Charges'))
        data['origin_customs_charges'] = self.validate_decimal(row.get('Origin Customs Charges'))
        data['freight_charges'] = self.validate_decimal(row.get('Freight Charges'))
        data['fuel_surcharge'] = self.validate_decimal(row.get('Fuel Surcharge'))
        data['security_surcharge'] = self.validate_decimal(row.get('Security Surcharge'))
        data['destination_customs_charges'] = self.validate_decimal(row.get('Destination Customs Charges'))
        data['destination_storage_charges'] = self.validate_decimal(row.get('Destination Storage Charges'))
        data['destination_demurrage_charges'] = self.validate_decimal(row.get('Destination Demurrage Charges'))
        data['destination_handling_charges'] = self.validate_decimal(row.get('Destination Handling Charges'))
        data['delivery_charges'] = self.validate_decimal(row.get('Delivery Charges'))
        data['other_charges'] = self.validate_decimal(row.get('Other Charges'))
        data['duties_and_taxes'] = self.validate_decimal(row.get('Duties and Taxes'))
        data['total_charges_without_duty_tax'] = self.validate_decimal(row.get('Total Charges without Duties and Taxes'))
        data['total_charges_with_duty_tax'] = self.validate_decimal(row.get('Total Charges with Duties and Taxes'))
        
        # EUR charges
        data['pickup_charges_eur'] = self.validate_decimal(row.get('Pick-up Charges (EUR)'))
        data['origin_handling_charges_eur'] = self.validate_decimal(row.get('Origin Handling Charges (EUR)'))
        data['origin_demurrage_charges_eur'] = self.validate_decimal(row.get('Origin Demurrage Charges (EUR)'))
        data['origin_storage_charges_eur'] = self.validate_decimal(row.get('Origin Storage Charges (EUR)'))
        data['origin_customs_charges_eur'] = self.validate_decimal(row.get('Origin Customs Charges (EUR)'))
        data['freight_charges_eur'] = self.validate_decimal(row.get('Freight Charges (EUR)'))
        data['fuel_surcharges_eur'] = self.validate_decimal(row.get('Fuel Surcharges (EUR)'))
        data['security_surcharges_eur'] = self.validate_decimal(row.get('Security Surcharges (EUR)'))
        data['destination_customs_charges_eur'] = self.validate_decimal(row.get('Destination Customs Charges (EUR)'))
        data['destination_storage_charges_eur'] = self.validate_decimal(row.get('Destination Storage Charges (EUR)'))
        data['destination_demurrage_charges_eur'] = self.validate_decimal(row.get('Destination Demurrage Charges (EUR)'))
        data['destination_handling_charges_eur'] = self.validate_decimal(row.get('Destination Handling Charges (EUR)'))
        data['delivery_charges_eur'] = self.validate_decimal(row.get('Delivery Charges (EUR)'))
        data['other_charges_eur'] = self.validate_decimal(row.get('Other Charges (EUR)'))
        data['duties_and_taxes_eur'] = self.validate_decimal(row.get('Duties and Taxes (EUR)'))
        data['total_charges_without_duty_tax_eur'] = self.validate_decimal(row.get('Total Charges without Duty and Tax (EUR)'))
        data['total_charges_with_duty_tax_eur'] = self.validate_decimal(row.get('Total Charges with Duty and Tax (EUR)'))
        
        # USD charges
        data['pickup_charges_usd'] = self.validate_decimal(row.get('Pick-up Charges (USD)'))
        data['origin_handling_charges_usd'] = self.validate_decimal(row.get('Origin Handling Charges (USD)'))
        data['origin_demurrage_charges_usd'] = self.validate_decimal(row.get('Origin Demurrage Charges (USD)'))
        data['origin_storage_charges_usd'] = self.validate_decimal(row.get('Origin Storage Charges (USD)'))
        data['origin_customs_charges_usd'] = self.validate_decimal(row.get('Origin Customs Charges (USD)'))
        data['freight_charges_usd'] = self.validate_decimal(row.get('Freight Charges (USD)'))
        data['fuel_surcharges_usd'] = self.validate_decimal(row.get('Fuel Surcharges (USD)'))
        data['security_surcharges_usd'] = self.validate_decimal(row.get('Security Surcharges (USD)'))
        data['destination_customs_charges_usd'] = self.validate_decimal(row.get('Destination Customs Charges (USD)'))
        data['destination_storage_charges_usd'] = self.validate_decimal(row.get('Destination Storage Charges (USD)'))
        data['destination_demurrage_charges_usd'] = self.validate_decimal(row.get('Destination Demurrage Charges (USD)'))
        data['destination_handling_charges_usd'] = self.validate_decimal(row.get('Destination Handling Charges (USD)'))
        data['delivery_charges_usd'] = self.validate_decimal(row.get('Delivery Charges (USD)'))
        data['other_charges_usd'] = self.validate_decimal(row.get('Other Charges (USD)'))
        data['duties_and_taxes_usd'] = self.validate_decimal(row.get('Duties and Taxes (USD)'))
        data['total_charges_without_duty_tax_usd'] = self.validate_decimal(row.get('Total Charges without Duty and Tax (USD)'))
        data['total_charges_with_duty_tax_usd'] = self.validate_decimal(row.get('Total Charges with Duty and Tax (USD)'))
        
        # Exchange rates and shipment details
        data['exchange_rate_eur'] = self.validate_decimal(row.get('Exchange Rate (EUR)'))
        data['exchange_rate_usd'] = self.validate_decimal(row.get('Exchange Rate (USD)'))
        data['shipment_weight_kg'] = self.validate_decimal(row.get('Shipment Weight, kg'))
        data['total_shipment_chargeable_weight_kg'] = self.validate_decimal(row.get('Total Shipment Chargeable Weight, kg'))
        data['total_shipment_volume_m3'] = self.validate_decimal(row.get('Total Shipment Volume, m3'))
        data['total_shipment_chargeable_volume_m3'] = self.validate_decimal(row.get('Total Shipment Chargeable Volume, m3'))
        data['total_pieces'] = self.validate_integer(row.get('Total Pieces'))
        data['fcl_lcl'] = safe_str(row.get('FCL/LCL', ''))
        data['number_of_teus'] = self.validate_integer(row.get('Number of TEUs'))
        data['nb_of_20ft_containers'] = self.validate_integer(row.get('Nb of 20ft Containers'))
        data['nb_of_40ft_containers'] = self.validate_integer(row.get('Nb of 40ft Containers'))
        data['container_numbers'] = safe_str(row.get('Container Numbers', ''))
        data['purchase_order_number'] = safe_str(row.get('Purchase Order Number', ''))
        data['invoice_compliance_number'] = safe_str(row.get('Invoice Compliance Number', ''))
        data['shipment_cancelled'] = safe_str(row.get('Shipment Cancelled', ''))
        
        # Processing metadata
        data['upload_batch_id'] = batch_id
        data['processed_date'] = datetime.now().isoformat()
        
        return data
    
    def get_upload_status(self, batch_id):
        """Get upload processing status"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT * FROM dhl_ytd_uploads WHERE batch_id = ?
        ''', (batch_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            columns = [desc[0] for desc in cursor.description]
            return dict(zip(columns, row))
        return None
    
    def get_ytd_invoices_for_comparison(self, invoice_nos):
        """Get YTD invoice data for comparison with EDI data"""
        if not invoice_nos:
            return {}
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        placeholders = ', '.join(['?'] * len(invoice_nos))
        cursor.execute(f'''
            SELECT invoice_no, invoice_type, invoice_currency, transportation_mode,
                   total_charges_without_duty_tax, total_charges_with_duty_tax,
                   total_charges_without_duty_tax_usd, total_charges_with_duty_tax_usd,
                   pickup_charges, origin_handling_charges, freight_charges, fuel_surcharge,
                   destination_handling_charges, delivery_charges, other_charges,
                   pickup_charges_usd, origin_handling_charges_usd, freight_charges_usd, 
                   fuel_surcharges_usd, destination_handling_charges_usd, delivery_charges_usd, 
                   other_charges_usd, exchange_rate_usd, invoice_creation_date,
                   shipper_name, consignee_name, commodity
            FROM dhl_ytd_invoices 
            WHERE invoice_no IN ({placeholders})
        ''', invoice_nos)
        
        results = {}
        for row in cursor.fetchall():
            invoice_no = row[0]
            results[invoice_no] = {
                'invoice_type': row[1],
                'invoice_currency': row[2],
                'transportation_mode': row[3],
                'total_without_tax': row[4],
                'total_with_tax': row[5],
                'total_without_tax_usd': row[6],
                'total_with_tax_usd': row[7],
                'pickup_charges': row[8],
                'origin_handling_charges': row[9],
                'freight_charges': row[10],
                'fuel_surcharge': row[11],
                'destination_handling_charges': row[12],
                'delivery_charges': row[13],
                'other_charges': row[14],
                'pickup_charges_usd': row[15],
                'origin_handling_charges_usd': row[16],
                'freight_charges_usd': row[17],
                'fuel_surcharges_usd': row[18],
                'destination_handling_charges_usd': row[19],
                'delivery_charges_usd': row[20],
                'other_charges_usd': row[21],
                'exchange_rate_usd': row[22],
                'invoice_creation_date': row[23],
                'shipper_name': row[24],
                'consignee_name': row[25],
                'commodity': row[26]
            }
        
        conn.close()
        return results

def main():
    """Test the DHL YTD processor"""
    if __name__ == "__main__":
        processor = DHLYTDProcessor()
        
        # Test with the sample file
        test_file = "uploads/t_inv_level_rep_dg_399574_20250719224545.csv"
        
        try:
            result = processor.process_csv_file(test_file)
            print(f"Processing complete: {result}")
        except Exception as e:
            print(f"Error processing file: {e}")

if __name__ == "__main__":
    main()
