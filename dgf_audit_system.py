#!/usr/bin/env python3
"""
DGF Air & Sea Freight Audit System

This system audits DGF invoices against their spot quotes baseline.
DGF doesn't use traditional rate cards but issues spot quotes for each lane,
which we use as the baseline for auditing their invoices.

Features:
- Extract spot quotes as baseline rates per lane
- Audit air and sea freight invoices against quotes
- Handle currency conversion with exchange rates
- Track variances and exceptions
- Generate audit reports
"""

import sqlite3
import pandas as pd
import os
import re
import PyPDF2
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DGFAuditSystem:
    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path
        self.init_dgf_tables()
    
    def init_dgf_tables(self):
        """Initialize DGF-specific database tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # DGF Spot Quotes table (baseline rates)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dgf_spot_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quote_id TEXT UNIQUE NOT NULL,
                mode TEXT NOT NULL,  -- 'AIR' or 'SEA'
                lane TEXT NOT NULL,  -- origin-destination
                origin_country TEXT,
                origin_port TEXT,
                destination_country TEXT,
                destination_port TEXT,
                terms TEXT,  -- FOB, CIF, etc.
                quote_date DATE,
                validity_start DATE,
                validity_end DATE,
                
                -- Air specific
                rate_per_kg DECIMAL(10,4),
                min_weight DECIMAL(10,2),
                
                -- Sea specific  
                rate_per_cbm DECIMAL(10,4),
                container_type TEXT,
                
                -- Common charges
                origin_handling_fee DECIMAL(10,2),
                origin_currency TEXT,
                dest_handling_fee DECIMAL(10,2),
                dest_currency TEXT,
                fuel_surcharge_pct DECIMAL(5,2),
                customs_clearance DECIMAL(10,2),
                documentation_fee DECIMAL(10,2),
                other_charges DECIMAL(10,2),
                
                -- Exchange rates at quote time
                origin_fx_rate DECIMAL(10,6),
                dest_fx_rate DECIMAL(10,6),
                
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'ACTIVE'
            )
        ''')
        
        # DGF Invoices table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dgf_invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_number TEXT,
                quote_id TEXT,
                mode TEXT NOT NULL,  -- 'AIR' or 'SEA'
                hbl_number TEXT,
                actual_arrival_date DATE,
                
                -- Shipment details
                pieces INTEGER,
                gross_weight DECIMAL(10,2),
                chargeable_weight DECIMAL(10,2),  -- Air
                volume_cbm DECIMAL(10,2),  -- Sea
                container_info TEXT,  -- Sea
                
                -- Route
                origin_country TEXT,
                origin_port TEXT,
                destination_country TEXT,
                destination_port TEXT,
                terms TEXT,
                
                -- Origin charges
                origin_pickup_fee DECIMAL(10,2),
                origin_handling_fee DECIMAL(10,2),
                origin_customs_fee DECIMAL(10,2),
                origin_scan_fee DECIMAL(10,2),
                origin_other_charges DECIMAL(10,2),
                origin_freight DECIMAL(10,2),
                origin_subtotal DECIMAL(10,2),
                origin_currency TEXT,
                origin_fx_rate DECIMAL(10,6),
                origin_subtotal_cny DECIMAL(10,2),
                
                -- Destination charges
                dest_handling_fee DECIMAL(10,2),
                dest_pickup_fee DECIMAL(10,2),
                dest_storage_fee DECIMAL(10,2),
                dest_other_charges DECIMAL(10,2),
                dest_subtotal DECIMAL(10,2),
                dest_currency TEXT,
                dest_fx_rate DECIMAL(10,6),
                dest_subtotal_cny DECIMAL(10,2),
                
                -- Sea specific
                imo_charges DECIMAL(10,2),
                dtp_fee DECIMAL(10,2),
                
                -- Totals
                total_cny DECIMAL(10,2),
                
                -- Processing info
                file_path TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed_at TIMESTAMP,
                status TEXT DEFAULT 'PENDING'
            )
        ''')
        
        # DGF Audit Results table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dgf_audit_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_id INTEGER,
                quote_id TEXT,
                audit_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                -- Audit status
                overall_status TEXT,  -- PASS, FAIL, WARNING
                audit_score DECIMAL(5,2),
                
                -- Rate variances
                freight_variance_pct DECIMAL(5,2),
                freight_variance_amount DECIMAL(10,2),
                origin_variance_pct DECIMAL(5,2),
                origin_variance_amount DECIMAL(10,2),
                dest_variance_pct DECIMAL(5,2),
                dest_variance_amount DECIMAL(10,2),
                
                -- Exception flags
                rate_exception BOOLEAN DEFAULT 0,
                currency_exception BOOLEAN DEFAULT 0,
                route_exception BOOLEAN DEFAULT 0,
                weight_exception BOOLEAN DEFAULT 0,
                
                -- Comments and details
                audit_comments TEXT,
                exception_details TEXT,
                
                -- Financial impact
                overcharge_amount DECIMAL(10,2),
                undercharge_amount DECIMAL(10,2),
                net_variance DECIMAL(10,2),
                
                FOREIGN KEY (invoice_id) REFERENCES dgf_invoices (id)
            )
        ''')
        
        # DGF Exchange Rates table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dgf_exchange_rates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rate_date DATE,
                currency_from TEXT,
                currency_to TEXT,
                rate DECIMAL(10,6),
                source TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(rate_date, currency_from, currency_to)
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info("DGF audit tables initialized successfully")
    
    def extract_spot_quotes_from_air_file(self, file_path: str) -> List[Dict]:
        """Extract spot quotes from DGF Air Excel file."""
        try:
            df = pd.read_excel(file_path, sheet_name='Air')
            
            # Skip header row (row 0 contains column descriptions)
            df = df.iloc[1:].reset_index(drop=True)
            
            quotes = []
            for _, row in df.iterrows():
                quote_id = str(row['报价单号']).strip()
                if pd.isna(quote_id) or quote_id == '' or quote_id == 'nan':
                    continue
                
                # Extract lane information from quote_id if it contains route info
                lane_parts = quote_id.split('_')
                origin_port = lane_parts[1] if len(lane_parts) > 1 else 'SHA'
                
                quote = {
                    'quote_id': quote_id,
                    'mode': 'AIR',
                    'lane': f"{origin_port}-{row.get('发货港', 'UNKNOWN')}",
                    'origin_country': str(row.get('发货国', '')),
                    'origin_port': str(row.get('发货港', '')),
                    'destination_country': 'CN',  # Assuming China destination
                    'destination_port': origin_port,
                    'terms': str(row.get('条款', '')),
                    'quote_date': datetime.now().date(),
                    
                    # Calculate rate per kg from sample data
                    'chargeable_weight': float(row.get('计费重', 0)) if pd.notna(row.get('计费重')) else 0,
                    'origin_freight': float(row.get('港到港', 0)) if pd.notna(row.get('港到港')) else 0,
                    'origin_currency': str(row.get('到港币种', 'USD')),
                    'origin_fx_rate': float(row.get('到港汇率', 1)) if pd.notna(row.get('到港汇率')) else 1,
                    
                    # Extract fees
                    'origin_pickup_fee': float(row.get('提货费', 0)) if pd.notna(row.get('提货费')) else 0,
                    'origin_handling_fee': float(row.get('DTA操作费', 0)) if pd.notna(row.get('DTA操作费')) else 0,
                    'customs_clearance': float(row.get('报关费', 0)) if pd.notna(row.get('报关费')) else 0,
                    'dest_handling_fee': float(row.get('目的港操作费', 0)) if pd.notna(row.get('目的港操作费')) else 0,
                    'dest_currency': str(row.get('目的港币种', 'USD')),
                    'dest_fx_rate': float(row.get('目的港汇率', 1)) if pd.notna(row.get('目的港汇率')) else 1,
                }
                
                # Calculate rate per kg if we have valid data
                if quote['chargeable_weight'] > 0 and quote['origin_freight'] > 0:
                    quote['rate_per_kg'] = quote['origin_freight'] / quote['chargeable_weight']
                else:
                    quote['rate_per_kg'] = 0
                
                quotes.append(quote)
            
            logger.info(f"Extracted {len(quotes)} air quotes from {file_path}")
            return quotes
            
        except Exception as e:
            logger.error(f"Error extracting air quotes: {e}")
            return []
    
    def extract_spot_quotes_from_pdf_files(self, pdf_folder: str) -> List[Dict]:
        """
        Extract DGF spot quote data from PDF files in the specified folder
        """
        try:
            quotes = []
            
            if not os.path.exists(pdf_folder):
                logger.warning(f"PDF folder not found: {pdf_folder}")
                return quotes
            
            # Process each PDF file in the folder
            for filename in os.listdir(pdf_folder):
                if filename.endswith('.pdf'):
                    pdf_path = os.path.join(pdf_folder, filename)
                    try:
                        with open(pdf_path, 'rb') as file:
                            pdf_reader = PyPDF2.PdfReader(file)
                            page = pdf_reader.pages[0]
                            text = page.extract_text()
                            
                            quote_data = {}
                            lines = text.split('\n')
                            
                            # Extract quote information
                            for line in lines:
                                line = line.strip()
                                
                                # Quote Reference
                                if 'Quote Reference:' in line:
                                    parts = line.split('Quote Reference:')
                                    if len(parts) > 1:
                                        quote_data['quote_reference'] = parts[1].strip()
                                
                                # Origin Airport
                                if 'Origin Airport:' in line:
                                    parts = line.split('Origin Airport:')
                                    if len(parts) > 1:
                                        origin_info = parts[1].strip()
                                        # Extract airport code from format "SAN DIEGO (SAN), UNITED STATES"
                                        if '(' in origin_info and ')' in origin_info:
                                            airport_code = origin_info.split('(')[1].split(')')[0]
                                            quote_data['origin'] = airport_code
                                        else:
                                            quote_data['origin'] = origin_info
                                
                                # Destination Airport
                                if 'Destination Airport:' in line:
                                    parts = line.split('Destination Airport:')
                                    if len(parts) > 1:
                                        dest_info = parts[1].strip()
                                        # Extract airport code from format "SHANGHAI (SHA), CHINA"
                                        if '(' in dest_info and ')' in dest_info:
                                            airport_code = dest_info.split('(')[1].split(')')[0]
                                            quote_data['destination'] = airport_code
                                        else:
                                            quote_data['destination'] = dest_info
                                
                                # Total Chargeable Weight
                                if 'Total Chargeable Weight' in line:
                                    try:
                                        # Extract weight from format "9.00"
                                        weight_match = re.search(r'(\d+\.?\d*)\s*$', line)
                                        if weight_match:
                                            quote_data['chargeable_weight'] = float(weight_match.group(1))
                                    except:
                                        pass
                                
                                # Total Pickup Charges
                                if 'Total Pickup Charges' in line:
                                    try:
                                        # Extract amount from format "USD 40.00"
                                        amount_match = re.search(r'USD\s*(\d+\.?\d*)', line)
                                        if amount_match:
                                            quote_data['pickup_charges'] = float(amount_match.group(1))
                                    except:
                                        pass
                                
                                # Total Freight Charges
                                if 'Total Freight Charges' in line:
                                    try:
                                        # Extract amount from format "USD 425.00"
                                        amount_match = re.search(r'USD\s*(\d+[,\d]*\.?\d*)', line)
                                        if amount_match:
                                            amount_str = amount_match.group(1).replace(',', '')
                                            quote_data['freight_charges'] = float(amount_str)
                                    except:
                                        pass
                                
                                # Total Destination Charges
                                if 'Total Destination Charges' in line:
                                    try:
                                        # Extract amount from format "USD 58.55"
                                        amount_match = re.search(r'USD\s*(\d+\.?\d*)', line)
                                        if amount_match:
                                            quote_data['destination_charges'] = float(amount_match.group(1))
                                    except:
                                        pass
                                
                                # Grand Total
                                if 'Grand Total' in line and 'USD' in line:
                                    try:
                                        # Extract amount from format "USD 740.55"
                                        amount_match = re.search(r'USD\s*(\d+[,\d]*\.?\d*)', line)
                                        if amount_match:
                                            amount_str = amount_match.group(1).replace(',', '')
                                            quote_data['total_charges'] = float(amount_str)
                                    except:
                                        pass
                                
                                # Service Type (AIR PRIORITY, AIR ECONOMY)
                                if 'DHL AIR' in line:
                                    if 'PRIORITY' in line:
                                        quote_data['service_type'] = 'AIR PRIORITY'
                                    elif 'ECONOMY' in line:
                                        quote_data['service_type'] = 'AIR ECONOMY'
                                    else:
                                        quote_data['service_type'] = 'AIR'
                            
                            # Only add if we have essential data
                            if quote_data.get('quote_reference') and quote_data.get('origin') and quote_data.get('destination'):
                                # Convert to database format
                                quote = {
                                    'quote_id': quote_data['quote_reference'],
                                    'mode': 'AIR',
                                    'lane': f"{quote_data['origin']}-{quote_data['destination']}",
                                    'origin_port': quote_data['origin'],
                                    'destination_port': quote_data['destination'],
                                    'origin_country': '',  # Extract from airport codes if needed
                                    'destination_country': 'CN',  # Assuming China destination
                                    'terms': 'EXW',  # From PDFs
                                    'quote_date': datetime.now().date(),
                                    
                                    # Map PDF fields to database fields
                                    'origin_handling_fee': quote_data.get('pickup_charges', 0),  # Pickup charges
                                    'dest_handling_fee': quote_data.get('destination_charges', 0),  # Destination charges
                                    'rate_per_kg': 0,  # Calculate below
                                    'customs_clearance': 0,  # Not specifically extracted
                                    
                                    # Store the detailed charges for audit comparison
                                    'pdf_pickup_charges': quote_data.get('pickup_charges', 0),
                                    'pdf_freight_charges': quote_data.get('freight_charges', 0),
                                    'pdf_destination_charges': quote_data.get('destination_charges', 0),
                                    'pdf_total_charges': quote_data.get('total_charges', 0),
                                    'chargeable_weight': quote_data.get('chargeable_weight', 0),
                                    'service_type': quote_data.get('service_type', 'AIR'),
                                    'file_path': pdf_path,
                                    'pdf_filename': filename,
                                    'origin_currency': 'USD',
                                    'dest_currency': 'USD',
                                    'origin_fx_rate': 1.0,
                                    'dest_fx_rate': 1.0
                                }
                                
                                # Calculate rate per kg if possible
                                if quote['chargeable_weight'] > 0 and quote_data.get('freight_charges', 0) > 0:
                                    quote['rate_per_kg'] = quote_data['freight_charges'] / quote['chargeable_weight']
                                else:
                                    quote['rate_per_kg'] = 0
                                
                                quotes.append(quote)
                                logger.info(f"Extracted quote {quote['quote_id']} from {filename}")
                            else:
                                logger.warning(f"Incomplete quote data in {filename}")
                    
                    except Exception as e:
                        logger.error(f"Error processing PDF {filename}: {e}")
            
            logger.info(f"Successfully extracted {len(quotes)} quotes from PDF files")
            return quotes
            
        except Exception as e:
            logger.error(f"Error extracting quotes from PDF folder {pdf_folder}: {e}")
            return []
    
    def extract_spot_quotes_from_sea_file(self, file_path: str) -> List[Dict]:
        """Extract spot quotes from DGF Sea Excel file."""
        try:
            df = pd.read_excel(file_path, sheet_name='Sheet1')
            
            # Skip header row
            df = df.iloc[1:].reset_index(drop=True)
            
            quotes = []
            for _, row in df.iterrows():
                quote_id = str(row['报价单号']).strip()
                if pd.isna(quote_id) or quote_id == '' or quote_id == 'nan':
                    continue
                
                # Extract route from quote_id if it contains route info
                if ':' in quote_id:
                    route_part = quote_id.split(':')[0]
                    lane_parts = route_part.split('_')
                    origin_port = lane_parts[1] if len(lane_parts) > 1 else 'SHA'
                else:
                    lane_parts = quote_id.split('_')
                    origin_port = lane_parts[1] if len(lane_parts) > 1 else 'SHA'
                
                quote = {
                    'quote_id': quote_id,
                    'mode': 'SEA',
                    'lane': f"{origin_port}-{row.get('发货港', 'UNKNOWN')}",
                    'origin_country': str(row.get('发货国', '')),
                    'origin_port': str(row.get('发货港', '')),
                    'destination_country': 'CN',
                    'destination_port': origin_port,
                    'terms': str(row.get('条款', '')),
                    'quote_date': datetime.now().date(),
                    
                    # Sea specific
                    'volume_cbm': float(row.get('立方数', 0)) if pd.notna(row.get('立方数')) else 0,
                    'container_info': str(row.get('箱型、箱数', '')),
                    'origin_freight': float(row.get('港到港', 0)) if pd.notna(row.get('港到港')) else 0,
                    'origin_currency': str(row.get('到港币种', 'USD')),
                    'origin_fx_rate': float(row.get('到港汇率', 1)) if pd.notna(row.get('到港汇率')) else 1,
                    
                    # Extract fees
                    'origin_pickup_fee': float(row.get('提货费', 0)) if pd.notna(row.get('提货费')) else 0,
                    'origin_handling_fee': float(row.get('DTP操作费', 0)) if pd.notna(row.get('DTP操作费')) else 0,
                    'customs_clearance': float(row.get('报关费', 0)) if pd.notna(row.get('报关费')) else 0,
                    'imo_charges': float(row.get('IMO', 0)) if pd.notna(row.get('IMO')) else 0,
                    'dest_handling_fee': float(row.get('抽单费', 0)) if pd.notna(row.get('抽单费')) else 0,
                    'dest_currency': str(row.get('目的港币种', 'USD')),
                    'dest_fx_rate': float(row.get('目的港汇率J', 1)) if pd.notna(row.get('目的港汇率J')) else 1,
                }
                
                # Calculate rate per CBM if we have valid data
                if quote['volume_cbm'] > 0 and quote['origin_freight'] > 0:
                    quote['rate_per_cbm'] = quote['origin_freight'] / quote['volume_cbm']
                else:
                    quote['rate_per_cbm'] = 0
                
                quotes.append(quote)
            
            logger.info(f"Extracted {len(quotes)} sea quotes from {file_path}")
            return quotes
            
        except Exception as e:
            logger.error(f"Error extracting sea quotes: {e}")
            return []
    
    def save_spot_quotes(self, quotes: List[Dict]):
        """Save spot quotes to database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        saved_count = 0
        for quote in quotes:
            try:
                if quote['mode'] == 'AIR':
                    cursor.execute('''
                        INSERT OR REPLACE INTO dgf_spot_quotes 
                        (quote_id, mode, lane, origin_country, origin_port, destination_country, 
                         destination_port, terms, quote_date, rate_per_kg, origin_handling_fee, 
                         origin_currency, dest_handling_fee, dest_currency, customs_clearance,
                         origin_fx_rate, dest_fx_rate, pdf_pickup_charges, pdf_freight_charges,
                         pdf_destination_charges, pdf_total_charges, chargeable_weight)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        quote['quote_id'], quote['mode'], quote['lane'],
                        quote['origin_country'], quote['origin_port'], 
                        quote['destination_country'], quote['destination_port'],
                        quote['terms'], quote['quote_date'], quote.get('rate_per_kg', 0),
                        quote.get('origin_handling_fee', 0), quote['origin_currency'],
                        quote.get('dest_handling_fee', 0), quote['dest_currency'],
                        quote.get('customs_clearance', 0), quote['origin_fx_rate'],
                        quote['dest_fx_rate'], quote.get('pdf_pickup_charges', 0),
                        quote.get('pdf_freight_charges', 0), quote.get('pdf_destination_charges', 0),
                        quote.get('pdf_total_charges', 0), quote.get('chargeable_weight', 0)
                    ))
                else:  # SEA
                    cursor.execute('''
                        INSERT OR REPLACE INTO dgf_spot_quotes 
                        (quote_id, mode, lane, origin_country, origin_port, destination_country,
                         destination_port, terms, quote_date, rate_per_cbm, container_type,
                         origin_handling_fee, origin_currency, dest_handling_fee, dest_currency,
                         customs_clearance, origin_fx_rate, dest_fx_rate)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        quote['quote_id'], quote['mode'], quote['lane'],
                        quote['origin_country'], quote['origin_port'],
                        quote['destination_country'], quote['destination_port'],
                        quote['terms'], quote['quote_date'], quote.get('rate_per_cbm', 0),
                        quote.get('container_info', ''), quote.get('origin_handling_fee', 0),
                        quote['origin_currency'], quote.get('dest_handling_fee', 0),
                        quote['dest_currency'], quote.get('customs_clearance', 0),
                        quote['origin_fx_rate'], quote['dest_fx_rate']
                    ))
                saved_count += 1
            except Exception as e:
                logger.error(f"Error saving quote {quote['quote_id']}: {e}")
                continue
        
        conn.commit()
        conn.close()
        logger.info(f"Saved {saved_count} spot quotes to database")
    
    def load_and_process_dgf_files(self, air_file_path: str = None, sea_file_path: str = None, pdf_folder: str = None):
        """Load and process DGF files to extract spot quotes and invoices."""
        
        # Extract quotes from PDF files first (these are the actual baseline quotes)
        if pdf_folder and os.path.exists(pdf_folder):
            pdf_quotes = self.extract_spot_quotes_from_pdf_files(pdf_folder)
            self.save_spot_quotes(pdf_quotes)
        
        # Extract air invoices from Excel file
        if air_file_path and os.path.exists(air_file_path):
            # Save as invoices for auditing (not as quotes)
            self.save_air_invoices(air_file_path)
        
        # Extract sea invoices from Excel file  
        if sea_file_path and os.path.exists(sea_file_path):
            # Save as invoices for auditing (not as quotes)
            self.save_sea_invoices(sea_file_path)
    
    def save_air_invoices(self, file_path: str):
        """Save air freight invoices from Excel file."""
        try:
            df = pd.read_excel(file_path, sheet_name='Air')
            df = df.iloc[1:].reset_index(drop=True)  # Skip header
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            for _, row in df.iterrows():
                quote_id = str(row['报价单号']).strip()
                if pd.isna(quote_id) or quote_id == '' or quote_id == 'nan':
                    continue
                
                cursor.execute('''
                    INSERT OR REPLACE INTO dgf_invoices 
                    (quote_id, mode, hbl_number, actual_arrival_date, pieces, gross_weight,
                     chargeable_weight, origin_country, origin_port, terms, origin_pickup_fee,
                     origin_handling_fee, origin_customs_fee, origin_scan_fee, origin_other_charges,
                     origin_freight, origin_subtotal, origin_currency, origin_fx_rate,
                     origin_subtotal_cny, dest_handling_fee, dest_pickup_fee, dest_storage_fee,
                     dest_other_charges, dest_subtotal, dest_currency, dest_fx_rate,
                     dest_subtotal_cny, total_cny, file_path, processed_at, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    quote_id, 'AIR', str(row.get('分单号', '')),
                    row.get('实际到港日期'), int(row.get('件数', 0)) if pd.notna(row.get('件数')) else 0,
                    float(row.get('毛重', 0)) if pd.notna(row.get('毛重')) else 0,
                    float(row.get('计费重', 0)) if pd.notna(row.get('计费重')) else 0,
                    str(row.get('发货国', '')), str(row.get('发货港', '')), str(row.get('条款', '')),
                    float(row.get('提货费', 0)) if pd.notna(row.get('提货费')) else 0,
                    float(row.get('DTA操作费', 0)) if pd.notna(row.get('DTA操作费')) else 0,
                    float(row.get('报关费', 0)) if pd.notna(row.get('报关费')) else 0,
                    float(row.get('X或扫描费', 0)) if pd.notna(row.get('X或扫描费')) else 0,
                    float(row.get('其他', 0)) if pd.notna(row.get('其他')) else 0,
                    float(row.get('港到港', 0)) if pd.notna(row.get('港到港')) else 0,
                    float(row.get('到港小计', 0)) if pd.notna(row.get('到港小计')) else 0,
                    str(row.get('到港币种', 'USD')), 
                    float(row.get('到港汇率', 1)) if pd.notna(row.get('到港汇率')) else 1,
                    float(row.get('到港小计人民币', 0)) if pd.notna(row.get('到港小计人民币')) else 0,
                    float(row.get('目的港操作费', 0)) if pd.notna(row.get('目的港操作费')) else 0,
                    float(row.get('抽单费', 0)) if pd.notna(row.get('抽单费')) else 0,
                    float(row.get('仓储费', 0)) if pd.notna(row.get('仓储费')) else 0,
                    float(row.get('目的港费用', 0)) if pd.notna(row.get('目的港费用')) else 0,
                    float(row.get('目的港费用', 0)) if pd.notna(row.get('目的港费用')) else 0,
                    str(row.get('目的港币种', 'USD')),
                    float(row.get('目的港汇率', 1)) if pd.notna(row.get('目的港汇率')) else 1,
                    float(row.get('目的港费用人民币', 0)) if pd.notna(row.get('目的港费用人民币')) else 0,
                    float(row.get('总计', 0)) if pd.notna(row.get('总计')) else 0,
                    file_path, datetime.now(), 'PROCESSED'
                ))
            
            conn.commit()
            conn.close()
            logger.info(f"Saved air invoices from {file_path}")
            
        except Exception as e:
            logger.error(f"Error saving air invoices: {e}")
    
    def save_sea_invoices(self, file_path: str):
        """Save sea freight invoices from Excel file - Updated for DGF-CN10 billing.xlsx format."""
        try:
            # Try to read the Excel file - check if it has multiple sheets
            xl = pd.ExcelFile(file_path)
            sheet_names = xl.sheet_names
            logger.info(f"Available sheets: {sheet_names}")
            
            # Use the first sheet or look for a specific sheet name
            df = pd.read_excel(file_path, sheet_name=0)
            
            logger.info(f"Original columns in the file: {list(df.columns)}")
            logger.info(f"First few rows:")
            logger.info(str(df.head()))
            
            # Handle the case where the first row contains English headers
            # and the actual column names are in Chinese
            if len(df) > 0 and str(df.iloc[0, 0]).strip() == 'Lane ID/FQR#':
                logger.info("Detected English headers in first data row, using them as column names")
                # Use the first row as column names
                new_columns = df.iloc[0].tolist()
                # Remove the first row and reset the dataframe
                df = df.iloc[1:].copy()
                df.columns = new_columns
                df = df.reset_index(drop=True)
                logger.info(f"Updated columns: {list(df.columns)}")
                logger.info(f"Data after header correction:")
                logger.info(str(df.head()))
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            saved_count = 0
            
            for index, row in df.iterrows():
                try:
                    # Skip rows without invoice number
                    invoice_no = str(row.get('invoice No.', '')).strip()
                    if pd.isna(invoice_no) or invoice_no == '' or invoice_no == 'nan':
                        continue
                    
                    # Extract values based on the new DGF-CN10 format
                    lane_id_fqr = str(row.get('Lane ID/FQR#', '')).strip()
                    ata_date = row.get('ATA date')
                    hbl = str(row.get('HBL', '')).strip() 
                    pkg_no = int(row.get('PKG No', 0)) if pd.notna(row.get('PKG No')) else 0
                    m3 = float(row.get('M3', 0)) if pd.notna(row.get('M3')) else 0
                    container = str(row.get('Container', '')).strip()
                    inco_term = str(row.get('Inco-term', '')).strip()
                    origin_country = str(row.get('Origin Country', '')).strip()
                    origin_port = str(row.get('Origin Port', '')).strip()
                    currency_dta_dtp = str(row.get('Currency(DTA/DTP)', 'USD')).strip()
                    
                    # Charges
                    pickup = float(row.get('Pickup', 0)) if pd.notna(row.get('Pickup')) else 0
                    dtp_handling = float(row.get('DTP-Handling', 0)) if pd.notna(row.get('DTP-Handling')) else 0
                    customs = float(row.get('Customs', 0)) if pd.notna(row.get('Customs')) else 0
                    dtp_others = float(row.get('DTP-Others', 0)) if pd.notna(row.get('DTP-Others')) else 0
                    ptp = float(row.get('PTP', 0)) if pd.notna(row.get('PTP')) else 0
                    imo = float(row.get('IMO', 0)) if pd.notna(row.get('IMO')) else 0
                    
                    # Subtotals and exchange rates
                    subtotal_dta_dtp = float(row.get('Sub-total(DTA/DTP)', 0)) if pd.notna(row.get('Sub-total(DTA/DTP)')) else 0
                    exchange_rate_dta_dtp = float(row.get('Exchange rate(DTA/DTP)', 1)) if pd.notna(row.get('Exchange rate(DTA/DTP)')) else 1
                    subtotal_cny_dta_dtp = float(row.get('Sub-total CNY(DTA/DTP)', 0)) if pd.notna(row.get('Sub-total CNY(DTA/DTP)')) else 0
                    
                    # Destination charges
                    currency_dc = str(row.get('Currency(DC)', 'CNY')).strip()
                    doc_turnover = float(row.get('Doc Turnover', 0)) if pd.notna(row.get('Doc Turnover')) else 0
                    others = float(row.get('Others', 0)) if pd.notna(row.get('Others')) else 0
                    subtotal_dc = float(row.get('Sub-total', 0)) if pd.notna(row.get('Sub-total')) else 0
                    fx_rate_dc = float(row.get('FX Rate(DC)', 1)) if pd.notna(row.get('FX Rate(DC)')) else 1
                    subtotal_cny_dc = float(row.get('Sub-total CNY', 0)) if pd.notna(row.get('Sub-total CNY')) else 0
                    total_cny = float(row.get('Total CNY', 0)) if pd.notna(row.get('Total CNY')) else 0
                    
                    # Status and tax info
                    trax_status = str(row.get('Trax status', '')).strip()
                    tax_invoice = str(row.get('税票', '')).strip()
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO dgf_invoices 
                        (invoice_number, lane_id_fqr, ata_date, hbl_number, pkg_no, m3_volume,
                         container_info, inco_term, origin_country, origin_port, 
                         origin_currency, pickup_charge, dtp_handling_charge, customs_charge,
                         dtp_others_charge, ptp_charge, imo_charge, subtotal_dta_dtp,
                         exchange_rate_dta_dtp, subtotal_cny_dta_dtp, currency_dc,
                         doc_turnover, others_charge, subtotal_dc, fx_rate_dc,
                         subtotal_cny_dc, total_cny, trax_status, tax_invoice_number,
                         mode, file_path, processed_at, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        invoice_no, lane_id_fqr, ata_date, hbl, pkg_no, m3,
                        container, inco_term, origin_country, origin_port,
                        currency_dta_dtp, pickup, dtp_handling, customs,
                        dtp_others, ptp, imo, subtotal_dta_dtp,
                        exchange_rate_dta_dtp, subtotal_cny_dta_dtp, currency_dc,
                        doc_turnover, others, subtotal_dc, fx_rate_dc,
                        subtotal_cny_dc, total_cny, trax_status, tax_invoice,
                        'SEA', file_path, datetime.now(), 'PROCESSED'
                    ))
                    
                    saved_count += 1
                    logger.info(f"Saved invoice: {invoice_no}")
                    
                except Exception as e:
                    logger.error(f"Error processing row {index}: {e}")
                    continue
            
            conn.commit()
            conn.close()
            logger.info(f"Successfully saved {saved_count} sea invoices from {file_path}")
            
        except Exception as e:
            logger.error(f"Error saving sea invoices: {e}")
            raise
    
    def audit_invoice_against_quote(self, invoice_id: int) -> Dict:
        """Audit a single invoice against its corresponding spot quote."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get invoice details
        cursor.execute('SELECT * FROM dgf_invoices WHERE id = ?', (invoice_id,))
        invoice_row = cursor.fetchone()
        
        if not invoice_row:
            return {'error': 'Invoice not found'}
        
        # Convert to dict for easier access
        columns = [desc[0] for desc in cursor.description]
        invoice = dict(zip(columns, invoice_row))
        
        # Get corresponding quote
        cursor.execute('SELECT * FROM dgf_spot_quotes WHERE quote_id = ?', (invoice['quote_id'],))
        quote_row = cursor.fetchone()
        
        if not quote_row:
            return {'error': 'Quote not found', 'invoice_id': invoice_id, 'quote_id': invoice['quote_id']}
        
        # Convert to dict for easier access
        quote_columns = [desc[0] for desc in cursor.description]
        quote = dict(zip(quote_columns, quote_row))
        
        # Perform audit comparison
        audit_result = {
            'invoice_id': invoice_id,
            'quote_id': invoice['quote_id'],
            'mode': invoice['mode'],
            'overall_status': 'PASS',
            'audit_score': 100.0,
            'variances': [],
            'exceptions': [],
            'overcharge_amount': 0.0,
            'undercharge_amount': 0.0
        }
        
        # Tolerance levels
        RATE_TOLERANCE = 0.05  # 5%
        FEE_TOLERANCE = 0.10   # 10%
        
        # Helper function to safely convert to float
        def safe_float(value, default=0.0):
            if value is None:
                return default
            try:
                return float(value)
            except (ValueError, TypeError):
                return default
        
        if invoice['mode'] == 'AIR':  # Air freight audit
            # Compare freight rates
            chargeable_weight = safe_float(invoice['chargeable_weight'])
            rate_per_kg = safe_float(quote['rate_per_kg'])
            actual_freight = safe_float(invoice['origin_freight'])
            
            if chargeable_weight > 0 and rate_per_kg > 0:
                expected_freight = chargeable_weight * rate_per_kg
                
                if actual_freight > 0:
                    variance_pct = abs(actual_freight - expected_freight) / expected_freight
                    if variance_pct > RATE_TOLERANCE:
                        audit_result['variances'].append({
                            'type': 'FREIGHT_RATE',
                            'expected': expected_freight,
                            'actual': actual_freight,
                            'variance_pct': variance_pct * 100,
                            'variance_amount': actual_freight - expected_freight
                        })
                        audit_result['overall_status'] = 'FAIL' if variance_pct > 0.15 else 'WARNING'
                        audit_result['audit_score'] -= min(50, variance_pct * 100)
        
        elif invoice['mode'] == 'SEA':  # Sea freight audit
            # Compare freight rates
            volume_cbm = safe_float(invoice['volume_cbm'])
            rate_per_cbm = safe_float(quote['rate_per_cbm'])
            actual_freight = safe_float(invoice['origin_freight'])
            
            if volume_cbm > 0 and rate_per_cbm > 0:
                expected_freight = volume_cbm * rate_per_cbm
                
                if actual_freight > 0:
                    variance_pct = abs(actual_freight - expected_freight) / expected_freight
                    if variance_pct > RATE_TOLERANCE:
                        audit_result['variances'].append({
                            'type': 'FREIGHT_RATE',
                            'expected': expected_freight,
                            'actual': actual_freight,
                            'variance_pct': variance_pct * 100,
                            'variance_amount': actual_freight - expected_freight
                        })
                        audit_result['overall_status'] = 'FAIL' if variance_pct > 0.15 else 'WARNING'
                        audit_result['audit_score'] -= min(50, variance_pct * 100)
        
        # Compare handling fees
        quote_origin_fee = safe_float(quote['origin_handling_fee'])
        invoice_origin_fee = safe_float(invoice['origin_handling_fee'])
        
        if quote_origin_fee > 0 and invoice_origin_fee > 0:
            fee_variance_pct = abs(invoice_origin_fee - quote_origin_fee) / quote_origin_fee
            if fee_variance_pct > FEE_TOLERANCE:
                audit_result['variances'].append({
                    'type': 'ORIGIN_HANDLING_FEE',
                    'expected': quote_origin_fee,
                    'actual': invoice_origin_fee,
                    'variance_pct': fee_variance_pct * 100,
                    'variance_amount': invoice_origin_fee - quote_origin_fee
                })
                if audit_result['overall_status'] == 'PASS':
                    audit_result['overall_status'] = 'WARNING'
                audit_result['audit_score'] -= min(25, fee_variance_pct * 50)
        
        # Calculate financial impact
        total_variance = sum(v.get('variance_amount', 0) for v in audit_result['variances'])
        if total_variance > 0:
            audit_result['overcharge_amount'] = total_variance
        else:
            audit_result['undercharge_amount'] = abs(total_variance)
        
        audit_result['net_variance'] = total_variance
        
        # Save audit result
        self.save_audit_result(audit_result)
        
        conn.close()
        return audit_result
    
    def save_audit_result(self, audit_result: Dict):
        """Save audit result to database."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Calculate aggregate variances
        freight_variance = next((v for v in audit_result['variances'] if v['type'] == 'FREIGHT_RATE'), {})
        origin_variance = next((v for v in audit_result['variances'] if v['type'] == 'ORIGIN_HANDLING_FEE'), {})
        
        cursor.execute('''
            INSERT OR REPLACE INTO dgf_audit_results
            (invoice_id, quote_id, overall_status, audit_score, freight_variance_pct,
             freight_variance_amount, origin_variance_pct, origin_variance_amount,
             overcharge_amount, undercharge_amount, net_variance, audit_comments)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            audit_result['invoice_id'], audit_result['quote_id'], audit_result['overall_status'],
            audit_result['audit_score'], freight_variance.get('variance_pct', 0),
            freight_variance.get('variance_amount', 0), origin_variance.get('variance_pct', 0),
            origin_variance.get('variance_amount', 0), audit_result['overcharge_amount'],
            audit_result['undercharge_amount'], audit_result['net_variance'],
            f"Found {len(audit_result['variances'])} variances"
        ))
        
        conn.commit()
        conn.close()
    
    def audit_all_invoices(self) -> Dict:
        """Audit all processed invoices against their quotes."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT id FROM dgf_invoices WHERE status = "PROCESSED"')
        invoice_ids = [row[0] for row in cursor.fetchall()]
        
        results = {
            'total_invoices': len(invoice_ids),
            'audited': 0,
            'passed': 0,
            'warnings': 0,
            'failed': 0,
            'errors': 0,
            'total_overcharge': 0.0,
            'total_undercharge': 0.0
        }
        
        for invoice_id in invoice_ids:
            try:
                audit_result = self.audit_invoice_against_quote(invoice_id)
                
                if 'error' in audit_result:
                    results['errors'] += 1
                    continue
                
                results['audited'] += 1
                
                if audit_result['overall_status'] == 'PASS':
                    results['passed'] += 1
                elif audit_result['overall_status'] == 'WARNING':
                    results['warnings'] += 1
                else:
                    results['failed'] += 1
                
                results['total_overcharge'] += audit_result.get('overcharge_amount', 0)
                results['total_undercharge'] += audit_result.get('undercharge_amount', 0)
                
            except Exception as e:
                logger.error(f"Error auditing invoice {invoice_id}: {e}")
                results['errors'] += 1
        
        conn.close()
        return results
    
    def generate_audit_report(self, output_file: str = 'dgf_audit_report.xlsx'):
        """Generate comprehensive audit report."""
        conn = sqlite3.connect(self.db_path)
        
        # Get audit results with invoice details
        query = '''
            SELECT 
                i.quote_id,
                i.mode,
                i.hbl_number,
                i.actual_arrival_date,
                i.origin_port,
                i.destination_port,
                i.total_cny,
                ar.overall_status,
                ar.audit_score,
                ar.freight_variance_pct,
                ar.freight_variance_amount,
                ar.origin_variance_pct,
                ar.origin_variance_amount,
                ar.overcharge_amount,
                ar.undercharge_amount,
                ar.net_variance,
                ar.audit_date
            FROM dgf_invoices i
            LEFT JOIN dgf_audit_results ar ON i.id = ar.invoice_id
            WHERE i.status = 'PROCESSED'
            ORDER BY ar.audit_date DESC
        '''
        
        df_results = pd.read_sql_query(query, conn)
        
        # Get summary statistics
        summary_query = '''
            SELECT 
                mode,
                COUNT(*) as total_invoices,
                SUM(CASE WHEN overall_status = 'PASS' THEN 1 ELSE 0 END) as passed,
                SUM(CASE WHEN overall_status = 'WARNING' THEN 1 ELSE 0 END) as warnings,
                SUM(CASE WHEN overall_status = 'FAIL' THEN 1 ELSE 0 END) as failed,
                AVG(audit_score) as avg_score,
                SUM(overcharge_amount) as total_overcharge,
                SUM(undercharge_amount) as total_undercharge,
                SUM(net_variance) as net_impact
            FROM dgf_audit_results ar
            JOIN dgf_invoices i ON ar.invoice_id = i.id
            GROUP BY mode
        '''
        
        df_summary = pd.read_sql_query(summary_query, conn)
        
        # Get spot quotes summary
        quotes_query = '''
            SELECT 
                mode,
                COUNT(*) as total_quotes,
                AVG(rate_per_kg) as avg_air_rate,
                AVG(rate_per_cbm) as avg_sea_rate,
                COUNT(DISTINCT lane) as unique_lanes
            FROM dgf_spot_quotes
            WHERE status = 'ACTIVE'
            GROUP BY mode
        '''
        
        df_quotes = pd.read_sql_query(quotes_query, conn)
        
        # Write to Excel
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            df_results.to_excel(writer, sheet_name='Audit Results', index=False)
            df_summary.to_excel(writer, sheet_name='Summary', index=False)
            df_quotes.to_excel(writer, sheet_name='Quote Summary', index=False)
        
        conn.close()
        logger.info(f"Audit report generated: {output_file}")
        return output_file

def main():
    """Main function to run DGF audit system."""
    audit_system = DGFAuditSystem()
    
    # Define file paths
    air_file = r'c:\ChinaLogisticsAudit\uploads\DGF AIR\DGF Air.xlsx'
    sea_file = r'c:\ChinaLogisticsAudit\uploads\DGF SEA\DGF-CN10 billing.xlsx'
    
    print("=== DGF Audit System ===")
    print("Processing DGF files and extracting spot quotes...")
    
    # Load and process files
    audit_system.load_and_process_dgf_files(air_file, sea_file)
    
    print("Running audit against spot quotes...")
    
    # Run audit
    audit_results = audit_system.audit_all_invoices()
    
    print("\n=== Audit Results Summary ===")
    print(f"Total invoices: {audit_results['total_invoices']}")
    print(f"Successfully audited: {audit_results['audited']}")
    print(f"Passed: {audit_results['passed']}")
    print(f"Warnings: {audit_results['warnings']}")
    print(f"Failed: {audit_results['failed']}")
    print(f"Errors: {audit_results['errors']}")
    print(f"Total overcharge: ¥{audit_results['total_overcharge']:.2f}")
    print(f"Total undercharge: ¥{audit_results['total_undercharge']:.2f}")
    
    # Generate report
    report_file = audit_system.generate_audit_report()
    print(f"\nDetailed audit report saved to: {report_file}")

if __name__ == '__main__':
    main()
