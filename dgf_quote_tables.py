#!/usr/bin/env python3
"""
DGF Quote Tables Creation - Separate tables for AIR, FCL, and LCL quotes
This module creates dedicated database tables for different quote types
"""

import sqlite3
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DGFQuoteTables:
    def __init__(self, db_path: str = 'dhl_audit.db', force_recreate: bool = False):
        self.db_path = db_path
        self.create_quote_tables(force_recreate)
    
    def create_quote_tables(self, force_recreate: bool = False):
        """Create separate tables for AIR, FCL, and LCL quotes.
        
        Args:
            force_recreate: If True, drop existing tables and recreate them.
                          If False, only create tables if they don't exist.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if force_recreate:
            # Drop existing tables if they exist (fresh start)
            cursor.execute('DROP TABLE IF EXISTS dgf_air_quotes')
            cursor.execute('DROP TABLE IF EXISTS dgf_fcl_quotes')
            cursor.execute('DROP TABLE IF EXISTS dgf_lcl_quotes')
            logger.info("Dropped existing DGF quote tables for recreation")
        
        # DGF AIR Quotes Table - Updated to match actual air quote format
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dgf_air_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- Core Quote Information (from actual file structure)
                field_type TEXT, -- From 'Fild' column
                quote_reference_no TEXT UNIQUE NOT NULL, -- From 'Quote Reference No.'
                vendor_name TEXT, -- From 'Vendor Name'
                validity_period TEXT, -- From 'Validity Period' (original format)
                validity_start DATE, -- Parsed from validity_period
                validity_end DATE, -- Parsed from validity_period
                
                -- Route Information (from actual file structure)
                origin TEXT, -- From 'Origin' (full text like "DALLAS (DFW), US")
                destination TEXT, -- From 'Destination' (full text like "SHANGHAI(PVG), China")
                origin_airport_code TEXT, -- Extracted from origin
                destination_airport_code TEXT, -- Extracted from destination
                origin_country TEXT, -- Extracted from origin
                destination_country TEXT, -- Extracted from destination
                
                -- Service Information (from actual file structure)
                service_type TEXT, -- From 'Service Type'
                incoterms TEXT, -- From 'Incoterms'
                transit_time TEXT, -- From 'Transit Time' (original format like "6 days")
                transit_time_days INTEGER, -- Parsed from transit_time
                currency TEXT DEFAULT 'USD', -- From 'Currency'
                
                -- Cost Breakdown (exactly as in air quote file)
                dtp_min_charge DECIMAL(10,4), -- From 'DTP Min Charge'
                dtp_freight_cost DECIMAL(10,4), -- From 'DTP Freight Cost'
                customs_clearance DECIMAL(10,4), -- From 'CUSTOMS CLEARANCE'
                origin_min_charge DECIMAL(10,4), -- From 'Origin Min Charge'
                origin_fees DECIMAL(10,4), -- From 'Origin Fees (THC, ISS, Screening, etc.)'
                per_shipment_charges DECIMAL(10,4), -- From 'Per shpt charges'
                ata_min_charge DECIMAL(10,4), -- From 'ATA Min Charge'
                ata_cost_charge DECIMAL(10,4), -- From 'ATA Cost Charge'
                destination_min_charge DECIMAL(10,4), -- From 'Destination Min Charge'
                destination_fees DECIMAL(10,4), -- From 'Destination Fees (THC, ISS, Screening, etc.)'
                total_charges DECIMAL(12,4), -- From 'Total charges'
                remarks TEXT, -- From 'Remarks'
                
                -- Calculated/Derived Fields
                rate_per_kg DECIMAL(10,4), -- Calculated main rate (could be dtp_freight_cost)
                
                -- Upload Information
                file_name TEXT,
                sheet_name TEXT,
                row_number INTEGER,
                uploaded_by TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'ACTIVE' -- ACTIVE, EXPIRED, SUPERSEDED
            )
        ''')
        
        # DGF FCL (Full Container Load) Quotes Table - Based on actual FCL quote format
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dgf_fcl_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- Basic Quote Information (from Excel columns)
                field_type TEXT, -- From 'Fild' column (Sample, etc.)
                quote_reference_no TEXT UNIQUE NOT NULL, -- From 'Quote Reference No.'
                vendor_name TEXT, -- From 'Vendor Name'
                validity_period TEXT, -- From 'Validity Period' (8/21/2025-9/3/2025)
                validity_start DATE, -- Parsed from validity_period
                validity_end DATE, -- Parsed from validity_period
                
                -- Route Information
                origin TEXT, -- From 'Origin' (HAIPHONG (VNHPH), VIETNAM)
                destination TEXT, -- From 'Destination' (SHANGHAI (CNSHA), CHINA)
                origin_port_code TEXT, -- Extracted from origin (VNHPH)
                destination_port_code TEXT, -- Extracted from destination (CNSHA)
                origin_country TEXT, -- Extracted from origin (VIETNAM)
                destination_country TEXT, -- Extracted from destination (CHINA)
                
                -- Service Information
                service_type TEXT, -- From 'Service Type' (FCL)
                incoterms TEXT, -- From 'Incoterms' (FCA)
                transit_time TEXT, -- From 'Transit Time' (14 days)
                transit_time_days INTEGER, -- Parsed from transit_time
                currency TEXT, -- From 'Currency' (USD)
                
                -- Pickup Charges
                pickup_charges_20 DECIMAL(10,4), -- From "Pickup Charges 20'"
                pickup_charges_40 DECIMAL(10,4), -- From "Pickup Charges 40'"
                
                -- Origin Charges
                customs_clearance DECIMAL(10,4), -- From 'CUSTOMS CLEARANCE'
                origin_handling_20 DECIMAL(10,4), -- From "Origin Handling 20'"
                origin_handling_40 DECIMAL(10,4), -- From "Origin Handling 40'"
                
                -- Freight Rates
                freight_rate_20 DECIMAL(10,4), -- From "Freight Rate 20'"
                freight_rate_40 DECIMAL(10,4), -- From "Freight Rate 40'"
                
                -- Additional Charges
                per_shipment_charges DECIMAL(10,4), -- From 'Per Shipment Charges'
                destination_handling DECIMAL(10,4), -- From 'Destination Handling'
                total_charges DECIMAL(12,4), -- From 'Total Charges'
                
                -- Upload Information
                file_name TEXT,
                sheet_name TEXT,
                row_number INTEGER,
                uploaded_by TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'ACTIVE' -- ACTIVE, EXPIRED, SUPERSEDED
            )
        ''')
        
        # DGF LCL (Less than Container Load) Quotes Table - Based on actual LCL quote format
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dgf_lcl_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                
                -- Basic Quote Information (from Excel columns)
                field_type TEXT, -- From 'Fild' column (Sample, etc.)
                quote_reference_no TEXT UNIQUE NOT NULL, -- From 'Quote Reference No.'
                vendor_name TEXT, -- From 'Vendor Name'
                validity_period TEXT, -- From 'Validity Period' (8/12/2025-8/24/2025)
                validity_start DATE, -- Parsed from validity_period
                validity_end DATE, -- Parsed from validity_period
                
                -- Route Information
                origin TEXT, -- From 'Origin' (NEW YORK (USNYC), UNITED STATES)
                destination TEXT, -- From 'Destination' (SHANGHAI (CNSHA), CHINA)
                origin_port_code TEXT, -- Extracted from origin (USNYC)
                destination_port_code TEXT, -- Extracted from destination (CNSHA)
                origin_country TEXT, -- Extracted from origin (UNITED STATES)
                destination_country TEXT, -- Extracted from destination (CHINA)
                
                -- Service Information
                service_type TEXT, -- From 'Service Type' (LCL)
                incoterms TEXT, -- From 'Incoterms' (EXW)
                transit_time TEXT, -- From 'Transit Time' (60 days)
                transit_time_days INTEGER, -- Parsed from transit_time
                currency TEXT, -- From 'Currency' (USD)
                
                -- Pickup Charges
                lcl_pickup_charges_min DECIMAL(10,4), -- From 'LCL Pickup Charges Min'
                lcl_pickup_charges_rate DECIMAL(10,4), -- From 'LCL Pickup Charges Rate'
                
                -- Origin Charges
                customs_clearance DECIMAL(10,4), -- From 'CUSTOMS CLEARANCE'
                lcl_origin_handling_min DECIMAL(10,4), -- From 'LCL Origin Handling Min'
                lcl_origin_handling DECIMAL(10,4), -- From 'LCL Origin Handling'
                
                -- Freight Rates
                per_shipment_charges DECIMAL(10,4), -- From 'Per Shipment Charges'
                lcl_freight_min DECIMAL(10,4), -- From 'LCL Freight Min'
                lcl_freight_rate DECIMAL(10,4), -- From 'LCL Freight Rate'
                
                -- Destination Charges
                lcl_destination_handling_min DECIMAL(10,4), -- From 'LCL Destination Handling Min'
                lcl_destination_handling_rate DECIMAL(10,4), -- From 'LCL Destination Handling Rate'
                dest_document_handover DECIMAL(10,4), -- From 'Dest. Document Handove'
                total_charges DECIMAL(12,4), -- From 'Total Charges'
                
                -- Upload Information
                file_name TEXT,
                sheet_name TEXT,
                row_number INTEGER,
                uploaded_by TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'ACTIVE' -- ACTIVE, EXPIRED, SUPERSEDED
            )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_air_quotes_route ON dgf_air_quotes(origin_airport_code, destination_airport_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_air_quotes_date ON dgf_air_quotes(validity_start, validity_end)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_fcl_quotes_route ON dgf_fcl_quotes(origin_port_code, destination_port_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_fcl_quotes_date ON dgf_fcl_quotes(validity_start, validity_end)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_lcl_quotes_route ON dgf_lcl_quotes(origin_port_code, destination_port_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_lcl_quotes_date ON dgf_lcl_quotes(validity_start, validity_end)')
        
        # DGF Users Table - For storing upload user names
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS dgf_upload_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                full_name TEXT UNIQUE NOT NULL,
                first_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_used TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                usage_count INTEGER DEFAULT 1
            )
        ''')
        
        # Insert default user if not exists
        cursor.execute('''
            INSERT OR IGNORE INTO dgf_upload_users (full_name, usage_count) 
            VALUES ('JOE XIE', 1)
        ''')
        
        conn.commit()
        conn.close()
        
        if force_recreate:
            logger.info("DGF quote tables recreated successfully")
        else:
            logger.info("DGF quote tables created successfully")
    
    def get_table_stats(self):
        """Get statistics for all quote tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        stats = {}
        
        # AIR quotes stats
        cursor.execute('SELECT COUNT(*) FROM dgf_air_quotes WHERE status = "ACTIVE"')
        stats['air_active'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM dgf_air_quotes')
        stats['air_total'] = cursor.fetchone()[0]
        
        # FCL quotes stats
        cursor.execute('SELECT COUNT(*) FROM dgf_fcl_quotes WHERE status = "ACTIVE"')
        stats['fcl_active'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM dgf_fcl_quotes')
        stats['fcl_total'] = cursor.fetchone()[0]
        
        # LCL quotes stats
        cursor.execute('SELECT COUNT(*) FROM dgf_lcl_quotes WHERE status = "ACTIVE"')
        stats['lcl_active'] = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM dgf_lcl_quotes')
        stats['lcl_total'] = cursor.fetchone()[0]
        
        conn.close()
        return stats
    
    def get_upload_users(self):
        """Get list of users for the dropdown, ordered by usage frequency."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT full_name, usage_count 
            FROM dgf_upload_users 
            ORDER BY usage_count DESC, last_used DESC
        ''')
        users = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        return users
    
    def add_or_update_user(self, full_name):
        """Add a new user or update usage count for existing user."""
        if not full_name or not full_name.strip():
            return
            
        full_name = full_name.strip()
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Check if user exists
        cursor.execute('SELECT id, usage_count FROM dgf_upload_users WHERE full_name = ?', (full_name,))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing user
            cursor.execute('''
                UPDATE dgf_upload_users 
                SET usage_count = usage_count + 1, last_used = CURRENT_TIMESTAMP 
                WHERE full_name = ?
            ''', (full_name,))
        else:
            # Add new user
            cursor.execute('''
                INSERT INTO dgf_upload_users (full_name) 
                VALUES (?)
            ''', (full_name,))
        
        conn.commit()
        conn.close()

if __name__ == "__main__":
    # Create the tables
    dgf_tables = DGFQuoteTables()
    print("DGF Quote tables created successfully!")
    
    # Display stats
    stats = dgf_tables.get_table_stats()
    print(f"AIR Quotes: {stats['air_active']} active, {stats['air_total']} total")
    print(f"FCL Quotes: {stats['fcl_active']} active, {stats['fcl_total']} total")
    print(f"LCL Quotes: {stats['lcl_active']} active, {stats['lcl_total']} total")
