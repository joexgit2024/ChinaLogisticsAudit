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
    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path
        self.create_quote_tables()
    
    def create_quote_tables(self):
        """Create separate tables for AIR, FCL, and LCL quotes."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Drop existing tables if they exist (fresh start)
        cursor.execute('DROP TABLE IF EXISTS dgf_air_quotes')
        cursor.execute('DROP TABLE IF EXISTS dgf_fcl_quotes')
        cursor.execute('DROP TABLE IF EXISTS dgf_lcl_quotes')
        
        # DGF AIR Quotes Table
        cursor.execute('''
            CREATE TABLE dgf_air_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quote_id TEXT UNIQUE NOT NULL,
                quote_date DATE,
                validity_start DATE,
                validity_end DATE,
                
                -- Route Information
                origin_country TEXT,
                origin_city TEXT,
                origin_airport_code TEXT,
                destination_country TEXT,
                destination_city TEXT,
                destination_airport_code TEXT,
                
                -- Rate Information
                rate_per_kg DECIMAL(10,4),
                min_weight_kg DECIMAL(10,2),
                max_weight_kg DECIMAL(10,2),
                currency TEXT DEFAULT 'USD',
                
                -- Service Details
                transit_time_days INTEGER,
                service_type TEXT, -- Express, Standard, Economy
                
                -- Additional Charges
                fuel_surcharge_pct DECIMAL(5,2),
                security_surcharge DECIMAL(10,2),
                handling_fee DECIMAL(10,2),
                documentation_fee DECIMAL(10,2),
                customs_clearance_fee DECIMAL(10,2),
                pickup_fee DECIMAL(10,2),
                delivery_fee DECIMAL(10,2),
                other_charges DECIMAL(10,2),
                other_charges_description TEXT,
                
                -- Terms and Conditions
                incoterms TEXT, -- EXW, FOB, CIF, etc.
                payment_terms TEXT,
                special_instructions TEXT,
                
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
        
        # DGF FCL (Full Container Load) Quotes Table
        cursor.execute('''
            CREATE TABLE dgf_fcl_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quote_id TEXT UNIQUE NOT NULL,
                quote_date DATE,
                validity_start DATE,
                validity_end DATE,
                
                -- Route Information
                origin_country TEXT,
                origin_port TEXT,
                origin_port_code TEXT,
                destination_country TEXT,
                destination_port TEXT,
                destination_port_code TEXT,
                
                -- Container Information
                container_type TEXT, -- 20GP, 40GP, 40HQ, etc.
                container_size INTEGER, -- 20, 40
                container_height TEXT, -- Standard, High Cube
                
                -- Rate Information
                rate_per_container DECIMAL(10,2),
                currency TEXT DEFAULT 'USD',
                
                -- Service Details
                transit_time_days INTEGER,
                service_type TEXT, -- Direct, Transshipment
                vessel_operator TEXT,
                
                -- Origin Charges
                origin_terminal_handling DECIMAL(10,2),
                origin_documentation DECIMAL(10,2),
                origin_customs_clearance DECIMAL(10,2),
                origin_trucking DECIMAL(10,2),
                origin_other_charges DECIMAL(10,2),
                origin_charges_currency TEXT DEFAULT 'CNY',
                
                -- Destination Charges
                dest_terminal_handling DECIMAL(10,2),
                dest_documentation DECIMAL(10,2),
                dest_customs_clearance DECIMAL(10,2),
                dest_trucking DECIMAL(10,2),
                dest_other_charges DECIMAL(10,2),
                dest_charges_currency TEXT DEFAULT 'USD',
                
                -- Additional Information
                bunker_adjustment_factor DECIMAL(5,2),
                currency_adjustment_factor DECIMAL(5,2),
                equipment_imbalance_surcharge DECIMAL(10,2),
                
                -- Terms and Conditions
                incoterms TEXT,
                payment_terms TEXT,
                free_time_days INTEGER,
                demurrage_rate DECIMAL(10,2),
                detention_rate DECIMAL(10,2),
                special_instructions TEXT,
                
                -- Upload Information
                file_name TEXT,
                sheet_name TEXT,
                row_number INTEGER,
                uploaded_by TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'ACTIVE'
            )
        ''')
        
        # DGF LCL (Less than Container Load) Quotes Table
        cursor.execute('''
            CREATE TABLE dgf_lcl_quotes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                quote_id TEXT UNIQUE NOT NULL,
                quote_date DATE,
                validity_start DATE,
                validity_end DATE,
                
                -- Route Information
                origin_country TEXT,
                origin_port TEXT,
                origin_port_code TEXT,
                destination_country TEXT,
                destination_port TEXT,
                destination_port_code TEXT,
                
                -- Rate Information
                rate_per_cbm DECIMAL(10,4),
                rate_per_ton DECIMAL(10,4),
                min_charge_cbm DECIMAL(10,2),
                min_charge_ton DECIMAL(10,2),
                currency TEXT DEFAULT 'USD',
                
                -- Weight/Measurement
                weight_measure_ratio DECIMAL(5,2), -- W/M ratio (typically 1000kg = 1cbm)
                
                -- Service Details
                transit_time_days INTEGER,
                service_type TEXT,
                consolidation_port TEXT,
                
                -- Origin Charges
                origin_handling_fee DECIMAL(10,2),
                origin_documentation DECIMAL(10,2),
                origin_customs_clearance DECIMAL(10,2),
                origin_pickup_fee DECIMAL(10,2),
                origin_other_charges DECIMAL(10,2),
                origin_charges_currency TEXT DEFAULT 'CNY',
                
                -- Destination Charges
                dest_handling_fee DECIMAL(10,2),
                dest_documentation DECIMAL(10,2),
                dest_customs_clearance DECIMAL(10,2),
                dest_delivery_fee DECIMAL(10,2),
                dest_other_charges DECIMAL(10,2),
                dest_charges_currency TEXT DEFAULT 'USD',
                
                -- Additional Charges
                bunker_adjustment_factor DECIMAL(5,2),
                currency_adjustment_factor DECIMAL(5,2),
                consolidation_fee DECIMAL(10,2),
                deconsolidation_fee DECIMAL(10,2),
                
                -- Terms and Conditions
                incoterms TEXT,
                payment_terms TEXT,
                free_time_days INTEGER,
                storage_rate DECIMAL(10,2),
                special_instructions TEXT,
                
                -- Upload Information
                file_name TEXT,
                sheet_name TEXT,
                row_number INTEGER,
                uploaded_by TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'ACTIVE'
            )
        ''')
        
        # Create indexes for better performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_air_quotes_route ON dgf_air_quotes(origin_airport_code, destination_airport_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_air_quotes_date ON dgf_air_quotes(validity_start, validity_end)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_fcl_quotes_route ON dgf_fcl_quotes(origin_port_code, destination_port_code, container_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_fcl_quotes_date ON dgf_fcl_quotes(validity_start, validity_end)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_lcl_quotes_route ON dgf_lcl_quotes(origin_port_code, destination_port_code)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_lcl_quotes_date ON dgf_lcl_quotes(validity_start, validity_end)')
        
        conn.commit()
        conn.close()
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

if __name__ == "__main__":
    # Create the tables
    dgf_tables = DGFQuoteTables()
    print("DGF Quote tables created successfully!")
    
    # Display stats
    stats = dgf_tables.get_table_stats()
    print(f"AIR Quotes: {stats['air_active']} active, {stats['air_total']} total")
    print(f"FCL Quotes: {stats['fcl_active']} active, {stats['fcl_total']} total")
    print(f"LCL Quotes: {stats['lcl_active']} active, {stats['lcl_total']} total")
