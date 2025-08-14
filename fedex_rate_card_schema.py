#!/usr/bin/env python3
"""
FedEx Rate Card Database Schema and Loader
=========================================

Creates the database schema for FedEx rate cards based on the analysis
of the FedEx rate card structure.
"""

import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple

class FedExRateCardSchema:
    """Creates and manages FedEx rate card database schema"""
    
    def __init__(self, db_path: str = 'fedex_audit.db'):
        self.db_path = db_path
    
    def create_all_tables(self):
        """Create all FedEx rate card tables"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # 1. Zone Regions Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fedex_zone_regions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    region_code VARCHAR(20) UNIQUE NOT NULL,
                    region_name VARCHAR(100) NOT NULL,
                    description TEXT,
                    active BOOLEAN DEFAULT 1,
                    created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 2. Zone Matrix Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fedex_zone_matrix (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    origin_region VARCHAR(20) NOT NULL,
                    destination_region VARCHAR(20) NOT NULL,
                    zone_code VARCHAR(10) NOT NULL,
                    service_type VARCHAR(50) DEFAULT 'EXPRESS',
                    effective_date DATE,
                    expiry_date DATE,
                    created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(origin_region, destination_region, service_type)
                )
            ''')
            
            # 3. Country Zone Mappings
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fedex_country_zones (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    country_code VARCHAR(3) NOT NULL,
                    country_name VARCHAR(100) NOT NULL,
                    region_code VARCHAR(20) NOT NULL,
                    sub_region VARCHAR(50),
                    zone_letter VARCHAR(2),
                    currency_code VARCHAR(3),
                    exchange_rate DECIMAL(10,6),
                    active BOOLEAN DEFAULT 1,
                    created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(country_code)
                )
            ''')
            
            # 4. Rate Cards Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fedex_rate_cards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rate_card_name VARCHAR(200) NOT NULL,
                    service_type VARCHAR(50) NOT NULL,
                    origin_region VARCHAR(20),
                    destination_region VARCHAR(20),
                    zone_code VARCHAR(10),
                    weight_from DECIMAL(10,3) NOT NULL,
                    weight_to DECIMAL(10,3) NOT NULL,
                    rate_usd DECIMAL(10,2) NOT NULL,
                    rate_type VARCHAR(20) DEFAULT 'STANDARD',
                    effective_date DATE,
                    expiry_date DATE,
                    created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 5. Service Types Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fedex_service_types (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    service_code VARCHAR(10) UNIQUE NOT NULL,
                    service_name VARCHAR(100) NOT NULL,
                    service_description TEXT,
                    is_express BOOLEAN DEFAULT 1,
                    is_economy BOOLEAN DEFAULT 0,
                    priority_level INTEGER DEFAULT 1,
                    active BOOLEAN DEFAULT 1,
                    created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 6. Surcharges Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fedex_surcharges (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    surcharge_code VARCHAR(20) UNIQUE NOT NULL,
                    surcharge_name VARCHAR(200) NOT NULL,
                    surcharge_description TEXT,
                    rate_type VARCHAR(20) NOT NULL, -- 'FIXED', 'PERCENTAGE', 'PER_KG'
                    rate_value DECIMAL(10,2) NOT NULL,
                    minimum_charge DECIMAL(10,2),
                    maximum_charge DECIMAL(10,2),
                    applies_to_service VARCHAR(100), -- Service types this applies to
                    origin_regions TEXT, -- JSON array of applicable regions
                    destination_regions TEXT, -- JSON array of applicable regions
                    weight_threshold DECIMAL(10,3),
                    active BOOLEAN DEFAULT 1,
                    effective_date DATE,
                    expiry_date DATE,
                    created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # 7. Rate Card Uploads Tracking
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fedex_rate_card_uploads (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    filename VARCHAR(255) NOT NULL,
                    original_filename VARCHAR(255),
                    upload_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                    processed_date DATETIME,
                    status VARCHAR(50) DEFAULT 'uploaded',
                    records_processed INTEGER DEFAULT 0,
                    zones_loaded INTEGER DEFAULT 0,
                    rates_loaded INTEGER DEFAULT 0,
                    surcharges_loaded INTEGER DEFAULT 0,
                    errors_count INTEGER DEFAULT 0,
                    notes TEXT,
                    uploaded_by VARCHAR(100)
                )
            ''')
            
            # 8. Rate Card Versions
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fedex_rate_card_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_name VARCHAR(100) NOT NULL,
                    version_date DATE NOT NULL,
                    description TEXT,
                    is_active BOOLEAN DEFAULT 0,
                    upload_id INTEGER,
                    created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (upload_id) REFERENCES fedex_rate_card_uploads(id)
                )
            ''')
            
            # Create indexes for better performance
            self._create_indexes(cursor)
            
            # Insert default data
            self._insert_default_data(cursor)
            
            conn.commit()
            print("✅ FedEx rate card database schema created successfully")
    
    def _create_indexes(self, cursor):
        """Create indexes for better query performance"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_fedex_zone_matrix_origin ON fedex_zone_matrix(origin_region)",
            "CREATE INDEX IF NOT EXISTS idx_fedex_zone_matrix_dest ON fedex_zone_matrix(destination_region)",
            "CREATE INDEX IF NOT EXISTS idx_fedex_country_zones_country ON fedex_country_zones(country_code)",
            "CREATE INDEX IF NOT EXISTS idx_fedex_country_zones_region ON fedex_country_zones(region_code)",
            "CREATE INDEX IF NOT EXISTS idx_fedex_rate_cards_weight ON fedex_rate_cards(weight_from, weight_to)",
            "CREATE INDEX IF NOT EXISTS idx_fedex_rate_cards_zone ON fedex_rate_cards(zone_code)",
            "CREATE INDEX IF NOT EXISTS idx_fedex_rate_cards_service ON fedex_rate_cards(service_type)",
            "CREATE INDEX IF NOT EXISTS idx_fedex_surcharges_code ON fedex_surcharges(surcharge_code)",
            "CREATE INDEX IF NOT EXISTS idx_fedex_surcharges_active ON fedex_surcharges(active)"
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
    
    def _insert_default_data(self, cursor):
        """Insert default zone regions and service types"""
        
        # Default zone regions based on FedEx analysis
        regions = [
            ('AFRICA', 'Africa', 'African countries'),
            ('ASIA_ONE', 'Asia One', 'Primary Asian markets'),
            ('ASIA_TWO', 'Asia Two', 'Secondary Asian markets'),
            ('ASIA_OTHER', 'Asia Other', 'Other Asian countries'),
            ('CANADA', 'Canada', 'Canadian provinces'),
            ('CARIBBEAN', 'Caribbean', 'Caribbean islands'),
            ('CHINA', 'China', 'Mainland China'),
            ('CENTRAL_AMERICA', 'Central America', 'Central American countries'),
            ('SOUTH_AMERICA', 'South America', 'South American countries'),
            ('EUROPE_ONE', 'Europe One', 'Primary European markets'),
            ('EUROPE_TWO', 'Europe Two', 'Secondary European markets'),
            ('EUROPE_OTHER', 'Europe Other', 'Other European countries'),
            ('EASTERN_EUROPE', 'Eastern Europe', 'Eastern European countries'),
            ('INDIA_SUB', 'India Sub.', 'Indian subcontinent'),
            ('MIDDLE_EAST', 'Middle East', 'Middle Eastern countries'),
            ('MEXICO', 'Mexico', 'Mexico'),
            ('NPAC', 'NPAC', 'North Pacific region'),
            ('US_AK_HI_PR', 'US, AK, HI, PR', 'United States including Alaska, Hawaii, Puerto Rico'),
            ('IQ_AF_SA', 'IQ, AF, SA', 'Iraq, Afghanistan, Saudi Arabia')
        ]
        
        cursor.execute("SELECT COUNT(*) FROM fedex_zone_regions")
        if cursor.fetchone()[0] == 0:
            cursor.executemany('''
                INSERT OR IGNORE INTO fedex_zone_regions (region_code, region_name, description)
                VALUES (?, ?, ?)
            ''', regions)
        
        # Default service types
        services = [
            ('2A', 'International Priority Express', 'Fastest international express service', 1, 0, 1),
            ('2P', 'International Priority', 'Standard international priority service', 1, 0, 2),
            ('2E', 'International Economy Express', 'Economy express service', 1, 1, 3),
            ('FDX', 'FedEx Express', 'General express service', 1, 0, 2),
            ('GND', 'FedEx Ground', 'Ground service', 0, 1, 4)
        ]
        
        cursor.execute("SELECT COUNT(*) FROM fedex_service_types")
        if cursor.fetchone()[0] == 0:
            cursor.executemany('''
                INSERT OR IGNORE INTO fedex_service_types 
                (service_code, service_name, service_description, is_express, is_economy, priority_level)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', services)
        
        # Sample surcharges
        surcharges = [
            ('FUEL', 'Fuel Surcharge', 'Fuel surcharge adjustment', 'PERCENTAGE', 15.50, None, None, 'ALL'),
            ('RAD', 'Remote Area Delivery', 'Remote area delivery fee', 'FIXED', 25.00, 25.00, 100.00, 'EXPRESS'),
            ('RAP', 'Remote Area Pickup', 'Remote area pickup fee', 'FIXED', 25.00, 25.00, 100.00, 'EXPRESS'),
            ('SIG', 'Signature Required', 'Signature required service', 'FIXED', 5.00, 5.00, 5.00, 'ALL'),
            ('SAT', 'Saturday Delivery', 'Saturday delivery service', 'FIXED', 20.00, 20.00, 20.00, 'EXPRESS')
        ]
        
        cursor.execute("SELECT COUNT(*) FROM fedex_surcharges")
        if cursor.fetchone()[0] == 0:
            cursor.executemany('''
                INSERT OR IGNORE INTO fedex_surcharges 
                (surcharge_code, surcharge_name, surcharge_description, rate_type, rate_value, minimum_charge, maximum_charge, applies_to_service)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', surcharges)

def main():
    """Main function to create FedEx rate card schema"""
    schema = FedExRateCardSchema()
    schema.create_all_tables()

if __name__ == '__main__':
    main()
