#!/usr/bin/env python3
"""
Create Australia DHL Express Domestic rate card database tables
"""

import sqlite3
import os

def create_au_domestic_tables():
    """Create database tables for AU domestic rate cards"""
    print("🏗️ CREATING AU DOMESTIC RATE CARD TABLES")
    print("=" * 50)
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # 1. AU Domestic Zones table (City/State to Zone mapping)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dhl_express_au_domestic_zones (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            zone_number INTEGER NOT NULL,
            city_code VARCHAR(10),
            city_name VARCHAR(100),
            state_code VARCHAR(10),
            service_area VARCHAR(200),
            created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(zone_number, city_code, city_name)
        )
    ''')
    
    # 2. AU Domestic Zone Matrix (Zone to Zone mapping)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dhl_express_au_domestic_matrix (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            origin_zone INTEGER NOT NULL,
            destination_zone INTEGER NOT NULL,
            rate_zone VARCHAR(10) NOT NULL,
            created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(origin_zone, destination_zone)
        )
    ''')
    
    # 3. AU Domestic Rates (Weight and Zone-based rates)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dhl_express_au_domestic_rates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            weight_kg DECIMAL(10,3) NOT NULL,
            zone_a DECIMAL(10,2),
            zone_b DECIMAL(10,2),
            zone_c DECIMAL(10,2),
            zone_d DECIMAL(10,2),
            zone_e DECIMAL(10,2),
            zone_f DECIMAL(10,2),
            zone_g DECIMAL(10,2),
            zone_h DECIMAL(10,2),
            above_25kg_multiplier DECIMAL(10,4),
            effective_date DATE,
            created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(weight_kg, effective_date)
        )
    ''')
    
    # 4. AU Domestic Rate Card Uploads (Track uploads)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dhl_express_au_domestic_uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename VARCHAR(255) NOT NULL,
            upload_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            processed_date DATETIME,
            status VARCHAR(50) DEFAULT 'uploaded',
            zones_loaded INTEGER DEFAULT 0,
            matrix_loaded INTEGER DEFAULT 0,
            rates_loaded INTEGER DEFAULT 0,
            notes TEXT,
            uploaded_by VARCHAR(100)
        )
    ''')
    
    # Create indexes for performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_au_zones_zone_number ON dhl_express_au_domestic_zones(zone_number)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_au_zones_city_code ON dhl_express_au_domestic_zones(city_code)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_au_matrix_zones ON dhl_express_au_domestic_matrix(origin_zone, destination_zone)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_au_rates_weight ON dhl_express_au_domestic_rates(weight_kg)')
    
    conn.commit()
    conn.close()
    
    print("✅ Database tables created successfully:")
    print("   - dhl_express_au_domestic_zones")
    print("   - dhl_express_au_domestic_matrix") 
    print("   - dhl_express_au_domestic_rates")
    print("   - dhl_express_au_domestic_uploads")

if __name__ == "__main__":
    create_au_domestic_tables()
