#!/usr/bin/env python3
"""
DHL Express China Invoice Database Schema
=======================================

Creates database schema for Chinese DHL Express invoices
Based on the 68-column structure from DHL Bill.xlsx
"""
import sqlite3
from datetime import datetime

def create_china_invoice_schema():
    """Create database schema for Chinese DHL Express invoices"""
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    print("🏗️ Creating Chinese DHL Express invoice schema...")
    
    # Drop existing table if it exists
    cursor.execute('DROP TABLE IF EXISTS dhl_express_china_invoices')
    
    # Create new Chinese invoice table with all 68 columns
    cursor.execute('''
        CREATE TABLE dhl_express_china_invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            
            -- Basic billing information
            source_country TEXT,
            billing_period TEXT,
            service_type TEXT,
            billing_term TEXT,
            billing_type TEXT,
            transaction_status TEXT,
            ib434_line_id INTEGER,
            omit_code TEXT,
            offset_code TEXT,
            transaction_status_description TEXT,
            
            -- Shipment details
            shipment_date DATE,
            load_date DATE,
            air_waybill TEXT NOT NULL,
            unique_record_id TEXT,
            shipper_account TEXT,
            bill_to_account TEXT,
            bill_to_account_name TEXT,
            shipper_reference TEXT,
            local_product_code TEXT,
            origin_code TEXT,
            dest_code TEXT,
            
            -- Invoice details
            billing_currency TEXT,
            invoice_number TEXT NOT NULL,
            invoice_date DATE,
            original_invoice TEXT,
            original_invoice_date DATE,
            modify_code TEXT,
            modify_description TEXT,
            
            -- Weight and pieces
            pieces INTEGER,
            customer_weight DECIMAL(10,3),
            dhl_weight DECIMAL(10,3),
            customer_vol_weight DECIMAL(10,3),
            dhl_vol_weight DECIMAL(10,3),
            weight_code TEXT,
            billed_weight_kg DECIMAL(10,3),
            billed_weight_lbs DECIMAL(10,3),
            
            -- Billing Currency Unit (BCU) charges
            bcu_weight_charge DECIMAL(12,2),
            bcu_fuel_surcharges DECIMAL(12,2),
            bcu_other_charges DECIMAL(12,2),
            bcu_discount DECIMAL(12,2),
            bcu_duties_taxes DECIMAL(12,2),
            bcu_taxes_applicable DECIMAL(12,2),
            bcu_total DECIMAL(12,2),
            
            -- Local Currency Unit (LCU/CNY) charges
            exchange_rate DECIMAL(10,6),
            local_currency TEXT,
            lcu_weight_charge DECIMAL(12,2),
            lcu_fuel_surcharges DECIMAL(12,2),
            lcu_other_charges DECIMAL(12,2),
            lcu_discount DECIMAL(12,2),
            lcu_duties_taxes DECIMAL(12,2),
            lcu_taxes_applicable DECIMAL(12,2),
            lcu_total DECIMAL(12,2),
            
            -- Consignor (Shipper) details
            consignor_name TEXT,
            consignor_contact_name TEXT,
            consignor_address_1 TEXT,
            consignor_address_2 TEXT,
            consignor_city TEXT,
            consignor_province_state TEXT,
            consignor_country TEXT,
            consignor_postal_code TEXT,
            
            -- Consignee (Receiver) details
            consignee_name TEXT,
            consignee_contact_name TEXT,
            consignee_address_1 TEXT,
            consignee_address_2 TEXT,
            consignee_city TEXT,
            consignee_province_state TEXT,
            consignee_country TEXT,
            consignee_postal_code TEXT,
            
            -- Audit fields
            audit_status TEXT,
            expected_cost_cny DECIMAL(12,2),
            variance_cny DECIMAL(12,2),
            audit_timestamp DATETIME,
            audit_details TEXT,
            
            -- System fields
            created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            
            -- Indexes
            UNIQUE(invoice_number, air_waybill)
        )
    ''')
    
    # Create indexes for better performance
    indexes = [
        'CREATE INDEX idx_china_invoice_number ON dhl_express_china_invoices(invoice_number)',
        'CREATE INDEX idx_china_air_waybill ON dhl_express_china_invoices(air_waybill)',
        'CREATE INDEX idx_china_shipment_date ON dhl_express_china_invoices(shipment_date)',
        'CREATE INDEX idx_china_invoice_date ON dhl_express_china_invoices(invoice_date)',
        'CREATE INDEX idx_china_service_type ON dhl_express_china_invoices(service_type)',
        'CREATE INDEX idx_china_origin_dest ON dhl_express_china_invoices(origin_code, dest_code)',
        'CREATE INDEX idx_china_audit_status ON dhl_express_china_invoices(audit_status)',
        'CREATE INDEX idx_china_bill_to_account ON dhl_express_china_invoices(bill_to_account)'
    ]
    
    for index_sql in indexes:
        cursor.execute(index_sql)
    
    # Create audit results table for Chinese invoices
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dhl_express_china_audit_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER,
            air_waybill TEXT,
            invoice_number TEXT,
            expected_cost_cny DECIMAL(12,2),
            actual_cost_cny DECIMAL(12,2),
            variance_cny DECIMAL(12,2),
            variance_percent DECIMAL(8,4),
            audit_status TEXT,
            rate_card_match TEXT,
            zone_used TEXT,
            weight_used DECIMAL(10,3),
            service_type TEXT,
            audit_details TEXT,
            created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (invoice_id) REFERENCES dhl_express_china_invoices(id)
        )
    ''')
    
    # Create upload tracking table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dhl_express_china_uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            original_filename TEXT,
            upload_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            processed_date DATETIME,
            status TEXT DEFAULT 'uploaded',
            records_processed INTEGER DEFAULT 0,
            records_loaded INTEGER DEFAULT 0,
            errors_count INTEGER DEFAULT 0,
            notes TEXT,
            uploaded_by TEXT
        )
    ''')
    
    conn.commit()
    conn.close()
    
    print("✅ Chinese DHL Express invoice schema created successfully!")
    print("   📊 Tables created:")
    print("      - dhl_express_china_invoices (main invoice data)")
    print("      - dhl_express_china_audit_results (audit results)")
    print("      - dhl_express_china_uploads (upload tracking)")

if __name__ == '__main__':
    create_china_invoice_schema()
