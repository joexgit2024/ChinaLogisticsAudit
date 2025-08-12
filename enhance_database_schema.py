#!/usr/bin/env python3
"""
Schema Enhancement Script for DHL Invoice Audit System
Based on CSV analysis, adds new fields and tables for comprehensive invoice data capture
"""

import sqlite3
import os
from datetime import datetime

def backup_database():
    """Create a backup of the current database"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"dhl_audit_backup_{timestamp}.db"
    
    print(f"Creating backup: {backup_name}")
    os.system(f"copy dhl_audit.db {backup_name}")
    return backup_name

def enhance_llm_invoice_summary():
    """Add new fields to llm_invoice_summary table"""
    print("\n=== ENHANCING llm_invoice_summary TABLE ===")
    
    enhancements = [
        "ALTER TABLE llm_invoice_summary ADD COLUMN account_number VARCHAR(50)",
        "ALTER TABLE llm_invoice_summary ADD COLUMN payment_terms VARCHAR(20)",
        "ALTER TABLE llm_invoice_summary ADD COLUMN incoterms VARCHAR(10)", 
        "ALTER TABLE llm_invoice_summary ADD COLUMN transportation_mode VARCHAR(20)",
        "ALTER TABLE llm_invoice_summary ADD COLUMN masterbill VARCHAR(50)",
        "ALTER TABLE llm_invoice_summary ADD COLUMN housebill VARCHAR(50)",
        "ALTER TABLE llm_invoice_summary ADD COLUMN awb_number VARCHAR(50)",
        "ALTER TABLE llm_invoice_summary ADD COLUMN shipment_date DATE",
        "ALTER TABLE llm_invoice_summary ADD COLUMN total_pieces INTEGER",
        "ALTER TABLE llm_invoice_summary ADD COLUMN chargeable_weight DECIMAL(10,2)",
        "ALTER TABLE llm_invoice_summary ADD COLUMN volume_weight DECIMAL(10,2)",
        "ALTER TABLE llm_invoice_summary ADD COLUMN exchange_rate_eur DECIMAL(10,6)",
        "ALTER TABLE llm_invoice_summary ADD COLUMN exchange_rate_usd DECIMAL(10,6)",
        "ALTER TABLE llm_invoice_summary ADD COLUMN shipper_name VARCHAR(255)",
        "ALTER TABLE llm_invoice_summary ADD COLUMN shipper_address TEXT",
        "ALTER TABLE llm_invoice_summary ADD COLUMN consignee_name VARCHAR(255)",
        "ALTER TABLE llm_invoice_summary ADD COLUMN consignee_address TEXT",
        "ALTER TABLE llm_invoice_summary ADD COLUMN commodity_description TEXT"
    ]
    
    return enhancements

def enhance_llm_billing_line_items():
    """Add new fields to llm_billing_line_items table"""
    print("\n=== ENHANCING llm_billing_line_items TABLE ===")
    
    enhancements = [
        "ALTER TABLE llm_billing_line_items ADD COLUMN charge_type VARCHAR(50)",
        "ALTER TABLE llm_billing_line_items ADD COLUMN base_amount DECIMAL(15,2)",
        "ALTER TABLE llm_billing_line_items ADD COLUMN surcharge_amount DECIMAL(15,2)",
        "ALTER TABLE llm_billing_line_items ADD COLUMN discount_amount DECIMAL(15,2)",
        "ALTER TABLE llm_billing_line_items ADD COLUMN discount_code VARCHAR(20)",
        "ALTER TABLE llm_billing_line_items ADD COLUMN tax_code VARCHAR(10)",
        "ALTER TABLE llm_billing_line_items ADD COLUMN pal_col INTEGER",
        "ALTER TABLE llm_billing_line_items ADD COLUMN weight_charge DECIMAL(15,2)"
    ]
    
    return enhancements

def create_new_tables():
    """Create new supporting tables for enhanced data capture"""
    print("\n=== CREATING NEW SUPPORTING TABLES ===")
    
    tables = {
        "llm_shipment_routing": """
            CREATE TABLE IF NOT EXISTS llm_shipment_routing (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_no VARCHAR(50) NOT NULL,
                origin_port_code VARCHAR(10),
                origin_port_name VARCHAR(100),
                destination_port_code VARCHAR(10),
                destination_port_name VARCHAR(100),
                port_of_loading VARCHAR(100),
                port_of_discharge VARCHAR(100),
                routing_details TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (invoice_no) REFERENCES llm_pdf_extractions(invoice_no)
            )
        """,
        
        "llm_container_details": """
            CREATE TABLE IF NOT EXISTS llm_container_details (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_no VARCHAR(50) NOT NULL,
                container_number VARCHAR(50),
                container_type VARCHAR(20),
                num_teus INTEGER,
                num_20ft INTEGER,
                num_40ft INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (invoice_no) REFERENCES llm_pdf_extractions(invoice_no)
            )
        """,
        
        "llm_charge_categories": """
            CREATE TABLE IF NOT EXISTS llm_charge_categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_code VARCHAR(50) UNIQUE NOT NULL,
                category_name VARCHAR(100) NOT NULL,
                charge_type VARCHAR(50), -- FREIGHT, SURCHARGE, SERVICE, TAX, OTHER
                description TEXT,
                is_active BOOLEAN DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
    }
    
    return tables

def populate_charge_categories():
    """Populate the charge categories table with enhanced categories"""
    print("\n=== POPULATING CHARGE CATEGORIES ===")
    
    categories = [
        ("PICKUP_CHARGES", "Pickup Charges", "SERVICE", "Collection of shipment from origin"),
        ("ORIGIN_HANDLING", "Origin Handling", "SERVICE", "Handling charges at origin terminal"),
        ("ORIGIN_CUSTOMS", "Origin Customs", "SERVICE", "Customs processing at origin"),
        ("FREIGHT_CHARGES", "Freight Charges", "FREIGHT", "Main transportation charges"),
        ("FUEL_SURCHARGE", "Fuel Surcharge", "SURCHARGE", "Fuel price adjustment surcharge"),
        ("SECURITY_SURCHARGE", "Security Surcharge", "SURCHARGE", "Aviation security surcharge"),
        ("DESTINATION_CUSTOMS", "Destination Customs", "SERVICE", "Customs processing at destination"),
        ("DESTINATION_HANDLING", "Destination Handling", "SERVICE", "Handling charges at destination terminal"),
        ("DELIVERY_CHARGES", "Delivery Charges", "SERVICE", "Final delivery to consignee"),
        ("REMOTE_AREA_PICKUP", "Remote Area Pickup", "SURCHARGE", "Additional charge for remote pickup areas"),
        ("REMOTE_AREA_DELIVERY", "Remote Area Delivery", "SURCHARGE", "Additional charge for remote delivery areas"),
        ("OVERWEIGHT_PIECE", "Overweight Piece", "SURCHARGE", "Surcharge for pieces exceeding weight limits"),
        ("NON_CONVEYABLE_PIECE", "Non-Conveyable Piece", "SURCHARGE", "Surcharge for irregular shaped pieces"),
        ("CHANGE_OF_BILLING", "Change of Billing", "SERVICE", "Change of billing arrangement"),
        ("DIRECT_SIGNATURE", "Direct Signature", "SERVICE", "Signature required service"),
        ("PREMIUM_SERVICE", "Premium Service", "SERVICE", "Time-definite premium services"),
        ("BONDED_STORAGE", "Bonded Storage", "SERVICE", "Customs bonded storage charges"),
        ("GOGREEN_PLUS", "GoGreen Plus", "SERVICE", "Carbon-neutral shipping service"),
        ("DUTIES_TAXES", "Duties & Taxes", "TAX", "Import duties and taxes"),
        ("OTHER_CHARGES", "Other Charges", "OTHER", "Miscellaneous charges")
    ]
    
    insert_query = """
        INSERT OR IGNORE INTO llm_charge_categories 
        (category_code, category_name, charge_type, description) 
        VALUES (?, ?, ?, ?)
    """
    
    return categories, insert_query

def execute_schema_enhancements():
    """Execute all schema enhancements"""
    
    # Create backup first
    backup_file = backup_database()
    
    try:
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        print("Starting schema enhancements...")
        
        # 1. Enhance existing tables
        print("\n1. Enhancing llm_invoice_summary...")
        for sql in enhance_llm_invoice_summary():
            try:
                cursor.execute(sql)
                field_name = sql.split()[-2]  # Extract field name
                print(f"  ✅ Added field: {field_name}")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e):
                    field_name = sql.split()[-2]
                    print(f"  ⚠️  Field already exists: {field_name}")
                else:
                    print(f"  ❌ Error adding field: {e}")
        
        print("\n2. Enhancing llm_billing_line_items...")
        for sql in enhance_llm_billing_line_items():
            try:
                cursor.execute(sql)
                field_name = sql.split()[-2]
                print(f"  ✅ Added field: {field_name}")
            except sqlite3.OperationalError as e:
                if "duplicate column name" in str(e):
                    field_name = sql.split()[-2]
                    print(f"  ⚠️  Field already exists: {field_name}")
                else:
                    print(f"  ❌ Error adding field: {e}")
        
        # 2. Create new tables
        print("\n3. Creating new supporting tables...")
        tables = create_new_tables()
        for table_name, create_sql in tables.items():
            try:
                cursor.execute(create_sql)
                print(f"  ✅ Created table: {table_name}")
            except sqlite3.OperationalError as e:
                print(f"  ❌ Error creating {table_name}: {e}")
        
        # 3. Populate charge categories
        print("\n4. Populating charge categories...")
        categories, insert_query = populate_charge_categories()
        for category in categories:
            try:
                cursor.execute(insert_query, category)
                print(f"  ✅ Added category: {category[0]}")
            except sqlite3.Error as e:
                print(f"  ❌ Error adding {category[0]}: {e}")
        
        # Commit all changes
        conn.commit()
        print("\n✅ All schema enhancements completed successfully!")
        
        # Show updated schema
        print("\n=== UPDATED SCHEMA SUMMARY ===")
        cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name LIKE 'llm_%'")
        tables = cursor.fetchall()
        for table in tables:
            print(f"\nTable creation SQL:\n{table[0]}")
        
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Critical error during schema enhancement: {e}")
        print(f"Database backup available at: {backup_file}")
        return False

def generate_updated_prompts():
    """Generate updated LLM prompts with new fields"""
    print("\n=== GENERATING UPDATED LLM PROMPTS ===")
    
    updated_schema_prompt = """
You are an expert DHL invoice data extraction assistant. Extract the following information from DHL invoices and format as JSON:

INVOICE SUMMARY:
- invoice_no: Invoice number
- invoice_date: Invoice date  
- due_date: Payment due date
- customer_name: Customer/company name
- account_number: DHL account number
- payment_terms: Payment terms (Collect, Prepaid, etc.)
- incoterms: Incoterms code (FCA, EXW, DAP, etc.)
- transportation_mode: Air, Sea, Road, etc.
- masterbill: Master bill/AWB number
- housebill: House bill number
- awb_number: Air waybill number
- shipment_date: Shipment date
- currency: Invoice currency
- subtotal: Subtotal amount
- gst_total: GST/VAT amount
- final_total: Final total amount
- service_type: DHL service type
- origin: Origin location
- destination: Destination location
- weight: Total weight
- total_pieces: Number of pieces
- chargeable_weight: Chargeable weight
- volume_weight: Volumetric weight
- exchange_rate_eur: EUR exchange rate
- exchange_rate_usd: USD exchange rate
- shipper_name: Shipper company name
- shipper_address: Shipper full address
- consignee_name: Consignee company name  
- consignee_address: Consignee full address
- commodity_description: Description of goods
- shipment_ref: Shipment reference

BILLING LINE ITEMS (array):
- line_item_index: Line number
- description: Service/charge description
- charge_type: Categorized charge type
- amount: Line amount (before GST)
- base_amount: Base charge amount
- surcharge_amount: Surcharge amount
- discount_amount: Discount amount
- discount_code: Discount code
- gst_amount: GST/tax amount
- total_amount: Line total
- tax_code: Tax code
- pal_col: Package/container number
- weight_charge: Weight-based charge
- currency: Line currency
- category: Charge category code

ROUTING DETAILS:
- origin_port_code: Origin port/airport code
- origin_port_name: Origin port/airport name
- destination_port_code: Destination port/airport code
- destination_port_name: Destination port/airport name
- port_of_loading: Port of loading
- port_of_discharge: Port of discharge

CONTAINER DETAILS (if applicable):
- container_number: Container number
- container_type: FCL/LCL type
- num_teus: Number of TEUs
- num_20ft: Number of 20ft containers
- num_40ft: Number of 40ft containers

Return structured JSON matching this schema exactly.
"""
    
    with open("updated_llm_prompt_schema.txt", "w") as f:
        f.write(updated_schema_prompt)
    
    print("✅ Updated LLM prompt saved to updated_llm_prompt_schema.txt")
    
    return updated_schema_prompt

if __name__ == "__main__":
    print("DHL Invoice Audit System - Schema Enhancement")
    print("=" * 50)
    
    # Execute enhancements
    success = execute_schema_enhancements()
    
    if success:
        # Generate updated prompts
        generate_updated_prompts()
        
        print("\n🎉 SCHEMA ENHANCEMENT COMPLETE!")
        print("\nNEXT STEPS:")
        print("1. Update schema_driven_llm_processor.py with new field mappings")
        print("2. Test LLM extraction with sample invoices")
        print("3. Update web interface to display new fields")
        print("4. Validate data capture accuracy")
        
    else:
        print("\n❌ Schema enhancement failed. Check logs and restore from backup if needed.")
