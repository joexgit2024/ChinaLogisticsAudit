import sqlite3
import os
from datetime import datetime

DATABASE_NAME = 'dhl_audit.db'

def get_db_connection():
    """Get database connection."""
    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_database():
    """Initialize the database with required tables."""
    conn = sqlite3.connect(DATABASE_NAME)
    
    # Create invoices table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_number TEXT UNIQUE,
            
            -- Client/Account Information
            client_code TEXT,
            carrier_code TEXT,
            account_number TEXT,
            account_period TEXT,
            billed_to_type TEXT,
            
            -- Tracking & Status
            tracking_number TEXT,
            invoice_date TEXT,
            invoice_status TEXT,
            audit_exception_status TEXT,
            
            -- Shipper Information
            shipper_name TEXT,
            shipper_address TEXT,
            shipper_city TEXT,
            shipper_state TEXT,
            shipper_postal_code TEXT,
            shipper_country TEXT,
            
            -- Consignee Information
            consignee_name TEXT,
            consignee_address TEXT,
            consignee_city TEXT,
            consignee_state TEXT,
            consignee_postal_code TEXT,
            consignee_country TEXT,
            
            -- Bill To Information
            bill_to_name TEXT,
            bill_to_address TEXT,
            bill_to_city TEXT,
            bill_to_state TEXT,
            bill_to_postal_code TEXT,
            bill_to_country TEXT,
            
            -- Vessel/Container Information
            vessel_name TEXT,
            container_number TEXT,
            bill_of_lading TEXT,
            booking_number TEXT,
            
            -- Ports and Routing
            origin_port TEXT,
            destination_port TEXT,
            
            -- Dates
            pickup_date TEXT,
            delivery_date TEXT,
            service_date TEXT,
            ship_date TEXT,
            shipment_entered_date TEXT,
            invoice_created_date TEXT,
            
            -- Reference Numbers
            reference_number TEXT,
            pro_number TEXT,
            
            -- Financial Information
            total_charges REAL DEFAULT 0.0,
            net_charge REAL DEFAULT 0.0,
            invoice_amount REAL DEFAULT 0.0,
            check_number TEXT,
            check_date TEXT,
            
            -- Weight and Measurements
            weight REAL DEFAULT 0.0,
            bill_weight REAL DEFAULT 0.0,
            ship_weight REAL DEFAULT 0.0,
            pieces INTEGER DEFAULT 0,
            volume REAL DEFAULT 0.0,
            declared_value REAL DEFAULT 0.0,
            
            -- Currency and Exchange
            currency TEXT DEFAULT 'USD',
            exchange_rate REAL DEFAULT 1.0,
            from_currency TEXT,
            to_currency TEXT,
            
            -- Service Information
            shipping_mode TEXT,
            service_type TEXT,
            delivery_commitment TEXT,
            commodity_type TEXT,
            
            -- Business Information
            vendor_number TEXT,
            customer_vat_registration TEXT,
            sap_plant TEXT,
            shipper_company_code TEXT,
            mode TEXT,
            allocation_percentage REAL DEFAULT 100.0,
            master_shipper_address TEXT,
            company_code TEXT,
            shipper_description TEXT,
            gl_account TEXT,
            carrier_name TEXT,
            direction TEXT,
            charge_group TEXT,
            recipient_description TEXT,
            partner_bank_type TEXT,
            profit_center TEXT,
            carrier_vat_registration TEXT,
            recipient_type TEXT,
            carrier_country TEXT,
            shipper_plant TEXT,
            tax_code TEXT,
            
            -- Audit Information
            audit_status TEXT DEFAULT 'pending',
            audit_notes TEXT,
            
            -- System Fields
            raw_edi TEXT,
            uploaded_file_path TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create charges table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS charges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER,
            charge_type TEXT,
            amount NUMERIC DEFAULT 0.00,
            description TEXT,
            rate NUMERIC DEFAULT NULL,
            quantity NUMERIC DEFAULT NULL,
            unit TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (invoice_id) REFERENCES invoices (id)
        )
    ''')
    
    # Create shipments table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS shipments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER,
            tracking_number TEXT,
            origin_location TEXT,
            destination_location TEXT,
            pickup_date TEXT,
            delivery_date TEXT,
            service_type TEXT,
            weight REAL,
            dimensions TEXT,
            package_count INTEGER,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (invoice_id) REFERENCES invoices (id)
        )
    ''')
    
    # Create audit_rules table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS audit_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_name TEXT,
            rule_type TEXT,
            condition_field TEXT,
            operator TEXT,
            threshold_value REAL,
            action TEXT,
            is_active BOOLEAN DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create reference_numbers table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS reference_numbers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER,
            reference_type TEXT,
            reference_value TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (invoice_id) REFERENCES invoices (id)
        )
    ''')

    # Create line_items table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS line_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER,
            line_number INTEGER,
            item_description TEXT,
            quantity REAL,
            weight REAL,
            volume REAL,
            dimensions TEXT,
            unit_type TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (invoice_id) REFERENCES invoices (id)
        )
    ''')

    # Create charge_mapping table
    conn.execute('''
        CREATE TABLE IF NOT EXISTS charge_mapping (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            charge_code TEXT UNIQUE,
            charge_description TEXT,
            charge_type INTEGER
        )
    ''')

    # Create charge_codes table for detailed charge code tracking
    conn.execute('''
        CREATE TABLE IF NOT EXISTS charge_codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_id INTEGER,
            carrier_charge_code TEXT,
            account_charge_code TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (invoice_id) REFERENCES invoices (id)
        )
    ''')

    # Insert default audit rules
    default_rules = [
        ('High Value Charge', 'amount_check', 'total_charges', '>', 1000.0, 'flag_for_review'),
        ('Missing Shipper', 'data_validation', 'shipper_name', 'is_null', None, 'flag_for_review'),
        ('Missing Consignee', 'data_validation', 'consignee_name', 'is_null', None, 'flag_for_review'),
        ('Zero Weight', 'data_validation', 'weight', '=', 0.0, 'flag_for_review'),
        ('Excessive Fuel Surcharge', 'percentage_check', 'fuel_surcharge', '>', 25.0, 'flag_for_review')
    ]
    
    for rule in default_rules:
        conn.execute('''
            INSERT OR IGNORE INTO audit_rules 
            (rule_name, rule_type, condition_field, operator, threshold_value, action)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', rule)
    
    # Populate charge_mapping table
    mappings = [
        ('THC', 'TERMINAL HANDLING CHARGE (DEST)', 1),
        ('COF', 'OCEAN FREIGHT CHG', 2),
        ('360', 'CUSTOMS CLEARANCE', 3),
        ('OHC', 'TERMINAL HANDLING CHARGE (ORIGIN)', 4),
        ('PUC', 'PICK-UP CHARGE', 5),
        ('540', 'PICK-UP SURCHARGE (AFTER HOURS)', 6),
        ('DEL', 'DELIVERY CHARGE', 7),
        ('BSC', 'BUNKER SURCHARGE', 8),
        ('175', 'CERTIFICATE OF REGISTRATION', 9),
        ('750', 'VALUE ADDED TAX (VAT)', 10),
        ('400', 'AIR FREIGHT', 11),
        ('CHC', 'HANDLING CHARGE', 12),
        ('TPS', 'THIRD PARTY SERVICE (BLIND SHIPMENT)', 13)
    ]

    for mapping in mappings:
        conn.execute('''
            INSERT OR IGNORE INTO charge_mapping (charge_code, charge_description, charge_type)
            VALUES (?, ?, ?)
        ''', mapping)
    conn.commit()
    
    conn.close()
    print("Database initialized successfully!")

def execute_query(query, params=None):
    """Execute a query and return results."""
    conn = get_db_connection()
    try:
        if params:
            result = conn.execute(query, params)
        else:
            result = conn.execute(query)
        
        if query.strip().upper().startswith('SELECT'):
            return result.fetchall()
        else:
            conn.commit()
            return result.lastrowid
    finally:
        conn.close()

def get_invoice_summary():
    """Get summary statistics for invoices."""
    conn = get_db_connection()
    try:
        stats = {}
        
        # Total invoices
        stats['total_invoices'] = conn.execute('SELECT COUNT(*) FROM invoices').fetchone()[0]
        
        # Total charges
        total_charges = conn.execute('SELECT SUM(total_charges) FROM invoices').fetchone()[0]
        stats['total_charges'] = total_charges if total_charges else 0.0
        
        # Audit status breakdown
        audit_status = conn.execute('''
            SELECT audit_status, COUNT(*) as count 
            FROM invoices 
            GROUP BY audit_status
        ''').fetchall()
        stats['audit_status'] = dict(audit_status)
        
        # Recent activity
        stats['recent_invoices'] = conn.execute('''
            SELECT COUNT(*) FROM invoices 
            WHERE created_at >= date('now', '-7 days')
        ''').fetchone()[0]
        
        return stats
    finally:
        conn.close()

if __name__ == "__main__":
    init_database()
    print("Database initialized successfully!")
