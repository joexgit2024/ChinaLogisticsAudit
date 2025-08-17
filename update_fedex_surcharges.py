#!/usr/bin/env python3
"""
FedEx Surcharge Data Loader
Convert FedEx surcharge structure to database tables
"""
import sqlite3

def create_fedex_surcharges():
    conn = sqlite3.connect('fedex_audit.db')
    cursor = conn.cursor()
    
    # Clear existing surcharge data
    cursor.execute('DELETE FROM fedex_surcharges')
    
    # FedEx International Surcharges
    fedex_surcharges = [
        # Surcharge Code, Name, Description, Rate Type, Rate Value, Min Charge, Max Charge, Service Types, Active
        ('ADDR_CORR', 'Address Correction', 'Address correction fee', 'FIXED', 12.50, 12.50, 12.50, 'ALL', 1),
        ('ASIA_DROP', 'Asia Dropship - 3rd Party Consignee', 'Third party consignee fee', 'FIXED', 10.00, 10.00, 10.00, 'ALL', 1),
        ('BROKER_SEL', 'Broker Select Option', 'Broker select option fee', 'WEIGHT_OR_FIXED', 1.10, 10.00, None, 'ALL', 1),
        ('CUT_FLOWERS', 'Cut Flowers', 'Cut flowers handling fee', 'FIXED', 20.00, 20.00, 20.00, 'ALL', 1),
        ('DECLARED_VAL', 'Declared Value', 'Declared value insurance', 'VALUE_OR_WEIGHT', 0.0055, 19.95, None, 'ALL', 1),
        ('MISSING_ACCT', 'Missing or Invalid Account Number', 'Missing account number fee', 'FIXED', 10.00, 10.00, 10.00, 'ALL', 1),
        ('OOD_AREA', 'Out-of-Delivery Area', 'Out of delivery area surcharge', 'WEIGHT_OR_FIXED', 0.66, 30.00, None, 'ALL', 1),
        ('OOP_AREA', 'Out-of-Pickup Area', 'Out of pickup area surcharge', 'WEIGHT_OR_FIXED', 0.66, 30.00, None, 'ALL', 1),
        ('SAT_DEL', 'Saturday Delivery', 'Saturday delivery service', 'FIXED', 16.00, 16.00, 16.00, 'ALL', 1),
        ('SAT_PICKUP', 'Saturday Pickup', 'Saturday pickup service', 'FIXED', 16.00, 16.00, 16.00, 'ALL', 1),
        ('DG_INACCESS', 'Inaccessible Dangerous Goods', 'Inaccessible dangerous goods handling', 'WEIGHT_OR_FIXED', 0.74, 62.50, None, 'ALL', 1),
        ('DG_ACCESS', 'Accessible Dangerous Goods', 'Accessible dangerous goods handling', 'WEIGHT_OR_FIXED', 1.52, 127.00, None, 'ALL', 1),
        ('INTL_ONCALL', 'International On Call', 'International on call service', 'VARIABLE', 0, 0, None, 'ALL', 1),
        ('PEAK_SURGE', 'Peak Surcharge', 'Peak season surcharge', 'VARIABLE', 0, 0, None, 'ALL', 1),
        ('FUEL', 'Fuel Surcharge', 'Fuel surcharge based on US FSC index', 'PERCENTAGE', 15.5, None, None, 'ALL', 1)
    ]
    
    # Insert surcharge data
    for surcharge in fedex_surcharges:
        cursor.execute('''
            INSERT INTO fedex_surcharges 
            (surcharge_code, surcharge_name, surcharge_description, rate_type, rate_value, 
             minimum_charge, maximum_charge, applies_to_service, active, created_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ''', surcharge)
    
    print(f"Inserted {len(fedex_surcharges)} FedEx surcharges")
    
    # Create FedEx Service Discount Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fedex_service_discounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            service_code TEXT NOT NULL,
            service_name TEXT NOT NULL,
            weight_range TEXT NOT NULL,
            discount_percentage REAL NOT NULL,
            effective_date TEXT DEFAULT CURRENT_DATE,
            expiry_date TEXT,
            active INTEGER DEFAULT 1,
            created_timestamp TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Clear existing discount data
    cursor.execute('DELETE FROM fedex_service_discounts')
    
    # FedEx Service Discounts (Ratescale 01O - 45% off list rates)
    service_discounts = [
        ('IPE_LTR', 'International Priority Express Letter', 'Letter', 45.0),
        ('IPE_PAK', 'International Priority Express Pak', 'Pak', 45.0),
        ('IPE', 'International Priority Express', '1-70.5kg', 45.0),
        ('IP_LTR', 'International Priority Letter', 'Letter', 45.0),
        ('IP_PAK', 'International Priority Pak', 'Pak', 45.0),
        ('IP', 'International Priority', '1-70.5kg', 45.0),
        ('IPHW', 'International Priority Heavy Weight', '71kg+', 45.0),
        ('IPF', 'International Priority Freight', '71kg+', 45.0),
        ('IE', 'International Economy Express', '1-70.5kg', 45.0),
        ('IEHW', 'International Economy Heavy Weight', '71kg+', 45.0),
        ('IEF', 'International Economy Freight', '71kg+', 45.0)
    ]
    
    for discount in service_discounts:
        cursor.execute('''
            INSERT INTO fedex_service_discounts 
            (service_code, service_name, weight_range, discount_percentage, created_timestamp)
            VALUES (?, ?, ?, ?, datetime('now'))
        ''', discount)
    
    print(f"Inserted {len(service_discounts)} FedEx service discounts")
    
    # Create FedEx Fuel Surcharge Index Table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS fedex_fuel_surcharge_index (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            usgc_price_range_min REAL NOT NULL,
            usgc_price_range_max REAL NOT NULL,
            surcharge_percentage REAL NOT NULL,
            effective_date TEXT DEFAULT CURRENT_DATE,
            created_timestamp TEXT DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Sample fuel surcharge index (this would be updated weekly)
    cursor.execute('DELETE FROM fedex_fuel_surcharge_index')
    fuel_index = [
        (1.50, 1.59, 12.0),
        (1.60, 1.69, 13.0), 
        (1.70, 1.79, 14.0),
        (1.80, 1.89, 15.0),
        (1.90, 1.99, 16.0),
        (2.00, 2.09, 17.0),
        (2.10, 2.19, 18.0),
        (2.20, 2.29, 19.0),
        (2.30, 2.39, 20.0),
        (2.40, 2.49, 21.0)
    ]
    
    for fuel_rate in fuel_index:
        cursor.execute('''
            INSERT INTO fedex_fuel_surcharge_index 
            (usgc_price_range_min, usgc_price_range_max, surcharge_percentage, created_timestamp)
            VALUES (?, ?, ?, datetime('now'))
        ''', fuel_rate)
    
    print(f"Inserted {len(fuel_index)} fuel surcharge index entries")
    
    conn.commit()
    conn.close()
    print("FedEx surcharge data updated successfully!")

if __name__ == "__main__":
    create_fedex_surcharges()
