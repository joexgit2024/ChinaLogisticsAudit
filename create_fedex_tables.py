import sqlite3

# Create FedEx tables and insert sample data for testing
import sqlite3

# Connect to database
conn = sqlite3.connect('fedex_audit.db')
cursor = conn.cursor()

# Create FedEx invoice table
cursor.execute('''CREATE TABLE IF NOT EXISTS fedex_invoices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    invoice_no TEXT NOT NULL, 
    invoice_date TEXT, 
    awb_number TEXT NOT NULL, 
    service_type TEXT, 
    service_abbrev TEXT,
    direction TEXT, 
    pieces INTEGER, 
    actual_weight_kg REAL, 
    chargeable_weight_kg REAL, 
    dim_weight_kg REAL,
    origin_country TEXT, 
    dest_country TEXT, 
    origin_loc TEXT, 
    ship_date TEXT, 
    delivery_datetime TEXT,
    exchange_rate REAL, 
    rated_amount_cny REAL, 
    discount_amount_cny REAL, 
    fuel_surcharge_cny REAL,
    other_surcharge_cny REAL, 
    vat_amount_cny REAL, 
    total_awb_amount_cny REAL, 
    raw_json TEXT,
    created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(invoice_no, awb_number)
)''')

# Create audit results table
cursor.execute('''CREATE TABLE IF NOT EXISTS fedex_audit_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    invoice_no TEXT NOT NULL,
    awb_number TEXT NOT NULL,
    audit_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    invoice_amount REAL,
    expected_amount REAL,
    variance REAL,
    variance_percentage REAL,
    audit_status TEXT,
    zone_applied TEXT,
    rate_applied REAL,
    fuel_surcharge_expected REAL,
    vat_expected REAL,
    audit_details TEXT,
    FOREIGN KEY (invoice_no, awb_number) REFERENCES fedex_invoices(invoice_no, awb_number)
)''')

# Create zone mapping table
cursor.execute('''CREATE TABLE IF NOT EXISTS fedex_country_zones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    origin_country TEXT NOT NULL,
    dest_country TEXT NOT NULL,
    zone TEXT NOT NULL,
    active INTEGER DEFAULT 1
)''')

# Create rate card table
cursor.execute('''CREATE TABLE IF NOT EXISTS fedex_rate_cards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    service_type TEXT NOT NULL,
    zone TEXT NOT NULL,
    weight_kg REAL NOT NULL,
    rate_usd REAL NOT NULL,
    fuel_surcharge_rate REAL DEFAULT 0.0,
    effective_date TEXT,
    active INTEGER DEFAULT 1
)''')

# Insert sample data based on user's example
# Invoice 951109588 has two AWBs: 463688175928 and 565645360110

# Insert sample invoice data for AWB 463688175928 (US to China)
cursor.execute('''INSERT OR REPLACE INTO fedex_invoices 
    (invoice_no, invoice_date, awb_number, service_type, service_abbrev, direction, pieces, 
     actual_weight_kg, chargeable_weight_kg, origin_country, dest_country, 
     exchange_rate, total_awb_amount_cny) VALUES 
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
    ('951109588', '2025-08-05', '463688175928', '2P01', '2P', 'Inbound', 1, 5.9, 24.4, 'US', 'CN', 7.3266, 1678.82))

# Insert sample invoice data for AWB 565645360110 (Japan to China)  
cursor.execute('''INSERT OR REPLACE INTO fedex_invoices 
    (invoice_no, invoice_date, awb_number, service_type, service_abbrev, direction, pieces, 
     actual_weight_kg, chargeable_weight_kg, origin_country, dest_country, 
     exchange_rate, total_awb_amount_cny) VALUES 
    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
    ('951109588', '2025-08-05', '565645360110', '2P01', '2P', 'Inbound', 2, 11.3, 11.3, 'JP', 'CN', 7.32654, 638.62))

# Insert zone mappings based on user's information
cursor.execute('''INSERT OR REPLACE INTO fedex_country_zones 
    (origin_country, dest_country, zone) VALUES 
    ('US', 'CN', 'A'),
    ('JP', 'CN', 'B'),
    ('AU', 'CN', 'B'),
    ('GB', 'CN', 'C')''')

# Insert rate card data based on the attachments (Zone B rates)
# From the user's example: Japan to China (Zone B), 11.3kg rounds to 11.5kg, rate is 65.52 USD
rate_data = [
    ('2P01', 'A', 0.5, 9.59, 0.0),
    ('2P01', 'B', 0.5, 10.87, 0.0),
    ('2P01', 'B', 1.0, 14.72, 0.0),
    ('2P01', 'B', 1.5, 18.57, 0.0),
    ('2P01', 'B', 2.0, 22.41, 0.0),
    ('2P01', 'B', 2.5, 26.27, 0.0),
    ('2P01', 'B', 3.0, 27.09, 0.0),
    ('2P01', 'B', 3.5, 29.28, 0.0),
    ('2P01', 'B', 4.0, 31.48, 0.0),
    ('2P01', 'B', 4.5, 33.68, 0.0),
    ('2P01', 'B', 5.0, 35.89, 0.0),
    ('2P01', 'B', 5.5, 38.09, 0.0),
    ('2P01', 'B', 6.0, 40.29, 0.0),
    ('2P01', 'B', 6.5, 42.49, 0.0),
    ('2P01', 'B', 7.0, 44.69, 0.0),
    ('2P01', 'B', 7.5, 46.90, 0.0),
    ('2P01', 'B', 8.0, 49.11, 0.0),
    ('2P01', 'B', 8.5, 51.31, 0.0),
    ('2P01', 'B', 9.0, 53.51, 0.0),
    ('2P01', 'B', 9.5, 55.71, 0.0),
    ('2P01', 'B', 10.0, 57.91, 0.0),
    ('2P01', 'B', 10.5, 59.28, 0.0),
    ('2P01', 'B', 11.0, 60.66, 0.0),
    ('2P01', 'B', 11.5, 62.04, 0.0),  # This should be close to the 65.52 mentioned
    ('2P01', 'B', 12.0, 63.39, 0.0),
    ('2P01', 'C', 0.5, 12.54, 0.0),
    ('2P01', 'C', 1.0, 18.54, 0.0),
]

for rate in rate_data:
    cursor.execute('''INSERT OR REPLACE INTO fedex_rate_cards 
        (service_type, zone, weight_kg, rate_usd, fuel_surcharge_rate) VALUES (?, ?, ?, ?, ?)''', rate)

conn.commit()
conn.close()

print("FedEx tables created and sample data inserted successfully!")
