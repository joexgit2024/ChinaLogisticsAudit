import sqlite3

conn = sqlite3.connect('fedex_audit.db')
cursor = conn.cursor()

# Create zone mapping table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS fedex_zone_mappings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    origin_country TEXT NOT NULL,
    destination_country TEXT NOT NULL,
    zone TEXT NOT NULL,
    service_type TEXT DEFAULT '2P01',
    UNIQUE(origin_country, destination_country, service_type)
)
''')

# Insert zone mappings - Japan to China should be Zone B
zone_mappings = [
    ('JP', 'CN', 'B', '2P01'),  # Japan to China = Zone B
    ('US', 'CN', 'A', '2P01'),  # US to China = Zone A (example)
    ('KR', 'CN', 'B', '2P01'),  # Korea to China = Zone B (example)
    ('SG', 'CN', 'B', '2P01'),  # Singapore to China = Zone B (example)
]

for mapping in zone_mappings:
    try:
        cursor.execute('INSERT INTO fedex_zone_mappings (origin_country, destination_country, zone, service_type) VALUES (?, ?, ?, ?)', mapping)
        print(f"Inserted: {mapping[0]} -> {mapping[1]} = Zone {mapping[2]}")
    except sqlite3.IntegrityError:
        print(f"Mapping already exists: {mapping[0]} -> {mapping[1]} = Zone {mapping[2]}")

conn.commit()

# Verify the mapping
cursor.execute('SELECT * FROM fedex_zone_mappings WHERE origin_country = "JP" AND destination_country = "CN"')
mapping = cursor.fetchone()
print(f"\nJapan to China mapping: {mapping}")

conn.close()
