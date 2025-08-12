import sqlite3

conn = sqlite3.connect('dhl_audit.db')
cursor = conn.cursor()

# Just try to add the column if it doesn't exist
try:
    cursor.execute('ALTER TABLE dhl_express_services_surcharges ADD COLUMN original_service_code VARCHAR(10)')
    print('✅ Added original_service_code column')
except sqlite3.OperationalError as e:
    if 'duplicate column name' in str(e).lower():
        print('✅ original_service_code column already exists')
    else:
        print(f'Column add error: {e}')

conn.commit()
conn.close()
print('Database ready for enhanced converter')
