import sqlite3

conn = sqlite3.connect('dhl_audit.db')
cursor = conn.cursor()

# Find YTD tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%ytd%'")
ytd_tables = cursor.fetchall()
print("YTD tables:", [t[0] for t in ytd_tables])

# Check invoice-related tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%invoice%'")
invoice_tables = cursor.fetchall()
print("Invoice tables:", [t[0] for t in invoice_tables])

conn.close()
