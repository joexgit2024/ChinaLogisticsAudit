#!/usr/bin/env python3
"""Verify Chinese DHL invoice loading"""
import sqlite3

conn = sqlite3.connect('dhl_audit.db')
cursor = conn.cursor()

# Check total count
cursor.execute('SELECT COUNT(*) FROM dhl_express_china_invoices')
count = cursor.fetchone()[0]
print(f'Chinese invoices loaded: {count}')

# Check sample records
cursor.execute('''
    SELECT air_waybill, invoice_number, bcu_total, lcu_total, 
           consignor_name, consignee_name
    FROM dhl_express_china_invoices 
    LIMIT 5
''')

print('\nSample records:')
for row in cursor.fetchall():
    print(f'  AWB: {row[0]}, Invoice: {row[1]}, BCU Total: {row[2]}, LCU Total: {row[3]}')
    print(f'    From: {row[4]} -> To: {row[5]}')

# Check upload history
cursor.execute('SELECT * FROM dhl_express_china_uploads ORDER BY upload_date DESC LIMIT 1')
upload_info = cursor.fetchone()
if upload_info:
    print(f'\nLast upload: {upload_info[2]} - {upload_info[5]} records loaded')

conn.close()
