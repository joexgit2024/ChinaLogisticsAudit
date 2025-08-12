#!/usr/bin/env python3
import sqlite3

conn = sqlite3.connect('dhl_audit.db')
cursor = conn.cursor()

print('Checking existing audit results for MELR001495168:')
cursor.execute('''
    SELECT * FROM dhl_express_audit_results 
    WHERE invoice_no = 'MELR001495168'
''')

results = cursor.fetchall()
if results:
    print(f'Found {len(results)} existing audit results.')
    print('Deleting existing audit results to force fresh audit...')
    cursor.execute('DELETE FROM dhl_express_audit_results WHERE invoice_no = ?', ('MELR001495168',))
    conn.commit()
    print('Existing audit results deleted.')
else:
    print('No existing audit results found.')

conn.close()
print('Now re-run the audit to get fresh results with Export rates.')
