import sqlite3
conn = sqlite3.connect('dhl_audit.db')
cursor = conn.cursor()

# Update YY service charge to correct amount
cursor.execute('''
    UPDATE dhl_express_services_surcharges 
    SET charge_amount = 160.00 
    WHERE service_code = "YY"
''')

conn.commit()
print('Updated YY (OVERWEIGHT PIECE) charge amount to $160.00')

# Verify the update
cursor.execute('SELECT service_code, service_description, charge_type, charge_amount FROM dhl_express_services_surcharges WHERE service_code = "YY"')
result = cursor.fetchone()
print(f'Updated YY config: {result}')

conn.close()
