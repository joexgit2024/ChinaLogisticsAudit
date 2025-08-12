#!/usr/bin/env python3

import sqlite3

def examine_database():
    print('🔍 EXAMINING EXISTING DATABASE STRUCTURE')
    print('=' * 50)
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Check dhl_express_invoices table structure
    cursor.execute('PRAGMA table_info(dhl_express_invoices)')
    dhl_columns = cursor.fetchall()
    print('DHL Express Invoices table:')
    for col in dhl_columns:
        print(f'  - {col[1]} ({col[2]})')
    
    print()
    
    # Check if invoices table exists (DGF invoices)
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='invoices'")
    invoices_exists = cursor.fetchone()
    if invoices_exists:
        cursor.execute('PRAGMA table_info(invoices)')
        dgf_columns = cursor.fetchall()
        print('DGF Invoices table:')
        for col in dgf_columns:
            print(f'  - {col[1]} ({col[2]})')
    else:
        print('DGF Invoices table: NOT FOUND')
    
    print()
    
    # Check all tables in database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    all_tables = cursor.fetchall()
    print('All tables in database:')
    for table in all_tables:
        print(f'  - {table[0]}')
    
    conn.close()

if __name__ == "__main__":
    examine_database()
