#!/usr/bin/env python3
"""
Clear invoice table and all related data for China logistics setup
"""
import sqlite3

def clear_invoice_data():
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    print("Clearing invoice-related data for China logistics setup...")
    print("=" * 60)
    
    # Enable foreign key constraints
    cursor.execute("PRAGMA foreign_keys = ON;")
    
    # Step 1: Delete from child tables first (in order)
    child_tables = [
        'charges',           # 1 record
        'shipments',         # 0 records  
        'reference_numbers', # 0 records
        'line_items',        # 20 records
        'charge_codes',      # 0 records
        'audit_results'      # 0 records
    ]
    
    for table in child_tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            count_before = cursor.fetchone()[0]
            
            cursor.execute(f"DELETE FROM {table};")
            rows_deleted = cursor.rowcount
            
            print(f"✓ Deleted {rows_deleted} records from {table} (had {count_before} records)")
            
        except Exception as e:
            print(f"✗ Error deleting from {table}: {e}")
    
    # Step 2: Now delete from invoices table
    try:
        cursor.execute("SELECT COUNT(*) FROM invoices;")
        count_before = cursor.fetchone()[0]
        
        cursor.execute("DELETE FROM invoices;")
        rows_deleted = cursor.rowcount
        
        print(f"✓ Deleted {rows_deleted} records from invoices (had {count_before} records)")
        
    except Exception as e:
        print(f"✗ Error deleting from invoices: {e}")
    
    # Step 3: Also clear related audit and processing tables (optional)
    optional_tables = [
        'detailed_audit_results',      # 9 records
        'invoice_pdf_details_enhanced', # 10 records
        'unknown_charges',             # 826 records - may want to keep for learning
        'llm_pdf_extractions',         # 8 records
        'llm_billing_line_items',      # 45 records  
        'llm_invoice_summary',         # 8 records
        'invoice_images'               # 2 records
    ]
    
    print("\n" + "=" * 60)
    print("Clearing optional invoice-related processing tables...")
    
    for table in optional_tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            count_before = cursor.fetchone()[0]
            
            cursor.execute(f"DELETE FROM {table};")
            rows_deleted = cursor.rowcount
            
            print(f"✓ Deleted {rows_deleted} records from {table} (had {count_before} records)")
            
        except Exception as e:
            print(f"✗ Error deleting from {table}: {e}")
    
    # Commit all changes
    conn.commit()
    
    # Verify the cleanup
    print("\n" + "=" * 60)
    print("Verification - remaining records:")
    verification_tables = child_tables + ['invoices'] + optional_tables
    
    for table in verification_tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table};")
            count = cursor.fetchone()[0]
            status = "✓ CLEAR" if count == 0 else f"⚠ {count} records remaining"
            print(f"- {table}: {status}")
        except Exception as e:
            print(f"- {table}: Error - {e}")
    
    conn.close()
    
    print("\n" + "=" * 60)
    print("✅ Invoice table cleanup completed!")
    print("📋 The database is now ready for China logistics invoice data.")

if __name__ == '__main__':
    print("⚠️  WARNING: This will delete ALL invoice data from the database!")
    print("   This includes AU/DHL data and will prepare for China logistics data.")
    print()
    confirm = input("Do you want to proceed? (type 'YES' to confirm): ")
    
    if confirm == 'YES':
        clear_invoice_data()
    else:
        print("❌ Operation cancelled.")
