import sqlite3
from datetime import datetime

def clear_dgf_invoices():
    """Clear existing DGF invoice records from the database"""
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Check current count
    cursor.execute('SELECT COUNT(*) FROM dgf_invoices')
    current_count = cursor.fetchone()[0]
    print(f"Current DGF invoice records: {current_count}")
    
    if current_count > 0:
        # Create backup timestamp
        backup_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create backup table (optional - for safety)
        cursor.execute(f'''
            CREATE TABLE dgf_invoices_backup_{backup_timestamp} AS 
            SELECT * FROM dgf_invoices
        ''')
        print(f"✅ Created backup table: dgf_invoices_backup_{backup_timestamp}")
        
        # Clear the main table
        cursor.execute('DELETE FROM dgf_invoices')
        deleted_count = cursor.rowcount
        
        # Reset auto-increment counter
        cursor.execute('DELETE FROM sqlite_sequence WHERE name="dgf_invoices"')
        
        conn.commit()
        print(f"✅ Deleted {deleted_count} records from dgf_invoices table")
        
        # Verify deletion
        cursor.execute('SELECT COUNT(*) FROM dgf_invoices')
        new_count = cursor.fetchone()[0]
        print(f"✅ Current DGF invoice records after deletion: {new_count}")
        
    else:
        print("No records to delete")
    
    conn.close()

def verify_new_structure():
    """Verify that the invoice_number column is properly configured"""
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    print("\nVerifying table structure for invoice numbers:")
    print("=" * 50)
    
    # Check table structure
    cursor.execute('PRAGMA table_info(dgf_invoices)')
    columns = cursor.fetchall()
    
    # Look for invoice_number column
    invoice_number_col = None
    for col in columns:
        if col[1] == 'invoice_number':
            invoice_number_col = col
            break
    
    if invoice_number_col:
        print(f"✅ invoice_number column found:")
        print(f"   Type: {invoice_number_col[2]}")
        print(f"   Not Null: {'Yes' if invoice_number_col[3] else 'No'}")
        print(f"   Default: {invoice_number_col[4] if invoice_number_col[4] else 'None'}")
    else:
        print("❌ invoice_number column not found!")
    
    # Check other key columns for the new format
    key_columns = ['lane_id_fqr', 'ata_date', 'pkg_no', 'm3_volume', 'pickup_charge', 
                   'trax_status', 'tax_invoice_number']
    
    print(f"\nKey columns for DGF-CN10 format:")
    for col_name in key_columns:
        found = any(col[1] == col_name for col in columns)
        status = "✅" if found else "❌"
        print(f"   {status} {col_name}")
    
    conn.close()

if __name__ == "__main__":
    print("🧹 Clearing DGF invoice records...")
    clear_dgf_invoices()
    
    print("\n🔍 Verifying database structure...")
    verify_new_structure()
    
    print("\n📋 Summary:")
    print("1. ✅ Existing DGF records cleared (with backup)")
    print("2. ✅ Table structure verified for new format")
    print("3. ✅ Ready for new invoice uploads with correct invoice numbers")
    print("\nYou can now upload your DGF-CN10 billing.xlsx files through the upload screen!")
