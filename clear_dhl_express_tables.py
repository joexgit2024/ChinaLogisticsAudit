#!/usr/bin/env python3
"""
Clear DHL Express Tables
========================

Safely clear dhl_express_invoices and dhl_express_audit_results tables
to allow for fresh invoice loading and audit processing.
"""

import sqlite3
import os
from datetime import datetime


def clear_dhl_express_tables(db_path: str = 'dhl_audit.db'):
    """Clear DHL Express invoice and audit result tables"""
    
    if not os.path.exists(db_path):
        print(f"❌ Database file {db_path} not found!")
        return False
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print("=== Clearing DHL Express Tables ===")
        
        # Check current record counts
        cursor.execute("SELECT COUNT(*) FROM dhl_express_invoices")
        invoice_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM dhl_express_audit_results")
        audit_count = cursor.fetchone()[0]
        
        print(f"Current records:")
        print(f"  - dhl_express_invoices: {invoice_count:,} records")
        print(f"  - dhl_express_audit_results: {audit_count:,} records")
        
        if invoice_count == 0 and audit_count == 0:
            print("✅ Tables are already empty - nothing to clear")
            conn.close()
            return True
        
        # Create backup timestamp
        backup_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Clear the tables
        print(f"\n🗑️  Clearing tables...")
        
        cursor.execute("DELETE FROM dhl_express_audit_results")
        cleared_audit = cursor.rowcount
        
        cursor.execute("DELETE FROM dhl_express_invoices")
        cleared_invoices = cursor.rowcount
        
        # Reset auto-increment counters
        cursor.execute("DELETE FROM sqlite_sequence WHERE name='dhl_express_invoices'")
        cursor.execute("DELETE FROM sqlite_sequence WHERE name='dhl_express_audit_results'")
        
        conn.commit()
        
        print(f"✅ Successfully cleared:")
        print(f"  - dhl_express_invoices: {cleared_invoices:,} records deleted")
        print(f"  - dhl_express_audit_results: {cleared_audit:,} records deleted")
        print(f"  - Auto-increment counters reset")
        
        # Verify tables are empty
        cursor.execute("SELECT COUNT(*) FROM dhl_express_invoices")
        remaining_invoices = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM dhl_express_audit_results")
        remaining_audit = cursor.fetchone()[0]
        
        if remaining_invoices == 0 and remaining_audit == 0:
            print(f"✅ Verification: Both tables are now empty")
        else:
            print(f"⚠️  Warning: Some records may remain:")
            print(f"  - dhl_express_invoices: {remaining_invoices}")
            print(f"  - dhl_express_audit_results: {remaining_audit}")
        
        conn.close()
        
        print(f"\n🎯 Tables cleared successfully!")
        print(f"📋 Backup reference: {backup_timestamp}")
        print(f"💡 You can now reload invoices and redo audits")
        
        return True
        
    except Exception as e:
        print(f"❌ Error clearing tables: {e}")
        return False


def check_other_dhl_tables(db_path: str = 'dhl_audit.db'):
    """Check status of other DHL-related tables (non-destructive)"""
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        print(f"\n=== Other DHL Express Tables Status ===")
        
        # Check rate cards
        try:
            cursor.execute("SELECT COUNT(*) FROM dhl_express_rate_cards")
            rate_count = cursor.fetchone()[0]
            print(f"📊 dhl_express_rate_cards: {rate_count:,} records (preserved)")
        except:
            print(f"📊 dhl_express_rate_cards: table not found")
        
        # Check zone mappings
        try:
            cursor.execute("SELECT COUNT(*) FROM dhl_express_zone_mapping")
            zone_count = cursor.fetchone()[0]
            print(f"🗺️  dhl_express_zone_mapping: {zone_count:,} records (preserved)")
        except:
            print(f"🗺️  dhl_express_zone_mapping: table not found")
        
        # Check 3rd party tables
        try:
            cursor.execute("SELECT COUNT(*) FROM dhl_express_3rd_party_zones")
            third_zone_count = cursor.fetchone()[0]
            print(f"🌍 dhl_express_3rd_party_zones: {third_zone_count:,} records (preserved)")
        except:
            print(f"🌍 dhl_express_3rd_party_zones: table not found")
        
        try:
            cursor.execute("SELECT COUNT(*) FROM dhl_express_3rd_party_matrix")
            third_matrix_count = cursor.fetchone()[0]
            print(f"📋 dhl_express_3rd_party_matrix: {third_matrix_count:,} records (preserved)")
        except:
            print(f"📋 dhl_express_3rd_party_matrix: table not found")
        
        try:
            cursor.execute("SELECT COUNT(*) FROM dhl_express_3rd_party_rates")
            third_rate_count = cursor.fetchone()[0]
            print(f"💰 dhl_express_3rd_party_rates: {third_rate_count:,} records (preserved)")
        except:
            print(f"💰 dhl_express_3rd_party_rates: table not found")
        
        conn.close()
        
    except Exception as e:
        print(f"⚠️  Error checking other tables: {e}")


def main():
    """Main execution"""
    
    print("=== DHL Express Table Cleanup Utility ===")
    print("This will clear invoice and audit data for fresh processing\n")
    
    # Confirm operation
    response = input("Are you sure you want to clear all DHL Express invoices and audit results? (yes/no): ")
    
    if response.lower() in ['yes', 'y']:
        success = clear_dhl_express_tables()
        
        if success:
            check_other_dhl_tables()
            print(f"\n🚀 Ready for fresh invoice loading and audit processing!")
        else:
            print(f"\n❌ Table clearing failed - please check the error messages above")
    else:
        print("❌ Operation cancelled by user")


if __name__ == "__main__":
    main()
