#!/usr/bin/env python3
"""
Clear DHL Express AU Data for China Migration
===========================================

Clears all AU-specific DHL Express data to prepare for Chinese rate cards.
This includes:
- dhl_express_rate_cards (AU rates)
- dhl_express_au_domestic_* tables
- dhl_express_export_zones / import_zones (AU-specific)
- Any AU-specific service surcharges
"""

import sqlite3
from datetime import datetime

class DHLExpressAUCleaner:
    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path
        
    def clear_all_au_data(self):
        """Clear all AU-specific DHL Express data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            print("🧹 Clearing DHL Express AU data for China migration...")
            
            # 1. Clear main rate cards table
            cursor.execute("SELECT COUNT(*) FROM dhl_express_rate_cards")
            rate_count = cursor.fetchone()[0]
            print(f"   📊 Found {rate_count} rate card entries")
            
            cursor.execute("DELETE FROM dhl_express_rate_cards")
            print("   ✅ Cleared dhl_express_rate_cards")
            
            # 2. Clear AU domestic tables
            au_domestic_tables = [
                'dhl_express_au_domestic_zones',
                'dhl_express_au_domestic_rates', 
                'dhl_express_au_domestic_matrix',
                'dhl_express_au_domestic_uploads'
            ]
            
            for table in au_domestic_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    if count > 0:
                        cursor.execute(f"DELETE FROM {table}")
                        print(f"   ✅ Cleared {table} ({count} records)")
                    else:
                        print(f"   ➖ {table} already empty")
                except sqlite3.Error as e:
                    print(f"   ⚠️  Error clearing {table}: {e}")
            
            # 3. Clear export/import zones (likely AU-specific)
            zone_tables = ['dhl_express_export_zones', 'dhl_express_import_zones']
            for table in zone_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    if count > 0:
                        cursor.execute(f"DELETE FROM {table}")
                        print(f"   ✅ Cleared {table} ({count} records)")
                    else:
                        print(f"   ➖ {table} already empty")
                except sqlite3.Error as e:
                    print(f"   ⚠️  Error clearing {table}: {e}")
            
            # 4. Clear AU-specific service surcharges if any
            try:
                cursor.execute("SELECT COUNT(*) FROM dhl_express_services_surcharges WHERE service_type LIKE '%AU%' OR zone_info LIKE '%AU%'")
                au_surcharge_count = cursor.fetchone()[0]
                if au_surcharge_count > 0:
                    cursor.execute("DELETE FROM dhl_express_services_surcharges WHERE service_type LIKE '%AU%' OR zone_info LIKE '%AU%'")
                    print(f"   ✅ Cleared {au_surcharge_count} AU-specific surcharges")
                else:
                    print("   ➖ No AU-specific surcharges found")
            except sqlite3.Error as e:
                print(f"   ⚠️  Error clearing AU surcharges: {e}")
            
            # 5. Clear any AU-specific zone mappings
            try:
                cursor.execute("SELECT COUNT(*) FROM dhl_express_zone_mapping WHERE origin_region LIKE '%AU%' OR destination_region LIKE '%AU%'")
                au_zone_count = cursor.fetchone()[0]
                if au_zone_count > 0:
                    cursor.execute("DELETE FROM dhl_express_zone_mapping WHERE origin_region LIKE '%AU%' OR destination_region LIKE '%AU%'")
                    print(f"   ✅ Cleared {au_zone_count} AU zone mappings")
                else:
                    print("   ➖ No AU zone mappings found")
            except sqlite3.Error as e:
                print(f"   ⚠️  Error clearing AU zone mappings: {e}")
            
            # 6. Clear any backup tables
            backup_tables = ['dhl_express_rate_cards_backup', 'dhl_express_services_surcharges_backup']
            for table in backup_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    if count > 0:
                        cursor.execute(f"DELETE FROM {table}")
                        print(f"   ✅ Cleared backup table {table} ({count} records)")
                    else:
                        print(f"   ➖ Backup table {table} already empty")
                except sqlite3.Error as e:
                    print(f"   ⚠️  Error clearing {table}: {e}")
            
            # 7. Reset any auto-increment sequences
            try:
                cursor.execute("DELETE FROM sqlite_sequence WHERE name LIKE 'dhl_express%'")
                print("   ✅ Reset auto-increment sequences")
            except sqlite3.Error as e:
                print(f"   ⚠️  Error resetting sequences: {e}")
            
            conn.commit()
            
            # Verify cleanup
            print("\n📋 Cleanup verification:")
            verification_tables = [
                'dhl_express_rate_cards',
                'dhl_express_au_domestic_zones',
                'dhl_express_au_domestic_rates',
                'dhl_express_export_zones',
                'dhl_express_import_zones'
            ]
            
            for table in verification_tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    status = "✅ Empty" if count == 0 else f"⚠️  {count} records remaining"
                    print(f"   {table}: {status}")
                except sqlite3.Error:
                    print(f"   {table}: ❓ Table not accessible")
            
            print(f"\n🎉 DHL Express AU data cleanup completed at {datetime.now()}")
            print("   Ready for Chinese rate card loading!")
            
            return {
                'success': True,
                'message': 'AU data cleared successfully',
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            conn.rollback()
            error_msg = f"Failed to clear AU data: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                'success': False,
                'error': error_msg
            }
        finally:
            conn.close()
http://localhost:5000/dhl-express/invoices
def main():
    cleaner = DHLExpressAUCleaner()
    result = cleaner.clear_all_au_data()
    
    if result['success']:
        print("\n✅ Ready to proceed with Chinese rate card loading!")
    else:
        print(f"\n❌ Cleanup failed: {result['error']}")

if __name__ == '__main__':
    main()
