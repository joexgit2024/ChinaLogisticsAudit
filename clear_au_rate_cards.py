#!/usr/bin/env python3
"""
Check and clear existing DHL Express AU rate cards before loading CN cards
"""
import sqlite3

def check_existing_rates():
    """Check what rate cards currently exist"""
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    print("=== Current DHL Express Rate Cards ===")
    
    # Total count
    cursor.execute('SELECT COUNT(*) FROM dhl_express_rate_cards')
    total = cursor.fetchone()[0]
    print(f"Total rate cards: {total}")
    
    if total > 0:
        # By service type
        cursor.execute('SELECT service_type, rate_section, COUNT(*) FROM dhl_express_rate_cards GROUP BY service_type, rate_section')
        print("\nBreakdown by service and section:")
        for row in cursor.fetchall():
            print(f"  {row[0]} - {row[1]}: {row[2]} records")
        
        # Sample records
        cursor.execute('SELECT service_type, rate_section, weight_from, weight_to, zone_1, zone_2 FROM dhl_express_rate_cards LIMIT 5')
        print("\nSample records:")
        for row in cursor.fetchall():
            print(f"  {row[0]} {row[1]}: {row[2]}-{row[3]}kg, Zone1: {row[4]}, Zone2: {row[5]}")
    
    conn.close()

def clear_existing_rates():
    """Clear all existing rate cards"""
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Backup first
    cursor.execute('DROP TABLE IF EXISTS dhl_express_rate_cards_backup_au')
    cursor.execute('CREATE TABLE dhl_express_rate_cards_backup_au AS SELECT * FROM dhl_express_rate_cards')
    
    backup_count = cursor.execute('SELECT COUNT(*) FROM dhl_express_rate_cards_backup_au').fetchone()[0]
    print(f"Backed up {backup_count} AU rate cards to dhl_express_rate_cards_backup_au")
    
    # Clear main table
    cursor.execute('DELETE FROM dhl_express_rate_cards')
    cursor.execute('DELETE FROM dhl_express_rate_cards_new WHERE 1=1')  # Clear new table too if exists
    
    conn.commit()
    
    # Verify cleared
    remaining = cursor.execute('SELECT COUNT(*) FROM dhl_express_rate_cards').fetchone()[0]
    print(f"Cleared main table. Remaining records: {remaining}")
    
    conn.close()

if __name__ == '__main__':
    print("Checking existing DHL Express rate cards...")
    check_existing_rates()
    
    response = input("\nDo you want to clear all AU rate cards and proceed with CN loading? (y/N): ")
    if response.lower() == 'y':
        print("\nClearing AU rate cards...")
        clear_existing_rates()
        print("✅ AU rate cards cleared. Ready to load CN rate cards.")
    else:
        print("Cancelled. AU rate cards preserved.")
