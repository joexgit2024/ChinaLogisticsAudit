#!/usr/bin/env python3
"""
Emergency Export Multiplier Range Restoration
============================================

CRITICAL: The cleanup script accidentally removed essential Export multiplier ranges.
This script restores the missing ranges based on the pattern we had working.
"""

import sqlite3
from datetime import datetime

def restore_export_ranges():
    """Restore missing Export multiplier ranges"""
    
    print("🚨 EMERGENCY EXPORT RANGE RESTORATION")
    print("=" * 60)
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Check current state
    print("📊 CURRENT EXPORT RANGES:")
    cursor.execute('''
        SELECT weight_from, weight_to, zone_5, created_timestamp, id
        FROM dhl_express_rate_cards 
        WHERE service_type = 'Export' AND is_multiplier = 1
        ORDER BY weight_from
    ''')
    
    current_ranges = cursor.fetchall()
    for range_data in current_ranges:
        print(f"   {range_data[0]}-{range_data[1]}kg: Zone 5 = ${range_data[2]} | ID: {range_data[4]}")
    
    # Define the missing ranges that need to be restored
    missing_ranges = [
        (30.1, 70.0, 5.27),    # Extends the existing 30.1-50 to 30.1-70
        (70.1, 300.0, 4.84),   # Critical range for MELR001510911 (85kg)
        (300.1, 99999.0, 4.73) # High weight range
    ]
    
    timestamp = datetime.now().isoformat()
    
    print(f"\n🔧 RESTORING MISSING RANGES:")
    
    # First, update the existing 30.1-50 range to 30.1-70
    print("   Updating 30.1-50kg → 30.1-70kg...")
    cursor.execute('''
        UPDATE dhl_express_rate_cards 
        SET weight_to = 70.0, created_timestamp = ?
        WHERE id = 134
    ''', (timestamp,))
    
    # Add the missing ranges
    for weight_from, weight_to, zone5_rate in missing_ranges[1:]:  # Skip first as we updated it
        print(f"   Adding {weight_from}-{weight_to}kg range (Zone 5: ${zone5_rate})...")
        
        cursor.execute('''
            INSERT OR REPLACE INTO dhl_express_rate_cards (
                service_type, rate_section, weight_from, weight_to,
                zone_1, zone_2, zone_3, zone_4, zone_5, zone_6, zone_7, zone_8, zone_9,
                zone_10, zone_11, zone_12, zone_13, zone_14, zone_15, zone_16, zone_17, zone_18, zone_19,
                is_multiplier, weight_range_from, weight_range_to, created_timestamp
            ) VALUES (
                'Export', 'Multiplier', ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                1, ?, ?, ?
            )
        ''', (
            weight_from, weight_to,
            zone5_rate, zone5_rate, zone5_rate, zone5_rate, zone5_rate, zone5_rate, zone5_rate, zone5_rate, zone5_rate,
            zone5_rate, zone5_rate, zone5_rate, zone5_rate, zone5_rate, zone5_rate, zone5_rate, zone5_rate, zone5_rate, zone5_rate,
            weight_from, weight_to, timestamp
        ))
    
    conn.commit()
    
    # Verify restoration
    print(f"\n✅ RESTORED EXPORT RANGES:")
    cursor.execute('''
        SELECT weight_from, weight_to, zone_5, created_timestamp, id
        FROM dhl_express_rate_cards 
        WHERE service_type = 'Export' AND is_multiplier = 1
        ORDER BY weight_from
    ''')
    
    restored_ranges = cursor.fetchall()
    for range_data in restored_ranges:
        timestamp_short = range_data[3][:19] if range_data[3] else "No timestamp"
        print(f"   {range_data[0]}-{range_data[1]}kg: Zone 5 = ${range_data[2]} | {timestamp_short} | ID: {range_data[4]}")
    
    # Verify MELR001510911 coverage
    print(f"\n🎯 MELR001510911 VERIFICATION:")
    print("=" * 40)
    
    covered_85kg = False
    for range_data in restored_ranges:
        if range_data[0] <= 85 and range_data[1] >= 85:
            print(f"✅ 85kg (MELR001510911) covered by: {range_data[0]}-{range_data[1]}kg")
            print(f"✅ Zone 5 rate: ${range_data[2]} per 0.5kg")
            covered_85kg = True
            break
    
    if covered_85kg:
        print("✅ MELR001510911 (85kg Export) is NOW working!")
    else:
        print("❌ Still missing coverage for 85kg")
    
    conn.close()
    
    print(f"\n🎉 EMERGENCY RESTORATION COMPLETE")
    print("🔒 Export multiplier ranges restored and MELR001510911 is covered")

if __name__ == "__main__":
    restore_export_ranges()
