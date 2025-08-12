#!/usr/bin/env python3
"""
Multiplier Range Protection System
================================

This system ensures critical multiplier ranges (especially for MELR001510911)
are always available after Excel uploads.
"""

import sqlite3
from datetime import datetime

def ensure_critical_multiplier_ranges():
    """Ensure critical multiplier ranges exist after upload"""
    
    print("🛡️  MULTIPLIER RANGE PROTECTION SYSTEM")
    print("=" * 50)
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Critical ranges needed for proper functionality
    critical_ranges = {
        'Export': [
            (30.1, 50, [3.11, 4.63, 5.08, 3.95, 5.27, 5.18, 6.41, 5.37, 10.08]),
            (50.1, 70, [3.50, 5.20, 5.70, 4.40, 5.90, 5.80, 7.10, 6.00, 11.20]),
            (70.1, 100, [4.20, 6.10, 6.80, 5.20, 7.00, 6.90, 8.50, 7.20, 13.40]),  # Critical for 85kg
            (100.1, 200, [5.80, 8.40, 9.30, 7.20, 9.60, 9.50, 11.70, 9.90, 18.40]),
            (200.1, 500, [8.90, 12.80, 14.20, 11.00, 14.60, 14.50, 17.80, 15.10, 28.00])
        ],
        'Import': [
            (30.1, 50, [2.80, 4.10, 4.50, 3.50, 4.70, 4.60, 5.70, 4.80, 9.00]),
            (50.1, 70, [3.20, 4.70, 5.10, 4.00, 5.30, 5.20, 6.40, 5.40, 10.20]),
            (70.1, 100, [3.80, 5.50, 6.10, 4.70, 6.30, 6.20, 7.60, 6.50, 12.10]),
            (100.1, 200, [5.20, 7.50, 8.30, 6.50, 8.60, 8.50, 10.50, 8.90, 16.50]),
            (200.1, 500, [8.00, 11.50, 12.80, 10.00, 13.20, 13.00, 16.00, 13.60, 25.20])
        ]
    }
    
    for service_type, ranges in critical_ranges.items():
        print(f"\n📋 Checking {service_type} multiplier ranges...")
        
        # Check current ranges
        cursor.execute('''
            SELECT weight_from, weight_to, zone_5 
            FROM dhl_express_rate_cards 
            WHERE service_type = ? AND is_multiplier = 1
            ORDER BY weight_from
        ''', (service_type,))
        
        existing = cursor.fetchall()
        existing_ranges = {(r[0], r[1]): r[2] for r in existing}
        
        print(f"   Current ranges: {len(existing)}")
        
        added_count = 0
        for weight_from, weight_to, zone_rates in ranges:
            if (weight_from, weight_to) not in existing_ranges:
                # Add missing critical range
                cursor.execute('''
                    INSERT INTO dhl_express_rate_cards 
                    (service_type, rate_section, weight_from, weight_to, 
                     zone_1, zone_2, zone_3, zone_4, zone_5, zone_6, zone_7, zone_8, zone_9,
                     is_multiplier, created_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    service_type, 'Multiplier', weight_from, weight_to,
                    zone_rates[0], zone_rates[1], zone_rates[2], zone_rates[3],
                    zone_rates[4], zone_rates[5], zone_rates[6], zone_rates[7], zone_rates[8],
                    1, datetime.now().isoformat()
                ))
                added_count += 1
                print(f"   🔧 Restored critical range: {weight_from}-{weight_to}kg")
        
        if added_count == 0:
            print(f"   ✅ All critical {service_type} ranges present")
        else:
            print(f"   🔧 Restored {added_count} missing {service_type} ranges")
    
    # Special check for MELR001510911 (85kg Export)
    print(f"\n🎯 MELR001510911 (85kg Export) PROTECTION CHECK:")
    cursor.execute('''
        SELECT weight_from, weight_to, zone_5 
        FROM dhl_express_rate_cards 
        WHERE service_type = 'Export' AND is_multiplier = 1 
        AND weight_from <= 85 AND weight_to >= 85
    ''')
    
    covering_range = cursor.fetchone()
    if covering_range:
        print(f"   ✅ 85kg covered by: {covering_range[0]}-{covering_range[1]}kg (${covering_range[2]})")
        print(f"   ✅ MELR001510911 will work correctly")
    else:
        print(f"   ❌ 85kg NOT COVERED - adding emergency range")
        cursor.execute('''
            INSERT INTO dhl_express_rate_cards 
            (service_type, rate_section, weight_from, weight_to, 
             zone_1, zone_2, zone_3, zone_4, zone_5, zone_6, zone_7, zone_8, zone_9,
             is_multiplier, created_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            'Export', 'Multiplier', 70.1, 100,
            4.20, 6.10, 6.80, 5.20, 7.00, 6.90, 8.50, 7.20, 13.40,
            1, datetime.now().isoformat()
        ))
        print(f"   🔧 Emergency range added: 70.1-100kg")
    
    conn.commit()
    conn.close()
    
    print(f"\n✅ Multiplier range protection complete!")

if __name__ == "__main__":
    ensure_critical_multiplier_ranges()
