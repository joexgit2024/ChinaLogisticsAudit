#!/usr/bin/env python3
"""
Add Export adder rates to DHL Express rate card database
Based on the multiplier rate table provided by user
"""

import sqlite3
from datetime import datetime

def add_export_adder_rates():
    """Add Export multiplier adder rates"""
    
    db_path = 'dhl_audit.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("Adding Export adder rates...")
    
    # Export Multiplier rates from the table provided
    # Multiplier rate per 0.5 KG from 30.1 KG
    export_adder_rates = [
        # From 30.1 to 70 kg
        {
            'service_type': 'Export',
            'rate_section': 'Multiplier',
            'weight_from': 30.1,
            'weight_to': 70,
            'is_multiplier': 1,
            'zone_1': 3.11,
            'zone_2': 4.63,
            'zone_3': 5.08,
            'zone_4': 3.95,
            'zone_5': 5.27,
            'zone_6': 5.18,
            'zone_7': 6.41,
            'zone_8': 5.37,
            'zone_9': 10.08
        },
        # From 70.1 to 300 kg
        {
            'service_type': 'Export',
            'rate_section': 'Multiplier',
            'weight_from': 70.1,
            'weight_to': 300,
            'is_multiplier': 1,
            'zone_1': 2.77,
            'zone_2': 4.28,
            'zone_3': 4.49,
            'zone_4': 3.64,
            'zone_5': 4.84,
            'zone_6': 4.63,
            'zone_7': 6.09,
            'zone_8': 4.76,
            'zone_9': 9.22
        },
        # From 300.1 to 99,999 kg
        {
            'service_type': 'Export',
            'rate_section': 'Multiplier',
            'weight_from': 300.1,
            'weight_to': 99999,
            'is_multiplier': 1,
            'zone_1': 2.68,
            'zone_2': 4.28,
            'zone_3': 4.36,
            'zone_4': 3.59,
            'zone_5': 4.73,
            'zone_6': 4.49,
            'zone_7': 5.87,
            'zone_8': 4.63,
            'zone_9': 8.93
        }
    ]
    
    added_count = 0
    updated_count = 0
    
    for rate in export_adder_rates:
        try:
            # Check if this rate already exists
            cursor.execute('''
                SELECT id FROM dhl_express_rate_cards 
                WHERE service_type = ? AND rate_section = ? 
                AND weight_from = ? AND weight_to = ?
            ''', (rate['service_type'], rate['rate_section'], 
                  rate['weight_from'], rate['weight_to']))
            
            existing = cursor.fetchone()
            
            if existing:
                # Update existing record
                cursor.execute('''
                    UPDATE dhl_express_rate_cards 
                    SET is_multiplier = ?, zone_1 = ?, zone_2 = ?, zone_3 = ?, 
                        zone_4 = ?, zone_5 = ?, zone_6 = ?, zone_7 = ?, 
                        zone_8 = ?, zone_9 = ?
                    WHERE id = ?
                ''', (rate['is_multiplier'], rate['zone_1'], rate['zone_2'], 
                      rate['zone_3'], rate['zone_4'], rate['zone_5'], 
                      rate['zone_6'], rate['zone_7'], rate['zone_8'], 
                      rate['zone_9'], existing[0]))
                updated_count += 1
                print(f"Updated Export {rate['rate_section']} {rate['weight_from']}-{rate['weight_to']}kg")
            else:
                # Insert new record
                cursor.execute('''
                    INSERT INTO dhl_express_rate_cards 
                    (service_type, rate_section, weight_from, weight_to, is_multiplier,
                     zone_1, zone_2, zone_3, zone_4, zone_5, zone_6, zone_7, zone_8, zone_9,
                     created_timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (rate['service_type'], rate['rate_section'], 
                      rate['weight_from'], rate['weight_to'], rate['is_multiplier'],
                      rate['zone_1'], rate['zone_2'], rate['zone_3'], rate['zone_4'],
                      rate['zone_5'], rate['zone_6'], rate['zone_7'], rate['zone_8'],
                      rate['zone_9'], datetime.now().isoformat()))
                added_count += 1
                print(f"Added Export {rate['rate_section']} {rate['weight_from']}-{rate['weight_to']}kg")
                
        except Exception as e:
            print(f"Error processing rate {rate['weight_from']}-{rate['weight_to']}: {e}")
            continue
    
    conn.commit()
    
    # Verify the new rates
    print(f"\nVerification - Export multiplier rates >= 30kg:")
    cursor.execute('''
        SELECT weight_from, weight_to, zone_3, is_multiplier
        FROM dhl_express_rate_cards 
        WHERE service_type = 'Export' AND rate_section = 'Multiplier' 
        AND weight_from >= 30
        ORDER BY weight_from
    ''')
    
    results = cursor.fetchall()
    for row in results:
        print(f"  {row[0]}-{row[1]}kg: Zone 3 = {row[2]} (Multiplier: {row[3]})")
    
    conn.close()
    
    print(f"\nSummary:")
    print(f"- Added {added_count} new Export adder rates")
    print(f"- Updated {updated_count} existing Export adder rates")
    print(f"- Total Export multiplier rates >= 30kg: {len(results)}")
    
    return {
        'success': True,
        'added': added_count,
        'updated': updated_count,
        'total_rates': len(results)
    }

if __name__ == '__main__':
    result = add_export_adder_rates()
    print(f"\nOperation completed: {result}")
