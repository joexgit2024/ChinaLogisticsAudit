#!/usr/bin/env python3
"""
Verify AU Domestic Rate Card Data
Check what data was actually loaded into the database
"""

import sqlite3

def verify_au_domestic_data():
    """Verify the loaded AU domestic data"""
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    print("🔍 AU DOMESTIC RATE CARD DATA VERIFICATION")
    print("=" * 60)
    
    # Check zones
    print("\n📍 ZONES:")
    cursor.execute("""
        SELECT zone_number, city_name, city_code, service_area 
        FROM dhl_express_au_domestic_zones 
        ORDER BY zone_number
    """)
    zones = cursor.fetchall()
    for zone in zones:
        zone_num, city_name, city_code, service_area = zone
        print(f"  Zone {zone_num}: {city_name or 'N/A'} ({city_code or 'N/A'}) - {service_area or 'N/A'}")
    
    # Check matrix (sample)
    print(f"\n🔄 MATRIX (sample - showing origin zone 1 and 3):")
    cursor.execute("""
        SELECT origin_zone, destination_zone, rate_zone 
        FROM dhl_express_au_domestic_matrix 
        WHERE origin_zone IN (1, 3)
        ORDER BY origin_zone, destination_zone
    """)
    matrix = cursor.fetchall()
    for origin, dest, rate_zone in matrix:
        print(f"  Zone {origin} → Zone {dest} = Rate Zone {rate_zone}")
    
    # Check rates (sample)
    print(f"\n💰 RATES (sample - showing rate zones A and B):")
    cursor.execute("""
        SELECT weight_kg, zone_a, zone_b, zone_c, zone_d, zone_e, zone_f, zone_g, zone_h
        FROM dhl_express_au_domestic_rates 
        WHERE weight_kg <= 2.0
        ORDER BY weight_kg
        LIMIT 5
    """)
    rates = cursor.fetchall()
    print("  Weight  Zone A  Zone B  Zone C  Zone D  Zone E  Zone F  Zone G  Zone H")
    for rate in rates:
        weight = rate[0]
        zones = rate[1:]
        zone_str = "  ".join([f"{z:>6.2f}" if z else "   N/A" for z in zones])
        print(f"  {weight:>5.1f}   {zone_str}")
    
    # Test Melbourne → Sydney calculation
    print(f"\n🎯 MELBOURNE → SYDNEY TEST:")
    
    # 1. Find Melbourne zone
    cursor.execute("SELECT zone_number FROM dhl_express_au_domestic_zones WHERE city_code = 'MEL'")
    mel_zone = cursor.fetchone()
    mel_zone_num = mel_zone[0] if mel_zone else None
    
    # 2. Find Sydney zone  
    cursor.execute("SELECT zone_number FROM dhl_express_au_domestic_zones WHERE city_code = 'SYD'")
    syd_zone = cursor.fetchone()
    syd_zone_num = syd_zone[0] if syd_zone else None
    
    if mel_zone_num and syd_zone_num:
        print(f"  Melbourne = Zone {mel_zone_num}")
        print(f"  Sydney = Zone {syd_zone_num}")
        
        # 3. Find rate zone
        cursor.execute("""
            SELECT rate_zone FROM dhl_express_au_domestic_matrix 
            WHERE origin_zone = ? AND destination_zone = ?
        """, (mel_zone_num, syd_zone_num))
        rate_zone_result = cursor.fetchone()
        rate_zone = rate_zone_result[0] if rate_zone_result else None
        
        if rate_zone:
            print(f"  Zone {mel_zone_num} → Zone {syd_zone_num} = Rate Zone {rate_zone}")
            
            # 4. Find 1.5kg rate
            cursor.execute(f"""
                SELECT weight_kg, zone_{rate_zone.lower()} FROM dhl_express_au_domestic_rates 
                WHERE weight_kg = 1.5
            """)
            rate_result = cursor.fetchone()
            if rate_result:
                weight, rate = rate_result
                print(f"  {weight}kg in Zone {rate_zone} = ${rate:.2f}")
                
                # Verify against expected $16.47
                expected = 16.47
                if abs(rate - expected) < 0.01:
                    print(f"  ✅ CORRECT: Rate matches expected ${expected:.2f}")
                else:
                    print(f"  ❌ INCORRECT: Expected ${expected:.2f}, got ${rate:.2f}")
            else:
                print(f"  ❌ No rate found for 1.5kg in Zone {rate_zone}")
        else:
            print(f"  ❌ No rate zone found for Zone {mel_zone_num} → Zone {syd_zone_num}")
    else:
        print(f"  ❌ Could not find Melbourne or Sydney zones")
    
    conn.close()

if __name__ == "__main__":
    verify_au_domestic_data()
