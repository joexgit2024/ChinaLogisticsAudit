#!/usr/bin/env python3
import sqlite3
import math

class DetailedAWBAudit:
    def __init__(self):
        self.db_path = 'fedex_audit.db'
    
    def get_zone_mapping(self, origin_country, dest_country):
        """Get zone mapping for country pair"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Map short codes to full names
            country_mapping = {
                'US': 'United States, PR',
                'HK': 'Hong Kong',
                'CN': 'China',
                'JP': 'Japan'
            }
            
            full_origin = country_mapping.get(origin_country, origin_country)
            
            cursor.execute('''
                SELECT zone_letter FROM fedex_zone_matrix 
                WHERE origin_country = ? AND destination_region LIKE ?
            ''', (full_origin, f'%{dest_country}%'))
            
            result = cursor.fetchone()
            if result:
                return result[0]
            
            # Fallback mappings based on known FedEx zones
            zone_map = {
                ('US', 'CN'): 'F',
                ('HK', 'CN'): 'A', 
                ('JP', 'CN'): 'B',
                ('United States, PR', 'CN'): 'F',
                ('Hong Kong', 'CN'): 'A'
            }
            
            zone = zone_map.get((origin_country, dest_country)) or zone_map.get((full_origin, dest_country))
            return zone if zone else 'UNKNOWN'
            
        finally:
            conn.close()
    
    def round_weight_for_billing(self, actual_weight):
        """Apply FedEx weight rounding rules"""
        if actual_weight > 21:
            # Over 21kg: round up to full kg
            return math.ceil(actual_weight)
        else:
            # Under 21kg: round up to next 0.5kg increment
            return math.ceil(actual_weight * 2) / 2
    
    def get_fedex_rate(self, chargeable_weight, zone, service_type='PRIORITY_EXPRESS'):
        """Get FedEx rate for weight and zone"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # For packages <= 20.5kg, use IP rates
            if chargeable_weight <= 20.5:
                cursor.execute('''
                    SELECT rate_usd FROM fedex_rate_cards
                    WHERE service_type = ? AND zone_code = ? 
                    AND weight_from = ? AND weight_to = ?
                    AND rate_type = 'IP'
                    LIMIT 1
                ''', (service_type, zone, chargeable_weight, chargeable_weight))
                
                result = cursor.fetchone()
                if result:
                    return {
                        'rate_usd': float(result[0]),
                        'rate_type': 'IP',
                        'is_per_kg': False
                    }
            
            # For heavyweight packages (>20kg), use per-kg IPKG rates
            if chargeable_weight > 20:
                # Try to find a base IPKG rate
                cursor.execute('''
                    SELECT rate_usd FROM fedex_rate_cards
                    WHERE service_type = ? AND zone_code = ? 
                    AND rate_type = 'IPKG'
                    LIMIT 1
                ''', (service_type, zone))
                
                result = cursor.fetchone()
                if result:
                    rate_per_kg = float(result[0])
                    total_rate = rate_per_kg * chargeable_weight
                    return {
                        'rate_usd': total_rate,
                        'rate_type': 'IPKG',
                        'rate_per_kg': rate_per_kg,
                        'is_per_kg': True
                    }
            
            return None
            
        finally:
            conn.close()
    
    def audit_awb_detailed(self, invoice_no, awb_number):
        """Detailed audit of a single AWB"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get AWB details
            cursor.execute('''
                SELECT awb_number, origin_country, dest_country, actual_weight_kg, 
                       total_awb_amount_cny, service_type, exchange_rate
                FROM fedex_invoices 
                WHERE invoice_no = ? AND awb_number = ?
            ''', (invoice_no, awb_number))
            
            awb_data = cursor.fetchone()
            if not awb_data:
                return {'success': False, 'error': f'AWB {awb_number} not found'}
            
            awb_num, origin, dest, actual_weight, claimed_cny, service_type, exchange_rate = awb_data
            
            print(f"\n🔍 DETAILED AUDIT: AWB {awb_num}")
            print("=" * 60)
            print(f"📍 Route: {origin} → {dest}")
            print(f"⚖️  Actual Weight: {actual_weight}kg")
            print(f"💰 Claimed Amount: ¥{claimed_cny:.2f}")
            print(f"💱 Exchange Rate: {exchange_rate} USD→CNY")
            
            # Step 1: Zone mapping
            zone = self.get_zone_mapping(origin, dest)
            print(f"\n📍 STEP 1 - Zone Mapping:")
            print(f"   {origin} → {dest} = Zone {zone}")
            
            # Step 2: Weight rounding
            chargeable_weight = self.round_weight_for_billing(actual_weight)
            print(f"\n⚖️  STEP 2 - Weight Rounding:")
            if actual_weight > 21:
                print(f"   Rule: >21kg → round up to full kg")
            else:
                print(f"   Rule: ≤21kg → round up to 0.5kg increment")
            print(f"   {actual_weight}kg → {chargeable_weight}kg")
            
            # Step 3: Rate lookup
            rate_info = self.get_fedex_rate(chargeable_weight, zone)
            print(f"\n💲 STEP 3 - Rate Lookup:")
            if rate_info:
                if rate_info['is_per_kg']:
                    print(f"   Rate Type: {rate_info['rate_type']} (per kg)")
                    print(f"   Rate: ${rate_info['rate_per_kg']:.2f}/kg × {chargeable_weight}kg = ${rate_info['rate_usd']:.2f}")
                else:
                    print(f"   Rate Type: {rate_info['rate_type']} (fixed)")
                    print(f"   Rate: ${rate_info['rate_usd']:.2f}")
                
                base_cost_usd = rate_info['rate_usd']
            else:
                print(f"   ❌ No rate found for {chargeable_weight}kg in zone {zone}")
                return {'success': False, 'error': f'No rate found for {chargeable_weight}kg in zone {zone}'}
            
            # Step 4: Fuel surcharge (25.5%)
            fuel_surcharge_usd = base_cost_usd * 0.255
            subtotal_usd = base_cost_usd + fuel_surcharge_usd
            print(f"\n⛽ STEP 4 - Fuel Surcharge (25.5%):")
            print(f"   Base Cost: ${base_cost_usd:.2f}")
            print(f"   Fuel Surcharge: ${fuel_surcharge_usd:.2f}")
            print(f"   Subtotal: ${subtotal_usd:.2f}")
            
            # Step 5: Convert to CNY
            subtotal_cny = subtotal_usd * exchange_rate
            print(f"\n💱 STEP 5 - Currency Conversion:")
            print(f"   ${subtotal_usd:.2f} × {exchange_rate} = ¥{subtotal_cny:.2f}")
            
            # Step 6: VAT (6%)
            vat_cny = subtotal_cny * 0.06
            total_expected_cny = subtotal_cny + vat_cny
            print(f"\n🧾 STEP 6 - VAT (6%):")
            print(f"   Subtotal: ¥{subtotal_cny:.2f}")
            print(f"   VAT: ¥{vat_cny:.2f}")
            print(f"   Total Expected: ¥{total_expected_cny:.2f}")
            
            # Step 7: Variance analysis
            variance_cny = claimed_cny - total_expected_cny
            variance_percent = (variance_cny / total_expected_cny) * 100
            print(f"\n📊 STEP 7 - Variance Analysis:")
            print(f"   Expected: ¥{total_expected_cny:.2f}")
            print(f"   Claimed:  ¥{claimed_cny:.2f}")
            print(f"   Variance: ¥{variance_cny:.2f} ({variance_percent:+.1f}%)")
            
            if abs(variance_percent) <= 2:
                status = "✅ PASS"
            elif variance_cny > 0:
                status = "⚠️  OVERCHARGE"
            else:
                status = "⚠️  UNDERCHARGE"
            print(f"   Status: {status}")
            
            return {
                'success': True,
                'awb_number': awb_num,
                'zone': zone,
                'actual_weight': actual_weight,
                'chargeable_weight': chargeable_weight,
                'base_cost_usd': base_cost_usd,
                'total_expected_cny': total_expected_cny,
                'claimed_cny': claimed_cny,
                'variance_cny': variance_cny,
                'variance_percent': variance_percent,
                'status': status
            }
            
        finally:
            conn.close()

def test_invoice_948921914():
    """Test all AWBs in invoice 948921914"""
    auditor = DetailedAWBAudit()
    
    print("🔍 DETAILED AWB AUDIT - Invoice 948921914")
    print("=" * 80)
    
    # AWB details from the interface
    awbs = [
        ('770960689095', 'US', 'CN', 8.2, 1042.57),
        ('770974916201', 'HK', 'CN', 167.0, 7312.16),
        ('771065223315', 'US', 'CN', 14.0, 1097.71)
    ]
    
    results = []
    for awb_number, origin, dest, weight, amount in awbs:
        result = auditor.audit_awb_detailed('948921914', awb_number)
        results.append(result)
    
    print(f"\n📋 SUMMARY - Invoice 948921914")
    print("=" * 60)
    total_expected = sum(r['total_expected_cny'] for r in results if r['success'])
    total_claimed = sum(r['claimed_cny'] for r in results if r['success'])
    total_variance = total_claimed - total_expected
    
    print(f"Total Expected: ¥{total_expected:.2f}")
    print(f"Total Claimed:  ¥{total_claimed:.2f}")
    print(f"Total Variance: ¥{total_variance:.2f}")

if __name__ == '__main__':
    test_invoice_948921914()
