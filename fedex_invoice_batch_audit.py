#!/usr/bin/env python3

import sqlite3
import math
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

class FedExInvoiceBatchAudit:
    """
    FedEx Invoice Batch Audit System
    Implements the exact audit logic based on invoice 951109588 examples
    """
    
    def __init__(self, db_path='fedex_audit.db'):
        self.db_path = db_path
        
        # Zone mappings based on your examples
        self.zone_mappings = {
            ('JP', 'CN'): 'B',  # Japan to China = Zone B
            ('US', 'CN'): 'F',  # US to China = Zone F
        }
        
        # Standard fuel surcharge rate from examples (~25.5%)
        self.fuel_surcharge_rate = 0.255
        
        # VAT rate
        self.vat_rate = 0.06  # 6%
    
    def round_weight_for_billing(self, weight_kg):
        """Round weight according to FedEx billing rules"""
        if weight_kg > 20:
            # For heavyweight packages, round up to next full kg
            return math.ceil(weight_kg)
        else:
            # For regular packages, round up to next 0.5kg increment  
            return math.ceil(weight_kg * 2) / 2
    
    def get_zone_mapping(self, origin_country, dest_country):
        """Get zone mapping for origin-destination pair"""
        key = (origin_country, dest_country)
        if key in self.zone_mappings:
            return self.zone_mappings[key]
        
        # Try database lookup as fallback
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT zone_letter FROM fedex_zone_matrix
                WHERE origin_country = ? AND destination_region LIKE ?
                LIMIT 1
            ''', (origin_country, f'%{dest_country}%'))
            
            result = cursor.fetchone()
            if result:
                return result[0]
        finally:
            conn.close()
        
        return 'A'  # Default zone
    
    def get_fedex_rate(self, weight_kg, zone, service_type='PRIORITY_EXPRESS'):
        """Get FedEx rate for weight and zone"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            chargeable_weight = self.round_weight_for_billing(weight_kg)
            
            # For packages <= 20.5kg, use fixed IP rates
            if chargeable_weight <= 20.5:
                cursor.execute('''
                    SELECT rate_usd, rate_type FROM fedex_rate_cards
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
                        'chargeable_weight': chargeable_weight,
                        'is_per_kg': False
                    }
            
            # For heavyweight packages (>20kg), use per-kg IPKG rates
            if chargeable_weight > 20:
                cursor.execute('''
                    SELECT rate_usd, rate_type FROM fedex_rate_cards
                    WHERE service_type = ? AND zone_code = ? 
                    AND weight_from <= ? AND weight_to >= ?
                    AND rate_type = 'IPKG'
                    LIMIT 1
                ''', (service_type, zone, chargeable_weight, chargeable_weight))
                
                result = cursor.fetchone()
                if result:
                    rate_per_kg = float(result[0])
                    total_rate = rate_per_kg * chargeable_weight
                    return {
                        'rate_usd': total_rate,
                        'rate_type': 'IPKG',
                        'rate_per_kg': rate_per_kg,
                        'chargeable_weight': chargeable_weight,
                        'is_per_kg': True
                    }
            
            return None
            
        finally:
            conn.close()
    
    def audit_single_awb(self, invoice_no, awb_number):
        """Audit a single AWB from the database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get AWB details using correct column names
            cursor.execute('''
                SELECT awb_number, origin_country, dest_country, 
                       actual_weight_kg, rated_amount_cny, exchange_rate, service_type,
                       total_awb_amount_cny
                FROM fedex_invoices 
                WHERE invoice_no = ? AND awb_number = ?
            ''', (invoice_no, awb_number))
            
            awb_data = cursor.fetchone()
            if not awb_data:
                return {'success': False, 'error': f'AWB {awb_number} not found in invoice {invoice_no}'}
            
            awb_num, origin_country, dest_country, weight_kg, rated_amount_cny, exchange_rate, service_type, total_claimed_cny = awb_data
            
            # Step 1: Get zone mapping
            zone = self.get_zone_mapping(origin_country, dest_country)
            
            # Step 2: Get FedEx rate
            rate_info = self.get_fedex_rate(weight_kg, zone, service_type or 'PRIORITY_EXPRESS')
            
            if not rate_info:
                return {
                    'success': False,
                    'error': f'No rate found for {weight_kg}kg to zone {zone}'
                }
            
            # Step 3: Calculate costs
            base_cost_usd = rate_info['rate_usd']
            base_cost_local = base_cost_usd * exchange_rate
            
            # Step 4: Calculate fuel surcharge
            fuel_surcharge_local = base_cost_local * self.fuel_surcharge_rate
            
            # Step 5: Calculate subtotal and VAT
            subtotal_local = base_cost_local + fuel_surcharge_local
            vat_local = subtotal_local * self.vat_rate
            total_expected_local = subtotal_local + vat_local
            
            # Step 6: Compare with claimed amount
            claimed_local = total_claimed_cny
            variance_local = total_expected_local - claimed_local
            variance_percent = (variance_local / claimed_local * 100) if claimed_local > 0 else 0
            
            # Determine audit status
            tolerance = 5.0  # 5 CNY tolerance
            if abs(variance_local) <= tolerance:
                audit_status = 'PASS'
            elif variance_local > 0:
                audit_status = 'UNDERCHARGE'  # Customer was undercharged (we expect more)
            else:
                audit_status = 'OVERCHARGE'   # Customer was overcharged (we expect less)
            
            return {
                'success': True,
                'invoice_no': invoice_no,
                'awb_number': awb_number,
                'origin_country': origin_country,
                'dest_country': dest_country,
                'actual_weight_kg': weight_kg,
                'chargeable_weight_kg': rate_info['chargeable_weight'],
                'zone': zone,
                'service_type': service_type,
                'rate_type': rate_info['rate_type'],
                'base_rate_usd': base_cost_usd,
                'exchange_rate': exchange_rate,
                'base_cost_local': round(base_cost_local, 2),
                'fuel_surcharge_local': round(fuel_surcharge_local, 2),
                'subtotal_local': round(subtotal_local, 2),
                'vat_local': round(vat_local, 2),
                'total_expected_local': round(total_expected_local, 2),
                'claimed_cost_local': round(claimed_local, 2),
                'variance_local': round(variance_local, 2),
                'variance_percent': round(variance_percent, 2),
                'audit_status': audit_status,
                'currency': 'CNY'
            }
            
        finally:
            conn.close()
    
    def audit_invoice(self, invoice_no):
        """Audit all AWBs in an invoice"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get all AWBs for this invoice
            cursor.execute('''
                SELECT DISTINCT awb_number 
                FROM fedex_invoices 
                WHERE invoice_no = ?
                ORDER BY awb_number
            ''', (invoice_no,))
            
            awb_numbers = [row[0] for row in cursor.fetchall()]
            
            if not awb_numbers:
                return {
                    'success': False,
                    'error': f'No AWBs found for invoice {invoice_no}'
                }
            
            # Audit each AWB
            awb_results = []
            total_expected = 0
            total_claimed = 0
            
            for awb_number in awb_numbers:
                result = self.audit_single_awb(invoice_no, awb_number)
                if result['success']:
                    awb_results.append(result)
                    total_expected += result['total_expected_local']
                    total_claimed += result['claimed_cost_local']
                else:
                    awb_results.append(result)
            
            # Calculate invoice-level summary
            total_variance = total_expected - total_claimed
            
            # Determine overall status
            pass_count = sum(1 for r in awb_results if r.get('audit_status') == 'PASS')
            overcharge_count = sum(1 for r in awb_results if r.get('audit_status') == 'OVERCHARGE')
            undercharge_count = sum(1 for r in awb_results if r.get('audit_status') == 'UNDERCHARGE')
            
            if overcharge_count > 0:
                overall_status = 'OVERCHARGE'
            elif undercharge_count > 0:
                overall_status = 'UNDERCHARGE'
            else:
                overall_status = 'PASS'
            
            return {
                'success': True,
                'invoice_no': invoice_no,
                'awb_count': len(awb_numbers),
                'awb_results': awb_results,
                'total_expected_local': round(total_expected, 2),
                'total_claimed_local': round(total_claimed, 2),
                'total_variance_local': round(total_variance, 2),
                'overall_status': overall_status,
                'summary': {
                    'pass_count': pass_count,
                    'overcharge_count': overcharge_count,
                    'undercharge_count': undercharge_count
                }
            }
            
        finally:
            conn.close()
    
    def update_audit_results(self, audit_result):
        """Update the database with audit results"""
        if not audit_result['success']:
            return False
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Check if audit results columns exist, if not add them
            try:
                cursor.execute('ALTER TABLE fedex_invoices ADD COLUMN audit_status TEXT')
            except:
                pass  # Column already exists
            
            try:
                cursor.execute('ALTER TABLE fedex_invoices ADD COLUMN expected_cost_cny REAL')
            except:
                pass
            
            try:
                cursor.execute('ALTER TABLE fedex_invoices ADD COLUMN variance_cny REAL')
            except:
                pass
            
            try:
                cursor.execute('ALTER TABLE fedex_invoices ADD COLUMN audit_timestamp TEXT')
            except:
                pass
            
            try:
                cursor.execute('ALTER TABLE fedex_invoices ADD COLUMN audit_details TEXT')
            except:
                pass
            
            # Update each AWB result
            for awb_result in audit_result['awb_results']:
                if awb_result['success']:
                    cursor.execute('''
                        UPDATE fedex_invoices 
                        SET audit_status = ?,
                            expected_cost_cny = ?,
                            variance_cny = ?,
                            audit_timestamp = ?,
                            audit_details = ?
                        WHERE invoice_no = ? AND awb_number = ?
                    ''', (
                        awb_result['audit_status'],
                        awb_result['total_expected_local'],
                        awb_result['variance_local'],
                        datetime.now().isoformat(),
                        f"Zone {awb_result['zone']}, {awb_result['chargeable_weight_kg']}kg, {awb_result['rate_type']}",
                        audit_result['invoice_no'],
                        awb_result['awb_number']
                    ))
            
            conn.commit()
            return True
            
        except Exception as e:
            conn.rollback()
            print(f"Error updating audit results: {e}")
            return False
        finally:
            conn.close()

def test_invoice_951109588():
    """Test the batch auditor with available invoices"""
    auditor = FedExInvoiceBatchAudit()
    
    print("🧪 Testing FedEx Batch Audit System")
    print("=" * 60)
    
    # First check what invoices we have
    conn = sqlite3.connect('fedex_audit.db')
    cursor = conn.cursor()
    
    cursor.execute('SELECT DISTINCT invoice_no FROM fedex_invoices LIMIT 5')
    available_invoices = [row[0] for row in cursor.fetchall()]
    
    print(f"� Available invoices: {available_invoices}")
    
    if not available_invoices:
        print("❌ No invoices found in database")
        return
    
    # Test with the first available invoice
    test_invoice = available_invoices[0]
    print(f"\n🧪 Testing with invoice: {test_invoice}")
    
    # Get AWBs for this invoice
    cursor.execute('SELECT awb_number, origin_country, dest_country, actual_weight_kg FROM fedex_invoices WHERE invoice_no = ? LIMIT 2', (test_invoice,))
    awb_data = cursor.fetchall()
    
    conn.close()
    
    # Test individual AWBs
    for awb_number, origin, dest, weight in awb_data:
        print(f"\n📦 AWB {awb_number} ({origin} to {dest}, {weight}kg)")
        print("-" * 50)
        
        result = auditor.audit_single_awb(test_invoice, awb_number)
        
        if result['success']:
            print(f"✅ Zone: {result['zone']}")
            print(f"✅ Weight: {result['actual_weight_kg']}kg → {result['chargeable_weight_kg']}kg")
            print(f"✅ Expected: {result['total_expected_local']:.2f} {result['currency']}")
            print(f"✅ Claimed: {result['claimed_cost_local']:.2f} {result['currency']}")
            print(f"✅ Variance: {result['variance_local']:.2f} {result['currency']} ({result['variance_percent']:.1f}%)")
            print(f"✅ Status: {result['audit_status']}")
        else:
            print(f"❌ Error: {result['error']}")
    
    # Test full invoice audit
    print(f"\n📋 Full Invoice Audit: {test_invoice}")
    print("=" * 50)
    
    invoice_result = auditor.audit_invoice(test_invoice)
    
    if invoice_result['success']:
        print(f"✅ Total AWBs: {invoice_result['awb_count']}")
        print(f"✅ Expected Total: {invoice_result['total_expected_local']:.2f} CNY")
        print(f"✅ Claimed Total: {invoice_result['total_claimed_local']:.2f} CNY")
        print(f"✅ Total Variance: {invoice_result['total_variance_local']:.2f} CNY")
        print(f"✅ Overall Status: {invoice_result['overall_status']}")
        print(f"✅ Summary: {invoice_result['summary']}")
        
        # Update database with results
        if auditor.update_audit_results(invoice_result):
            print("✅ Audit results saved to database")
        else:
            print("❌ Failed to save audit results")
    else:
        print(f"❌ Invoice audit failed: {invoice_result['error']}")

if __name__ == "__main__":
    test_invoice_951109588()
