#!/usr/bin/env python3

import sqlite3
import math
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP

class FedExUnifiedAudit:
    """
    Unified FedEx Audit System
    Can handle both single AWB audits and batch invoice processing
    Uses the same detailed logic for consistency
    """
    
    def __init__(self, db_path='fedex_audit.db'):
        self.db_path = db_path
        
        # Standard fuel surcharge rate (25.5%)
        self.fuel_surcharge_rate = 0.255
        
        # VAT rate
        self.vat_rate = 0.06  # 6%
    
    def get_zone_mapping(self, origin_country, dest_country):
        """Get zone mapping for country pair using DB view when available; fallback to safe heuristics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        origin_raw = (origin_country or '').strip()
        dest_raw = (dest_country or '').strip()
        origin_cc = origin_raw.upper()
        dest_cc = dest_raw.upper()

        try:
            # Map ISO country codes to names used in fedex_zone_matrix (origin name column)
            iso_to_name = {
                'US': 'United States, PR', 'PR': 'United States, PR',
                'HK': 'Hong Kong', 'CN': 'China', 'JP': 'Japan', 'KR': 'South Korea',
                'IT': 'Italy', 'IE': 'Ireland', 'DE': 'Germany', 'FR': 'France', 'GB': 'United Kingdom',
                'UK': 'United Kingdom', 'NL': 'Netherlands', 'BE': 'Belgium', 'ES': 'Spain', 'PT': 'Portugal',
                'CH': 'Switzerland', 'AT': 'Austria', 'SE': 'Sweden', 'NO': 'Norway', 'DK': 'Denmark',
                'FI': 'Finland', 'PL': 'Poland', 'CZ': 'Czech Republic', 'SK': 'Slovakia', 'HU': 'Hungary',
                'RO': 'Romania', 'BG': 'Bulgaria', 'GR': 'Greece', 'LT': 'Lithuania', 'LV': 'Latvia',
                'EE': 'Estonia', 'SG': 'Singapore', 'MY': 'Malaysia', 'TH': 'Thailand', 'TW': 'Taiwan',
                'PH': 'Philippines', 'VN': 'Vietnam', 'AE': 'United Arab Emirates', 'SA': 'Saudi Arabia',
                'IN': 'India', 'MX': 'Mexico', 'CA': 'Canada', 'AU': 'Australia', 'NZ': 'New Zealand'
            }
            full_origin = iso_to_name.get(origin_cc, origin_raw)

            # 1) Prefer the DB view if present (fedex_zone_lookup: origin_country name + destination_country ISO)
            try:
                cursor.execute('''
                    SELECT zone_letter FROM fedex_zone_lookup
                    WHERE origin_country = ? AND destination_country = ?
                    LIMIT 1
                ''', (full_origin, dest_cc))
                row = cursor.fetchone()
                if row and row[0]:
                    return row[0]
            except sqlite3.Error:
                # View missing or schema mismatch; continue to fallback
                pass

            # 2) Fallback to direct matrix using destination region names when obvious (e.g., CN -> 'China')
            dest_region_guess = {
                'CN': 'China', 'HK': 'China',
                'JP': 'NPAC', 'KR': 'NPAC',
                'SG': 'Asia One', 'MY': 'Asia One', 'TH': 'Asia One', 'TW': 'Asia One',
                'AU': 'Asia Two', 'NZ': 'Asia Two', 'ID': 'Asia Two', 'PH': 'Asia Two', 'VN': 'Asia Two',
                'US': 'US, AK, HI, PR', 'PR': 'US, AK, HI, PR', 'CA': 'Canada',
                'AE': 'Middle East', 'SA': 'IQ, AF, SA', 'IN': 'India Sub.'
            }.get(dest_cc, None)

            if dest_region_guess:
                cursor.execute('''
                    SELECT zone_letter FROM fedex_zone_matrix
                    WHERE origin_country = ? AND destination_region = ?
                    LIMIT 1
                ''', (full_origin, dest_region_guess))
                result = cursor.fetchone()
                if result and result[0]:
                    return result[0]

            # 3) Last-resort hardcoded pairs for common routes
            zone_map = {
                ('US', 'CN'): 'F', ('United States, PR', 'China'): 'F',
                ('HK', 'CN'): 'A', ('Hong Kong', 'China'): 'A',
                ('JP', 'CN'): 'B', ('Japan', 'China'): 'B',
                ('IT', 'CN'): 'L', ('Italy', 'China'): 'L',
                ('IE', 'CN'): 'L', ('Ireland', 'China'): 'L'
            }
            zone = zone_map.get((origin_cc, dest_cc)) or zone_map.get((full_origin, 'China'))
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

    def _normalize_service(self, service_type: str):
        s = (service_type or '').upper()
        if 'ECONOMY' in s or s == 'IE':
            return 'ECONOMY_EXPRESS', ('IE', 'IEKG')
        # default to Priority
        return 'PRIORITY_EXPRESS', ('IP', 'IPKG')

    def get_fedex_rate(self, chargeable_weight, zone, service_type='PRIORITY_EXPRESS'):
        """Get FedEx rate for weight and zone (service aware)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            norm_service, (fixed_rate_type, perkg_rate_type) = self._normalize_service(service_type)
            # Round to nearest 0.5 per FedEx rules for <= 21kg
            cw = chargeable_weight
            if cw <= 21:
                cw = (int(cw * 2 + 0.0001) / 2.0)
            
            # For packages <= 20.5kg, use IP rates
            if cw <= 20.5:
                cursor.execute('''
                    SELECT rate_usd FROM fedex_rate_cards
                    WHERE service_type = ? AND zone_code = ? 
                    AND weight_from = ? AND weight_to = ?
                    AND rate_type = ?
                    LIMIT 1
                ''', (norm_service, zone, cw, cw, fixed_rate_type))
                
                result = cursor.fetchone()
                if result:
                    return {
                        'rate_usd': float(result[0]),
                        'rate_type': fixed_rate_type,
                        'is_per_kg': False
                    }
            
            # For heavyweight packages (>20kg), use per-kg IPKG rates
            if cw > 20:
                cursor.execute('''
                    SELECT rate_usd FROM fedex_rate_cards
                    WHERE service_type = ? AND zone_code = ? 
                    AND rate_type = ?
                    LIMIT 1
                ''', (norm_service, zone, perkg_rate_type))
                
                result = cursor.fetchone()
                if result:
                    rate_per_kg = float(result[0])
                    total_rate = rate_per_kg * cw
                    return {
                        'rate_usd': total_rate,
                        'rate_type': perkg_rate_type,
                        'rate_per_kg': rate_per_kg,
                        'is_per_kg': True
                    }
            
            return None
            
        finally:
            conn.close()

    def audit_single_awb(self, invoice_no, awb_number, verbose=False):
        """
        Audit a single AWB - core audit logic used by both single and batch processing
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get AWB details from database
            cursor.execute('''
                SELECT awb_number, origin_country, dest_country, actual_weight_kg,
                       chargeable_weight_kg, total_awb_amount_cny, service_type, exchange_rate,
                       service_abbrev
                FROM fedex_invoices 
                WHERE invoice_no = ? AND awb_number = ?
            ''', (invoice_no, awb_number))
            
            awb_data = cursor.fetchone()
            if not awb_data:
                return {'success': False, 'error': f'AWB {awb_number} not found in invoice {invoice_no}'}
            
            awb_num, origin, dest, actual_weight, chargeable_weight_db, claimed_cny, service_type, exchange_rate, service_abbrev = awb_data
            
            if verbose:
                print(f"\n🔍 AUDITING AWB: {awb_num}")
                print(f"📍 Route: {origin} → {dest}")
                print(f"⚖️  Actual Weight: {actual_weight}kg")
                print(f"💰 Claimed Amount: ¥{claimed_cny:.2f}")
            
            # Step 1: Zone mapping
            zone = self.get_zone_mapping(origin, dest)
            if verbose:
                print(f"📍 Zone: {origin} → {dest} = Zone {zone}")
            
            # Step 2: Determine chargeable weight (prefer DB chargeable weight)
            calc_weight = chargeable_weight_db if (chargeable_weight_db and chargeable_weight_db > 0) else actual_weight
            chargeable_weight = self.round_weight_for_billing(calc_weight)
            if verbose:
                weight_rule = ">21kg → full kg" if calc_weight > 21 else "≤21kg → 0.5kg increment"
                print(f"⚖️  Weight: {calc_weight}kg → {chargeable_weight}kg ({weight_rule})")
            
            # Step 3: Rate lookup (use actual service type from invoice)
            # Prefer concise service abbreviation (e.g., IE/IP) if available for normalization
            effective_service = (service_abbrev or service_type or '').strip()
            rate_info = self.get_fedex_rate(chargeable_weight, zone, service_type=effective_service)
            if not rate_info:
                norm_service, (fixed_rt, perkg_rt) = self._normalize_service(effective_service)
                return {
                    'success': False,
                    'error': f'No rate found for {chargeable_weight}kg in zone {zone} (service {effective_service} → {norm_service}, try {fixed_rt}/{perkg_rt})',
                    'awb_number': awb_num,
                    'zone': zone,
                    'chargeable_weight': chargeable_weight,
                    'normalized_service': norm_service,
                    'expected_rate_types': [fixed_rt, perkg_rt]
                }
            
            base_cost_usd = rate_info['rate_usd']
            if verbose:
                if rate_info['is_per_kg']:
                    print(f"💲 Rate: ${rate_info['rate_per_kg']:.2f}/kg × {chargeable_weight}kg = ${base_cost_usd:.2f}")
                else:
                    print(f"💲 Rate: ${base_cost_usd:.2f} (fixed)")
            
            # Step 4: Fuel surcharge
            fuel_surcharge_usd = base_cost_usd * self.fuel_surcharge_rate
            subtotal_usd = base_cost_usd + fuel_surcharge_usd
            if verbose:
                print(f"⛽ Fuel Surcharge: ${fuel_surcharge_usd:.2f} (25.5%)")
                print(f"💲 Subtotal USD: ${subtotal_usd:.2f}")
            
            # Step 5: Convert to CNY
            subtotal_cny = subtotal_usd * exchange_rate
            if verbose:
                print(f"💱 CNY Conversion: ${subtotal_usd:.2f} × {exchange_rate} = ¥{subtotal_cny:.2f}")
            
            # Step 6: VAT
            vat_cny = subtotal_cny * self.vat_rate
            total_expected_cny = subtotal_cny + vat_cny
            if verbose:
                print(f"🧾 VAT: ¥{vat_cny:.2f} (6%)")
                print(f"💰 Total Expected: ¥{total_expected_cny:.2f}")
            
            # Step 7: Variance analysis
            variance_cny = claimed_cny - total_expected_cny
            variance_percent = (variance_cny / total_expected_cny) * 100 if total_expected_cny > 0 else 0
            
            if abs(variance_percent) <= 2:
                status = "PASS"
            elif variance_cny > 0:
                status = "OVERCHARGE"
            else:
                status = "UNDERCHARGE"
            
            if verbose:
                print(f"📊 Expected: ¥{total_expected_cny:.2f}")
                print(f"📊 Claimed:  ¥{claimed_cny:.2f}")
                print(f"📊 Variance: ¥{variance_cny:.2f} ({variance_percent:+.1f}%)")
                print(f"📊 Status: {status}")
            
            return {
                'success': True,
                'awb_number': awb_num,
                'invoice_no': invoice_no,
                'origin_country': origin,
                'dest_country': dest,
                'zone': zone,
                'actual_weight_kg': actual_weight,
                'chargeable_weight_kg': chargeable_weight,
                'base_cost_usd': base_cost_usd,
                'fuel_surcharge_usd': fuel_surcharge_usd,
                'subtotal_usd': subtotal_usd,
                'exchange_rate': exchange_rate,
                'subtotal_cny': subtotal_cny,
                'vat_cny': vat_cny,
                'total_expected_cny': total_expected_cny,
                'claimed_cny': claimed_cny,
                'variance_cny': variance_cny,
                'variance_percent': variance_percent,
                'audit_status': status,
                'rate_type': rate_info['rate_type']
            }
            
        finally:
            conn.close()

    def audit_invoice(self, invoice_no, verbose=False):
        """
        Audit all AWBs in an invoice - batch processing using single AWB logic
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get all AWBs for this invoice
            cursor.execute('''
                SELECT awb_number FROM fedex_invoices 
                WHERE invoice_no = ?
                ORDER BY awb_number
            ''', (invoice_no,))
            
            awb_numbers = [row[0] for row in cursor.fetchall()]
            
            if not awb_numbers:
                return {
                    'success': False,
                    'error': f'No AWBs found for invoice {invoice_no}'
                }
            
            if verbose:
                print(f"\n📋 BATCH AUDIT - Invoice {invoice_no}")
                print(f"📦 Found {len(awb_numbers)} AWBs")
                print("=" * 60)
            
            # Audit each AWB using the same single AWB logic
            awb_results = []
            total_expected_cny = 0
            total_claimed_cny = 0
            pass_count = 0
            overcharge_count = 0
            undercharge_count = 0
            
            for awb_number in awb_numbers:
                result = self.audit_single_awb(invoice_no, awb_number, verbose=verbose)
                
                if result['success']:
                    awb_results.append(result)
                    total_expected_cny += result['total_expected_cny']
                    total_claimed_cny += result['claimed_cny']
                    
                    if result['audit_status'] == 'PASS':
                        pass_count += 1
                    elif result['audit_status'] == 'OVERCHARGE':
                        overcharge_count += 1
                    elif result['audit_status'] == 'UNDERCHARGE':
                        undercharge_count += 1
                else:
                    if verbose:
                        print(f"❌ Failed to audit AWB {awb_number}: {result['error']}")
            
            total_variance_cny = total_claimed_cny - total_expected_cny
            
            # Determine overall status
            if overcharge_count > 0 or undercharge_count > 0:
                if abs(total_variance_cny) / total_expected_cny > 0.02:  # >2% variance
                    overall_status = "OVERCHARGE" if total_variance_cny > 0 else "UNDERCHARGE"
                else:
                    overall_status = "PASS"
            else:
                overall_status = "PASS"
            
            if verbose:
                print(f"\n📋 INVOICE SUMMARY")
                print("=" * 40)
                print(f"Total AWBs: {len(awb_results)}")
                print(f"Expected Total: ¥{total_expected_cny:.2f}")
                print(f"Claimed Total:  ¥{total_claimed_cny:.2f}")
                print(f"Total Variance: ¥{total_variance_cny:.2f}")
                print(f"Overall Status: {overall_status}")
                print(f"Pass: {pass_count}, Overcharge: {overcharge_count}, Undercharge: {undercharge_count}")
            
            return {
                'success': True,
                'invoice_no': invoice_no,
                'awb_count': len(awb_results),
                'awb_results': awb_results,
                'total_expected_cny': total_expected_cny,
                'total_claimed_cny': total_claimed_cny,
                'total_variance_cny': total_variance_cny,
                'overall_status': overall_status,
                'summary': {
                    'pass_count': pass_count,
                    'overcharge_count': overcharge_count,
                    'undercharge_count': undercharge_count
                }
            }
            
        finally:
            conn.close()

    def update_audit_results(self, invoice_result):
        """Update database with audit results"""
        if not invoice_result['success']:
            return False
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Update each AWB with audit results
            for awb_result in invoice_result['awb_results']:
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
                    awb_result['total_expected_cny'],
                    awb_result['variance_cny'],
                    datetime.now().isoformat(),
                    f"Zone {awb_result['zone']}, {awb_result['chargeable_weight_kg']}kg, Rate: ${awb_result['base_cost_usd']:.2f}",
                    awb_result['invoice_no'],
                    awb_result['awb_number']
                ))
            
            conn.commit()
            return True
            
        except Exception as e:
            print(f"Error updating audit results: {e}")
            return False
        finally:
            conn.close()
    
    def audit_batch_invoices(self, invoice_list, verbose=False):
        """
        Audit multiple invoices in batch
        
        Args:
            invoice_list: List of invoice numbers to audit
            verbose: Whether to print detailed output
            
        Returns:
            Dict with batch audit results
        """
        print(f"DEBUG: audit_batch_invoices called with {len(invoice_list)} invoices: {invoice_list}")
        
        if verbose:
            print(f"🚀 Starting batch audit for {len(invoice_list)} invoices")
        
        results = {
            'success': True,
            'total_invoices': len(invoice_list),
            'successful_audits': 0,
            'failed_audits': 0,
            'errors': [],
            'invoice_results': [],
            'total_variance': 0
        }
        
        for invoice_no in invoice_list:
            try:
                print(f"DEBUG: Processing invoice {invoice_no}")
                
                if verbose:
                    print(f"\n📋 Auditing invoice: {invoice_no}")
                
                # Audit this invoice
                print(f"DEBUG: Calling audit_invoice for {invoice_no}")
                invoice_result = self.audit_invoice(invoice_no, verbose=verbose)
                print(f"DEBUG: audit_invoice returned: {invoice_result.get('success', False)}")
                
                if invoice_result['success']:
                    # Update database with results
                    print(f"DEBUG: Calling update_audit_results for {invoice_no}")
                    if self.update_audit_results(invoice_result):
                        results['successful_audits'] += 1
                        results['total_variance'] += invoice_result.get('total_variance_cny', 0)
                        results['invoice_results'].append({
                            'invoice_no': invoice_no,
                            'status': 'success',
                            'awb_count': invoice_result.get('awb_count', 0),
                            'variance': invoice_result.get('total_variance_cny', 0)
                        })
                        
                        if verbose:
                            print(f"✅ Successfully audited invoice {invoice_no}")
                    else:
                        results['failed_audits'] += 1
                        error_msg = f"Failed to save audit results for invoice {invoice_no}"
                        results['errors'].append(error_msg)
                        results['invoice_results'].append({
                            'invoice_no': invoice_no,
                            'status': 'failed',
                            'error': error_msg
                        })
                        
                        if verbose:
                            print(f"❌ {error_msg}")
                else:
                    results['failed_audits'] += 1
                    error_msg = f"Audit failed for invoice {invoice_no}: {invoice_result.get('error', 'Unknown error')}"
                    results['errors'].append(error_msg)
                    results['invoice_results'].append({
                        'invoice_no': invoice_no,
                        'status': 'failed',
                        'error': error_msg
                    })
                    
                    if verbose:
                        print(f"❌ {error_msg}")
                        
            except Exception as e:
                results['failed_audits'] += 1
                error_msg = f"Exception auditing invoice {invoice_no}: {str(e)}"
                results['errors'].append(error_msg)
                results['invoice_results'].append({
                    'invoice_no': invoice_no,
                    'status': 'error',
                    'error': error_msg
                })
                
                print(f"DEBUG: Exception in audit_batch_invoices for {invoice_no}: {str(e)}")
                import traceback
                traceback.print_exc()
                
                if verbose:
                    print(f"💥 {error_msg}")
        
        # Set overall success status
        results['success'] = results['failed_audits'] == 0
        
        if verbose:
            print(f"\n📊 Batch audit completed:")
            print(f"   ✅ Successful: {results['successful_audits']}")
            print(f"   ❌ Failed: {results['failed_audits']}")
            print(f"   💰 Total variance: ¥{results['total_variance']:.2f}")
        
        return results

def test_unified_audit():
    """Test both single AWB and batch invoice audit"""
    auditor = FedExUnifiedAudit()
    
    print("🧪 TESTING UNIFIED FEDEX AUDIT SYSTEM")
    print("=" * 80)
    
    # Test 1: Single AWB audit
    print("\n🔍 TEST 1: Single AWB Audit")
    print("-" * 40)
    single_result = auditor.audit_single_awb('948921914', '770960689095', verbose=True)
    
    # Test 2: Full invoice batch audit
    print("\n📋 TEST 2: Batch Invoice Audit")
    print("-" * 40)
    batch_result = auditor.audit_invoice('948921914', verbose=True)
    
    # Test 3: Update database
    if batch_result['success']:
        print(f"\n💾 TEST 3: Database Update")
        print("-" * 40)
        if auditor.update_audit_results(batch_result):
            print("✅ Audit results saved to database")
        else:
            print("❌ Failed to save audit results")

if __name__ == '__main__':
    test_unified_audit()
