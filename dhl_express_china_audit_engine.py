"""
DHL Express China Audit Engine

This module provides audit functionality for DHL Express China invoices.
All invoices are inbound (import) to SZV (China) and billed in CNY.

Key Features:
1. Weight-based rate verification using zone mappings
2. Service charge matching for "other costs"
3. Fuel surcharge pass-through handling
4. BCU charge validation against rate cards

Author: System
Date: August 19, 2025
"""

import sqlite3
import logging
import math
from datetime import datetime
from typing import Dict, List, Optional, Any
import json
from decimal import Decimal

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DHLExpressChinaAuditEngine:
    """
    Audit engine for DHL Express China invoices.
    Handles import shipments to SZV (China) with CNY currency.
    """
    
    def __init__(self, db_path: str = 'dhl_audit.db'):
        """Initialize the audit engine with database connection."""
        self.db_path = db_path
        self.audit_tolerance = 0.01  # 1% tolerance for rate matching
        
    def get_db_connection(self) -> sqlite3.Connection:
        """Get database connection with row factory."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn
    
    def get_country_zone(
        self, origin_code: str, service_type: str = 'Import'
    ) -> Optional[str]:
        """
        Get the zone for a country code for import/export operations.
        
        Args:
            origin_code: country or city code (e.g., 'US', 'SYD')
            service_type: 'Import' or 'Export' (default: 'Import')
            
        Returns:
            Zone number as string or None if not found
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            if service_type.lower() == 'import':
                table_name = 'dhl_express_import_zones'
            else:
                table_name = 'dhl_express_export_zones'
            
            # Map common city codes to country codes
            city_to_country = {
                'SYD': 'AU',  # Sydney -> Australia
                'MEL': 'AU',  # Melbourne -> Australia
                'SIN': 'SG',  # Singapore -> Singapore
                'HKG': 'HK',  # Hong Kong -> Hong Kong
                'EIN': 'NL',  # Eindhoven -> Netherlands
                'AMS': 'NL',  # Amsterdam -> Netherlands
                'LAX': 'US',  # Los Angeles -> USA
                'JFK': 'US',  # New York -> USA
                'LHR': 'GB',  # London -> UK
                'CDG': 'FR',  # Paris -> France
                'FRA': 'DE',  # Frankfurt -> Germany
                'NRT': 'JP',  # Tokyo -> Japan
                'BOM': 'IN',  # Mumbai -> India
                'DEL': 'IN',  # Delhi -> India
                'CCU': 'IN',  # Kolkata -> India
                'MAA': 'IN',  # Chennai -> India
            }
            
            # Check if it's a known city code
            country_code = city_to_country.get(
                origin_code.upper(), origin_code
            )
            
            # Try exact match first
            cursor.execute(
                f"""
                SELECT zone FROM {table_name}
                WHERE UPPER(country_code) = UPPER(?)
                LIMIT 1
                """,
                (country_code,),
            )
            
            result = cursor.fetchone()
            if result:
                return result['zone']
            
            # Try original code if mapping didn't work
            if country_code != origin_code:
                cursor.execute(
                    f"""
                    SELECT zone FROM {table_name}
                    WHERE UPPER(country_code) = UPPER(?)
                    LIMIT 1
                    """,
                    (origin_code,),
                )
                
                result = cursor.fetchone()
                if result:
                    return result['zone']
            
            # Try partial match on country name
            cursor.execute(
                f"""
                SELECT zone FROM {table_name}
                WHERE UPPER(country_name) LIKE UPPER(?)
                LIMIT 1
                """,
                (f"%{origin_code}%",),
            )
            
            result = cursor.fetchone()
            return result['zone'] if result else None
            
        except Exception as e:
            logger.error(f"Error getting country zone: {e}")
            return None
        finally:
            conn.close()
    
    def get_rate_for_weight_zone(
        self,
        weight_kg: float,
        zone: str,
        service_type: str = 'Import',
        rate_section: str = 'Non-documents',
    ) -> Optional[Decimal]:
        """Return the rate for a given weight and zone.

        Uses per-kg multiplier rows for weights greater than 30kg.
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()

        try:
            zone_column = f'zone_{zone}'

            # Over 30kg: use multiplier (per-kg) rate when weight is in range
            # For weights over 30kg, round up to the next full kilogram
            if weight_kg > 30:
                rounded_weight_kg = math.ceil(weight_kg)
                cursor.execute(
                    f"""
                    SELECT {zone_column}
                    FROM dhl_express_rate_cards
                    WHERE service_type = ?
                      AND is_multiplier = 1
                      AND (weight_range_from IS NULL OR weight_range_from <= ?)
                      AND (weight_range_to IS NULL OR weight_range_to >= ?)
                    ORDER BY COALESCE(weight_range_from, 0) ASC
                    LIMIT 1
                    """,
                    (service_type, weight_kg, weight_kg),
                )
                row = cursor.fetchone()
                if row and row[0] is not None:
                    per_kg = Decimal(str(row[0]))
                    return per_kg * Decimal(str(rounded_weight_kg))

            # 0-30kg: exact bracket
            # Use proper weight bracket logic: weight_from <= weight < weight_to
            cursor.execute(
                f"""
                SELECT {zone_column}, weight_from, weight_to
                FROM dhl_express_rate_cards
                WHERE service_type = ? AND rate_section = ?
                  AND is_multiplier = 0
                  AND weight_from <= ? AND weight_to > ?
                  AND {zone_column} IS NOT NULL
                ORDER BY weight_from DESC
                LIMIT 1
                """,
                (service_type, rate_section, weight_kg, weight_kg),
            )
            row = cursor.fetchone()
            if row and row[0] is not None:
                return Decimal(str(row[0]))

            # Fallback: next higher bracket
            cursor.execute(
                f"""
                SELECT {zone_column}
                FROM dhl_express_rate_cards
                WHERE service_type = ? AND rate_section = ?
                  AND is_multiplier = 0
                  AND weight_from > ?
                  AND {zone_column} IS NOT NULL
                ORDER BY weight_from
                LIMIT 1
                """,
                (service_type, rate_section, weight_kg),
            )
            row = cursor.fetchone()
            if row and row[0] is not None:
                return Decimal(str(row[0]))

            return None

        except Exception as e:
            logger.error(f"Error getting rate for weight/zone: {e}")
            return None
        finally:
            conn.close()
    
    def find_matching_service_charges(
        self, amount: float
    ) -> List[Dict[str, Any]]:
        """Find service charges rows whose charge_amount ~= amount (±2%)."""
        conn = self.get_db_connection()
        cursor = conn.cursor()

        try:
            matches: List[Dict[str, Any]] = []
            tolerance = amount * 0.02  # 2% tolerance

            cursor.execute(
                """
                SELECT service_code, service_name, description, charge_type,
                       charge_amount, minimum_charge, percentage_rate
                FROM dhl_express_services_surcharges
                WHERE charge_amount IS NOT NULL
                  AND ABS(charge_amount - ?) <= ?
                ORDER BY ABS(charge_amount - ?)
                """,
                (amount, tolerance, amount),
            )

            for row in cursor.fetchall():
                matches.append(
                    {
                        'service_code': row['service_code'],
                        'service_name': row['service_name'],
                        'description': row['description'],
                        'charge_type': row['charge_type'],
                        'charge_amount': float(row['charge_amount']),
                        'minimum_charge': (
                            float(row['minimum_charge'])
                            if row['minimum_charge'] else None
                        ),
                        'percentage_rate': (
                            float(row['percentage_rate'])
                            if row['percentage_rate'] else None
                        ),
                        'match_difference': (
                            abs(float(row['charge_amount']) - amount)
                        ),
                    }
                )

            return matches

        except Exception as e:
            logger.error(f"Error finding matching service charges: {e}")
            return []
        finally:
            conn.close()
    
    def audit_invoice_line(
        self, invoice_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Audit a single invoice line item.
        
        Args:
            invoice_data: Dictionary containing invoice line data
            
        Returns:
            Audit result dictionary
        """
        try:
            # Extract key data
            awb = invoice_data.get('air_waybill', '')
            origin_code = invoice_data.get('origin_code', '')
            consignor_country = invoice_data.get('consignor_country', '')
            weight_kg = float(invoice_data.get('billed_weight_kg', 0))
            bcu_weight_charge = float(invoice_data.get('bcu_weight_charge', 0))
            bcu_other_charges = float(invoice_data.get('bcu_other_charges', 0))
            bcu_fuel_surcharges = float(
                invoice_data.get('bcu_fuel_surcharges', 0)
            )
            bcu_taxes_applicable = float(
                invoice_data.get('bcu_taxes_applicable', 0)
            )
            total_charge = float(invoice_data.get('bcu_total', 0))
            
            audit_result = {
                'awb': awb,
                'audit_status': 'pending',
                'weight_audit': {},
                'other_charges_audit': {},
                'fuel_surcharge_audit': {},
                'total_variance': 0,
                'comments': []
            }
            
            # 1. Zone Determination - Use consignor_country if available, fallback to origin_code
            country_for_zone = consignor_country if consignor_country else origin_code
            zone = self.get_country_zone(country_for_zone, 'Import')
            if not zone:
                audit_result['audit_status'] = 'error'
                audit_result['comments'].append(
                    f"Could not determine zone for country: {country_for_zone} (consignor_country: {consignor_country}, origin_code: {origin_code})"
                )
                return audit_result

            audit_result['zone_used'] = zone
            audit_result['comments'].append(
                f"✓ Country: {country_for_zone} → Zone {zone} (Import to SZV China)"
            )            # 2. Weight Charge Audit
            expected_weight_charge = self.get_rate_for_weight_zone(
                weight_kg,
                zone,
                'Import',
                'Non-documents',
            )
            if expected_weight_charge:
                weight_variance = (
                    bcu_weight_charge - float(expected_weight_charge)
                )
                weight_variance_pct = (
                    (weight_variance / float(expected_weight_charge)) * 100
                    if expected_weight_charge > 0
                    else 0
                )

                audit_result['weight_audit'] = {
                    'weight_kg': weight_kg,
                    'zone': zone,
                    'expected_charge': float(expected_weight_charge),
                    'actual_charge': bcu_weight_charge,
                    'variance': weight_variance,
                    'variance_percent': weight_variance_pct,
                    'status': (
                        'pass' if abs(weight_variance_pct) <= 5 else 'fail'
                    ),
                    'rate_card_type': 'DHL Express China Import Weight Rate Card',
                    'service_type': 'Non-documents Import'
                }
                
                # Enhanced weight charge comments
                audit_result['comments'].append(
                    f"📦 Weight: {weight_kg} kg → Zone {zone} Non-documents Import Rate"
                )
                audit_result['comments'].append(
                    f"💰 Rate Card: DHL Express China Import (Zone {zone})"
                )
                audit_result['comments'].append(
                    f"🔍 Expected: CNY {expected_weight_charge:.2f} | Actual: CNY {bcu_weight_charge:.2f} | Variance: {weight_variance_pct:+.1f}%"
                )
                
                if weight_kg > 30:
                    rounded_weight = math.ceil(weight_kg)
                    audit_result['comments'].append(
                        f"⚠️ Over-weight shipment ({weight_kg}kg > 30kg) - Rounded up to {rounded_weight}kg × per-kg rate"
                    )
                elif weight_kg <= 30:
                    audit_result['comments'].append(
                        f"📋 Standard weight band ({weight_kg}kg ≤ 30kg) - Fixed rate from rate card"
                    )
                
            else:
                audit_result['weight_audit']['status'] = 'error'
                audit_result['comments'].append(
                    (
                        "Could not find rate for weight "
                        f"{weight_kg}kg in zone {zone}"
                    )
                )
            
            # 3. Fuel Surcharge Audit (Pass-through)
            audit_result['fuel_surcharge_audit'] = {
                'amount': bcu_fuel_surcharges,
                'status': 'pass_through',
                'note': (
                    'Fuel surcharges are pass-through; no verification '
                    'required'
                ),
                'policy': 'DHL China passes fuel surcharges directly from origin'
            }
            if bcu_fuel_surcharges > 0:
                audit_result['comments'].append(
                    f"⛽ Fuel Surcharge: CNY {bcu_fuel_surcharges:.2f} (Pass-through - No audit required)"
                )
                audit_result['comments'].append(
                    f"📋 Policy: DHL China passes fuel surcharges directly from origin country"
                )
            else:
                audit_result['comments'].append(
                    f"⛽ Fuel Surcharge: CNY 0.00 (No fuel surcharge applied)"
                )
            
            # 4. Other Charges Audit
            if bcu_other_charges > 0:
                matching_services = self.find_matching_service_charges(
                    bcu_other_charges
                )
                audit_result['other_charges_audit'] = {
                    'amount': bcu_other_charges,
                    'matching_services': matching_services,
                    'match_count': len(matching_services),
                    'audit_method': 'Service charge lookup against DHL Express China service charge table'
                }
                
                if matching_services:
                    service_names = [
                        svc['service_name'] for svc in matching_services
                    ]
                    audit_result['comments'].append(
                        f"💼 Other Charges: CNY {bcu_other_charges:.2f} → Found {len(matching_services)} matching services"
                    )
                    audit_result['comments'].append(
                        f"✅ Matched Services: {', '.join(service_names)}"
                    )
                    audit_result['comments'].append(
                        f"📊 Lookup: DHL Express China Service Charge Table"
                    )
                else:
                    audit_result['comments'].append(
                        f"💼 Other Charges: CNY {bcu_other_charges:.2f} → ⚠️ NO MATCHING SERVICES FOUND"
                    )
                    audit_result['comments'].append(
                        f"🔍 Searched: DHL Express China Service Charge Table - No matches for CNY {bcu_other_charges:.2f}"
                    )
            else:
                audit_result['comments'].append(
                    f"💼 Other Charges: CNY 0.00 (No additional services charged)"
                )
            
            # 5. Tax Calculation (6% VAT on subtotal)
            subtotal_before_tax = (
                float(expected_weight_charge or 0)
                + bcu_fuel_surcharges
                + bcu_other_charges
            )
            expected_tax = subtotal_before_tax * 0.06
            expected_total = subtotal_before_tax + expected_tax
            
            audit_result['tax_audit'] = {
                'subtotal_before_tax': subtotal_before_tax,
                'expected_tax_rate': 0.06,
                'expected_tax': expected_tax,
                'actual_tax': bcu_taxes_applicable,
                'tax_variance': bcu_taxes_applicable - expected_tax,
                'status': 'pass' if abs(bcu_taxes_applicable - expected_tax) < 0.01 else 'variance'
            }
            
            # 6. Calculate Total Variance
            total_variance = total_charge - expected_total
            audit_result['total_variance'] = total_variance
            audit_result['expected_total'] = expected_total
            audit_result['actual_total'] = total_charge
            
            # Enhanced variance calculation comments
            audit_result['comments'].append(
                f"🧮 CALCULATION BREAKDOWN:"
            )
            audit_result['comments'].append(
                f"   Weight Charge: CNY {expected_weight_charge or 0:.2f} (Zone {zone} Rate Card)"
            )
            audit_result['comments'].append(
                f"   Fuel Surcharge: CNY {bcu_fuel_surcharges:.2f} (Pass-through)"
            )
            audit_result['comments'].append(
                f"   Other Charges: CNY {bcu_other_charges:.2f} (Service charges)"
            )
            audit_result['comments'].append(
                f"   Subtotal: CNY {subtotal_before_tax:.2f}"
            )
            audit_result['comments'].append(
                f"   Tax (6%): CNY {expected_tax:.2f} (Actual: CNY {bcu_taxes_applicable:.2f})"
            )
            audit_result['comments'].append(
                f"   Expected Total: CNY {expected_total:.2f}"
            )
            audit_result['comments'].append(
                f"   Actual Invoice: CNY {total_charge:.2f}"
            )
            audit_result['comments'].append(
                f"   VARIANCE: CNY {total_variance:+.2f} ({(total_variance/expected_total*100):+.1f}% of expected)"
            )
            
            # 7. Determine Overall Status
            variance_pct_signed = (total_variance / expected_total * 100) if expected_total > 0 else 0
            variance_pct_abs = abs(variance_pct_signed)
            tax_status = audit_result['tax_audit']['status']
            weight_status = audit_result['weight_audit'].get('status')

            # Check if this is a customer benefit scenario (negative variance)
            is_customer_benefit = variance_pct_signed < 0
            
            if (
                weight_status == 'pass'
                and tax_status == 'pass'
                and variance_pct_abs <= 5.0
            ):
                audit_result['audit_status'] = 'pass'
                audit_result['comments'].append(
                    f"✅ AUDIT RESULT: PASS (Variance {variance_pct_abs:.1f}% ≤ 5% tolerance)"
                )
            elif (
                weight_status == 'pass'
                and is_customer_benefit
            ):
                # Customer benefit: Pass even if tax has minor variance
                audit_result['audit_status'] = 'pass'
                audit_result['comments'].append(
                    f"✅ AUDIT RESULT: PASS (Variance {variance_pct_abs:.1f}% in customer favor - customer benefit)"
                )
            elif weight_status == 'error':
                audit_result['audit_status'] = 'error'
                audit_result['comments'].append(
                    f"❌ AUDIT RESULT: ERROR (Rate lookup failed)"
                )
            else:
                audit_result['audit_status'] = 'variance'
                audit_result['comments'].append(
                    f"⚠️ AUDIT RESULT: VARIANCE DETECTED (Variance {variance_pct_abs:.1f}% > 5% tolerance)"
                )
            
            return audit_result
            
        except Exception as e:
            logger.error(f"Error auditing invoice line: {e}")
            return {
                'awb': invoice_data.get('air_waybill', ''),
                'audit_status': 'error',
                'error': str(e),
                'comments': [f"Audit failed: {str(e)}"]
            }
    
    def audit_invoice(self, invoice_number: str) -> Dict[str, Any]:
        """
        Audit all lines in a DHL Express China invoice.
        
        Args:
            invoice_number: Invoice number to audit
            
        Returns:
            Complete audit results for the invoice
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Get all lines for the invoice
            cursor.execute("""
                SELECT * FROM dhl_express_china_invoices
                WHERE invoice_number = ?
                ORDER BY air_waybill
            """, (invoice_number,))
            
            invoice_lines = cursor.fetchall()
            if not invoice_lines:
                return {
                    'invoice_number': invoice_number,
                    'status': 'error',
                    'message': 'Invoice not found',
                    'lines': []
                }
            
            # Audit each line
            line_results = []
            total_expected = 0
            total_actual = 0
            
            for line in invoice_lines:
                line_dict = dict(line)
                line_result = self.audit_invoice_line(line_dict)
                line_results.append(line_result)
                
                total_expected += line_result.get('expected_total', 0)
                total_actual += line_result.get('actual_total', 0)
            
            # Summary
            total_variance = total_actual - total_expected
            variance_percent = (
                (total_variance / total_expected * 100)
                if total_expected > 0
                else 0
            )
            
            # Overall status
            pass_count = sum(
                1 for r in line_results if r['audit_status'] == 'pass'
            )
            error_count = sum(
                1 for r in line_results if r['audit_status'] == 'error'
            )
            variance_count = sum(
                1 for r in line_results if r['audit_status'] == 'variance'
            )
            
            if error_count > 0:
                overall_status = 'error'
            elif variance_count > 0:
                overall_status = 'variance'
            else:
                overall_status = 'pass'
            
            return {
                'invoice_number': invoice_number,
                'status': overall_status,
                'summary': {
                    'total_lines': len(line_results),
                    'pass_count': pass_count,
                    'variance_count': variance_count,
                    'error_count': error_count,
                    'total_expected_cny': total_expected,
                    'total_actual_cny': total_actual,
                    'total_variance_cny': total_variance,
                    'variance_percent': variance_percent
                },
                'lines': line_results,
                'audit_timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error auditing invoice {invoice_number}: {e}")
            return {
                'invoice_number': invoice_number,
                'status': 'error',
                'error': str(e),
                'lines': []
            }
        finally:
            conn.close()
    
    def save_audit_results(self, audit_results: Dict[str, Any]) -> bool:
        """
        Save audit results to the database.
        
        Args:
            audit_results: Audit results dictionary
            
        Returns:
            True if saved successfully, False otherwise
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            invoice_number = audit_results['invoice_number']
            
            # Update main invoice table with audit summary
            cursor.execute("""
                UPDATE dhl_express_china_invoices
                SET audit_status = ?,
                    expected_cost_cny = ?,
                    variance_cny = ?,
                    audit_timestamp = ?,
                    audit_details = ?
                WHERE invoice_number = ?
            """, (
                audit_results['status'],
                audit_results['summary']['total_expected_cny'],
                audit_results['summary']['total_variance_cny'],
                datetime.now(),
                json.dumps(audit_results),
                invoice_number,
            ))
            
            # Save detailed results for each line
            for line_result in audit_results['lines']:
                # Get invoice_id for this AWB
                cursor.execute("""
                    SELECT id FROM dhl_express_china_invoices
                    WHERE invoice_number = ? AND air_waybill = ?
                    LIMIT 1
                """, (invoice_number, line_result['awb']))
                
                invoice_row = cursor.fetchone()
                invoice_id = invoice_row['id'] if invoice_row else None
                
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO dhl_express_china_audit_results
                    (
                        invoice_id,
                        invoice_number,
                        air_waybill,
                        expected_cost_cny,
                        actual_cost_cny,
                        variance_cny,
                        variance_percent,
                        audit_status,
                        zone_used,
                        weight_used,
                        service_type,
                        audit_details
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        invoice_id,
                        invoice_number,
                        line_result['awb'],
                        line_result.get('expected_total', 0),
                        line_result.get('actual_total', 0),
                        line_result.get('total_variance', 0),
                        (
                            line_result.get('total_variance', 0)
                            / line_result.get('expected_total', 1)
                            * 100
                        ),
                        line_result['audit_status'],
                        line_result.get('zone_used', ''),
                        line_result.get('weight_audit', {}).get(
                            'weight_kg', 0
                        ),
                        'Import',
                        json.dumps(line_result),
                    ),
                )
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error saving audit results: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def get_invoice_summary(self) -> Dict:
        """Get summary of loaded DHL Express China invoices."""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Basic statistics
            cursor.execute('SELECT COUNT(DISTINCT invoice_number) FROM dhl_express_china_invoices')
            total_invoices = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT COUNT(*) FROM dhl_express_china_invoices')
            total_lines = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT SUM(bcu_total) FROM dhl_express_china_invoices')
            total_amount = cursor.fetchone()[0] or 0
            
            # Product/Service breakdown
            cursor.execute('''
                SELECT local_product_code, COUNT(*), SUM(bcu_total)
                FROM dhl_express_china_invoices
                WHERE local_product_code IS NOT NULL
                GROUP BY local_product_code
                ORDER BY SUM(bcu_total) DESC
                LIMIT 10
            ''')
            product_breakdown = []
            for row in cursor.fetchall():
                product_breakdown.append({
                    'description': row[0] or 'Unknown',
                    'count': row[1],
                    'amount': float(row[2] or 0),
                    'percentage': (float(row[2] or 0) / total_amount * 100) if total_amount > 0 else 0
                })
            
            # Route breakdown (Top routes by volume)
            cursor.execute('''
                SELECT 
                    COALESCE(consignor_country, origin_code) as origin,
                    dest_code,
                    COUNT(DISTINCT air_waybill) as awb_count,
                    SUM(bcu_total) as amount
                FROM dhl_express_china_invoices
                WHERE air_waybill IS NOT NULL
                GROUP BY COALESCE(consignor_country, origin_code), dest_code
                ORDER BY COUNT(DISTINCT air_waybill) DESC
                LIMIT 10
            ''')
            route_breakdown = []
            for row in cursor.fetchall():
                route_breakdown.append({
                    'route': f"{row[0] or 'Unknown'} → {row[1] or 'CN'}",
                    'awbs': row[2],
                    'amount': float(row[3] or 0)
                })
            
            # Recent invoices
            cursor.execute('''
                SELECT DISTINCT invoice_number, invoice_date, COUNT(*) as line_count, SUM(bcu_total) as total
                FROM dhl_express_china_invoices
                WHERE invoice_number IS NOT NULL
                GROUP BY invoice_number, invoice_date
                ORDER BY invoice_date DESC
                LIMIT 5
            ''')
            recent_invoices = []
            for row in cursor.fetchall():
                recent_invoices.append({
                    'invoice_number': row[0],
                    'invoice_date': row[1],
                    'line_count': row[2],
                    'total_amount': float(row[3] or 0)
                })
            
            return {
                'total_invoices': total_invoices,
                'total_lines': total_lines,
                'total_amount': float(total_amount),
                'currency': 'CNY',
                'product_breakdown': product_breakdown,
                'route_breakdown': route_breakdown,
                'recent_invoices': recent_invoices,
                'data_source': 'DHL Express China Import Invoices'
            }
            
        except Exception as e:
            logger.error(f"Error getting invoice summary: {e}")
            return {
                'total_invoices': 0,
                'total_lines': 0,
                'total_amount': 0.0,
                'currency': 'CNY',
                'product_breakdown': [],
                'route_breakdown': [],
                'recent_invoices': [],
                'data_source': 'DHL Express China Import Invoices',
                'error': str(e)
            }
        finally:
            conn.close()

    def get_unaudited_invoices(self) -> List[str]:
        """Get list of invoice numbers that haven't been audited yet."""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Get all unique invoice numbers from China invoices table
            cursor.execute('''
                SELECT DISTINCT invoice_number 
                FROM dhl_express_china_invoices 
                WHERE invoice_number NOT IN (
                    SELECT DISTINCT invoice_number 
                    FROM dhl_express_china_audit_results 
                    WHERE invoice_number IS NOT NULL
                )
                ORDER BY invoice_number
            ''')
            
            unaudited_invoices = [row[0] for row in cursor.fetchall()]
            return unaudited_invoices
            
        except Exception as e:
            logger.error(f"Error getting unaudited invoices: {e}")
            return []
        finally:
            conn.close()

    def get_audit_status_summary(self) -> Dict:
        """Get summary of audit status for all invoices."""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Total invoices in system
            cursor.execute('SELECT COUNT(DISTINCT invoice_number) FROM dhl_express_china_invoices')
            total_invoices = cursor.fetchone()[0] or 0
            
            # Audited invoices
            cursor.execute('SELECT COUNT(DISTINCT invoice_number) FROM dhl_express_china_audit_results')
            audited_invoices = cursor.fetchone()[0] or 0
            
            # Audit results breakdown
            cursor.execute('''
                SELECT audit_status, COUNT(*) 
                FROM dhl_express_china_audit_results 
                GROUP BY audit_status
            ''')
            
            status_breakdown = {}
            audit_status_breakdown = {}  # For template compatibility
            for row in cursor.fetchall():
                status_breakdown[row[0]] = row[1]
                # Map to template expected format
                if row[0] == 'pass':
                    audit_status_breakdown['PASS'] = row[1]
                elif row[0] == 'variance':
                    audit_status_breakdown['REVIEW'] = row[1]
                elif row[0] == 'error':
                    audit_status_breakdown['ERROR'] = row[1]
                    audit_status_breakdown['FAIL'] = row[1]  # Error can be both ERROR and FAIL
            
            # Ensure all expected keys exist
            for key in ['PASS', 'REVIEW', 'FAIL', 'ERROR']:
                if key not in audit_status_breakdown:
                    audit_status_breakdown[key] = 0
            
            # Calculate metrics
            pending_audit = total_invoices - audited_invoices
            completion_rate = (audited_invoices / total_invoices * 100) if total_invoices > 0 else 0
            
            return {
                'total_invoices': total_invoices,
                'audited_invoices': audited_invoices,
                'pending_audit': pending_audit,
                'completion_rate': completion_rate,
                'passed': status_breakdown.get('pass', 0),
                'variance': status_breakdown.get('variance', 0),
                'failed': status_breakdown.get('error', 0),
                'errors': status_breakdown.get('error', 0),
                'status_breakdown': status_breakdown,
                'audit_status_breakdown': audit_status_breakdown  # For template compatibility
            }
            
        except Exception as e:
            logger.error(f"Error getting audit status summary: {e}")
            return {
                'total_invoices': 0,
                'audited_invoices': 0,
                'pending_audit': 0,
                'completion_rate': 0,
                'passed': 0,
                'variance': 0,
                'failed': 0,
                'errors': 0,
                'status_breakdown': {}
            }
        finally:
            conn.close()

    def audit_all_unaudited_invoices(self) -> Dict:
        """Run audit on all unaudited invoices."""
        unaudited_invoices = self.get_unaudited_invoices()
        
        if not unaudited_invoices:
            return {
                'success': True,
                'message': 'No unaudited invoices found',
                'audited_count': 0,
                'results': []
            }
        
        results = []
        successful_audits = 0
        
        for invoice_number in unaudited_invoices:
            try:
                result = self.audit_invoice(invoice_number)
                if result['status'] != 'error':
                    # Save the audit result
                    if self.save_audit_results(result):
                        successful_audits += 1
                results.append(result)
                
            except Exception as e:
                logger.error(f"Error auditing invoice {invoice_number}: {e}")
                results.append({
                    'invoice_number': invoice_number,
                    'status': 'error',
                    'error': str(e)
                })
        
        return {
            'success': True,
            'message': f'Audited {successful_audits} of {len(unaudited_invoices)} invoices',
            'audited_count': successful_audits,
            'total_count': len(unaudited_invoices),
            'results': results
        }


def main():
    """Main function for testing the audit engine."""
    engine = DHLExpressChinaAuditEngine()
    
    # Test with the example invoice
    invoice_number = "SUZIR00188265"
    print(f"Auditing invoice: {invoice_number}")
    
    results = engine.audit_invoice(invoice_number)
    print(json.dumps(results, indent=2, default=str))
    
    # Save results
    if engine.save_audit_results(results):
        print("\nAudit results saved successfully!")
    else:
        print("\nFailed to save audit results.")

if __name__ == "__main__":
    main()
