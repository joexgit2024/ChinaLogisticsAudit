"""Core audit logic for DHL Express invoices."""

from typing import Dict, List, Optional, Union
import sqlite3
import json
import csv
from pathlib import Path
from datetime import datetime

# Import from our new modules
from dhl_express_audit_constants import (
    THIRD_PARTY_INDICATORS, VARIANCE_THRESHOLD_PASS, VARIANCE_THRESHOLD_REVIEW
)
from dhl_express_audit_utils import (
    extract_country_code, get_au_domestic_zone, parse_date, is_domestic_shipment
)
from dhl_express_audit_service_charges import (
    match_service_description, get_bonded_storage_charge, get_expected_service_charge
)


class DHLExpressAuditEngine:
    """DHL Express Audit Engine class for processing and auditing DHL Express invoices.
    
    This class provides comprehensive audit functionality for DHL Express invoices,
    including rate validation, service charge verification, and compliance checking.
    
    Used by:
    - Flask routes in dhl_express_routes.py
    - Batch processing scripts
    - Command-line audit tools
    """
    
    def __init__(self, db_path: str = 'dhl_audit.db'):
        """Initialize the audit engine with database connection."""
        self.db_path = db_path
    
    def audit_invoice(self, invoice_no: str, conn=None) -> Dict:
        """Audit a DHL Express invoice and return detailed results.
        
        Args:
            invoice_no: The invoice number to audit
            conn: Optional database connection (will create if not provided)
            
        Returns:
            Dict containing audit results with status, amounts, and line item details
        """
        # Connect to the database if connection not provided
        should_close_conn = False
        if conn is None:
            conn = sqlite3.connect(self.db_path)
            should_close_conn = True
        
        cursor = conn.cursor()
        
        # Get invoice line items
        cursor.execute('''
            SELECT * FROM dhl_express_invoices 
            WHERE invoice_no = ?
        ''', (invoice_no,))
        
        rows = cursor.fetchall()
        
        if not rows:
            if should_close_conn:
                conn.close()
            return {'status': 'ERROR', 'message': f'Invoice {invoice_no} not found'}
        
        # Process each line item
        line_items = []
        total_invoice_amount = 0
        total_expected_amount = 0
        
        for row in rows:
            # Extract relevant fields from the row
            line_number = row[5]  # line_number
            product_desc = row[7]  # dhl_product_description
            amount = row[9]  # amount
            awb_number = row[15]  # awb_number
            weight = row[16]  # weight
            
            # Build line item object
            line = {
                'line_number': line_number,
                'description': product_desc,
                'amount': float(amount) if amount is not None else 0,
                'awb_number': awb_number,
                'weight': float(weight) if weight is not None else 0
            }
            
            total_invoice_amount += line['amount']
            
            # Audit the line item
            audit_result = self._audit_line_item(line, conn)
            
            # Add audit results to the line item
            line_item_result = {
                'line_number': line['line_number'],
                'description': line['description'],
                'invoiced': line['amount'],
                'expected': audit_result['expected_amount'],
                'variance': audit_result['variance'],
                'result': audit_result['audit_result'],
                'comments': audit_result['comments']
            }
            
            total_expected_amount += audit_result['expected_amount']
            line_items.append(line_item_result)
        
        # Calculate overall variance
        total_variance = total_invoice_amount - total_expected_amount
        if total_expected_amount > 0:
            variance_percent = (total_variance / total_expected_amount) * 100
        else:
            variance_percent = 0
            
        # Determine overall audit status
        if abs(variance_percent) <= VARIANCE_THRESHOLD_PASS * 100:
            status = 'PASS'
        elif abs(variance_percent) <= VARIANCE_THRESHOLD_REVIEW * 100:
            status = 'REVIEW'
        else:
            status = 'FAIL'
            
        # Save the audit result to the database
        try:
            cursor.execute('''
                INSERT INTO dhl_express_audit_results (
                    invoice_no, awb_number, audit_timestamp, 
                    total_invoice_amount, total_expected_amount, 
                    total_variance, variance_percentage, 
                    audit_status, line_items_audited, 
                    line_items_passed, line_items_failed, 
                    detailed_results, confidence_score, created_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                invoice_no,
                rows[0][15] if rows[0][15] else 'unknown',
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                total_invoice_amount,
                total_expected_amount,
                total_variance,
                variance_percent,
                status,
                len(line_items),
                len([item for item in line_items if item['result'] == 'PASS']),
                len([item for item in line_items if item['result'] == 'FAIL']),
                json.dumps(line_items),
                max(0, 100 - abs(variance_percent)),
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
            conn.commit()
        except Exception as e:
            print(f"Error saving audit result: {e}")
            
        if should_close_conn:
            conn.close()
            
        return {
            'status': status,
            'total_invoice_amount': total_invoice_amount,
            'total_expected_amount': total_expected_amount,
            'total_variance': total_variance,
            'variance_percent': variance_percent,
            'line_items': line_items
        }
    
    def _audit_line_item(self, line: Dict, conn) -> Dict:
        """Audit a single line item based on product description."""
        product_desc = line.get('description', '').upper()
        
        # Different audit logic based on product description
        if 'EXPRESS' in product_desc and ('WORLDWIDE' in product_desc or 'DOMESTIC' in product_desc):
            # Express shipment rate
            return self._audit_express_rate(line, conn)
        elif 'FUEL SURCHARGE' in product_desc:
            # Fuel surcharge - accept as-is for now
            return {
                'expected_amount': line['amount'],
                'variance': 0,
                'audit_result': 'PASS',
                'comments': ['Fuel surcharge accepted (calculation pending)']
            }
        elif 'REMOTE AREA PICKUP' in product_desc:
            # Remote Area Pickup service charge
            return get_expected_service_charge(line, 'OB', conn)
        elif 'REMOTE AREA DELIVERY' in product_desc:
            # Remote Area Delivery service charge
            return get_expected_service_charge(line, 'OO', conn)
        elif 'REMOTE AREA' in product_desc:
            # Remote area surcharge (general)
            return get_expected_service_charge(line, 'RA', conn)
        elif 'OVERWEIGHT PIECE' in product_desc:
            # Overweight Piece service charge
            return get_expected_service_charge(line, 'YY', conn)
        elif 'OVER LENGTH' in product_desc or 'OVERWEIGHT' in product_desc:
            # Over length or overweight charge
            return get_expected_service_charge(line, 'KA', conn)
        elif 'BONDED STORAGE' in product_desc:
            # Bonded Storage service charge - Special handling for MAX formula
            return get_bonded_storage_charge(line, conn)
        elif 'EXPORT DECLARATION' in product_desc:
            # Export Declaration service charge
            return get_expected_service_charge(line, 'WO', conn)
        elif 'NEUTRAL DELIVERY' in product_desc:
            # Neutral Delivery service charge
            return get_expected_service_charge(line, 'NN', conn)
        elif 'SIGNATURE' in product_desc:
            # Other signature service charge
            return get_expected_service_charge(line, 'SIGNATURE', conn)
        else:
            # Try comprehensive service description matching
            service_code = match_service_description(product_desc, conn)
            return get_expected_service_charge(line, service_code, conn)
    
    def _is_3rd_party_charge(self, product_desc: str) -> bool:
        """Determine if this is a 3rd party charge."""
        for indicator in THIRD_PARTY_INDICATORS:
            if indicator in product_desc.upper():
                return True
        return False
    
    def _audit_express_rate(self, line: Dict, conn) -> Dict:
        """Audit express rate - determines rate card based on origin/destination."""
        cursor = conn.cursor()
        
        product_desc = line.get('description', '').upper()
        weight = line.get('weight', 0)
        amount = line.get('amount', 0)
        awb = line.get('awb_number')
        
        # First get origin and destination countries to determine rate card type
        cursor.execute('''
            SELECT shipper_details, receiver_details
            FROM dhl_express_invoices
            WHERE awb_number = ?
            LIMIT 1
        ''', (awb,))
        
        result = cursor.fetchone()
        if not result:
            return {
                'expected_amount': amount,
                'variance': 0,
                'audit_result': 'REVIEW',
                'comments': ['No shipment details found for audit']
            }
        
        shipper_details, receiver_details = result
        
        # Extract country codes
        origin_country = extract_country_code(shipper_details)
        dest_country = extract_country_code(receiver_details)
        
        if not origin_country or not dest_country:
            return {
                'expected_amount': amount,
                'variance': 0,
                'audit_result': 'REVIEW',
                'comments': ['Could not extract country codes for audit']
            }
        
        # TOP LEVEL LOGIC: Determine which rate card to use
        if origin_country == 'AU' and dest_country == 'AU':
            # AU domestic shipment → Use AU Domestic rate card
            return self._audit_au_domestic_rate(line, conn)
        elif origin_country == 'AU':
            # Shipper is Australia → Use Export rate card
            return self._audit_regular_express_rate(line, conn, 'Export')
        elif dest_country == 'AU':
            # Consignee is Australia → Use Import rate card  
            return self._audit_regular_express_rate(line, conn, 'Import')
        else:
            # Neither shipper nor consignee is Australia → Use 3rd Party rate card
            # But only if this is actually a 3rd party charge description
            if self._is_3rd_party_charge(product_desc):
                return self._audit_3rd_party_rate(line, conn)
            else:
                # This shouldn't happen - non-AU to non-AU but not 3rd party description
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': [f'Non-AU shipment ({origin_country}→{dest_country}) but no 3rd party description']
                }
    
    # Note: The following methods (_audit_regular_express_rate, _audit_3rd_party_rate, 
    # _audit_au_domestic_rate, etc.) contain the same logic as the original file.
    # For brevity, I'm including placeholders here - the full methods would be copied
    # from the original file in a complete implementation.
    
    def _audit_regular_express_rate(self, line: Dict, conn, rate_type: str) -> Dict:
        """Audit regular DHL Express rate using Import/Export rate cards"""
        cursor = conn.cursor()
        
        product_desc = line.get('description', '').upper()
        weight = line.get('weight', 0)
        amount = line.get('amount', 0)
        awb = line.get('awb_number')
        
        try:
            # Get origin and destination countries
            cursor.execute('''
                SELECT shipper_details, receiver_details
                FROM dhl_express_invoices
                WHERE awb_number = ?
                LIMIT 1
            ''', (awb,))
            
            result = cursor.fetchone()
            if not result:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': ['No shipment details found for regular audit']
                }
            
            shipper_details, receiver_details = result
            
            # Extract country codes
            origin_country = extract_country_code(shipper_details)
            dest_country = extract_country_code(receiver_details)
            
            if not origin_country or not dest_country:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': ['Could not extract country codes for regular audit']
                }
            
            # Look up the zone using country codes
            cursor.execute('''
                SELECT zone_number
                FROM dhl_express_zone_mapping
                WHERE origin_code = ? AND destination_code = ?
            ''', (origin_country, dest_country))
            
            zone_result = cursor.fetchone()
            zone = zone_result[0] if zone_result else None
            
            if not zone:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': [f'No zone mapping for {origin_country} → {dest_country}']
                }
            
            # Determine if this is document or non-document
            if 'NONDOC' in product_desc:
                is_document = False
            elif 'DOC' in product_desc:
                is_document = True
            else:
                is_document = False
            section = 'Documents' if is_document else 'Non-documents'
            
            # Query the rate card with the specified rate_type
            zone_column = f'zone_{zone}'
            
            cursor.execute(f'''
                SELECT {zone_column}, weight_from, weight_to, is_multiplier
                FROM dhl_express_rate_cards
                WHERE service_type = ? 
                AND rate_section = ?
                AND weight_from <= ?
                AND (weight_to >= ? OR weight_to IS NULL)
                ORDER BY weight_from DESC
                LIMIT 1
            ''', (rate_type, section, weight, weight))
            
            rate_result = cursor.fetchone()
            
            if rate_result:
                rate, weight_from, weight_to, is_multiplier = rate_result
                
                if rate is not None and rate > 0:
                    # Calculate expected amount
                    if is_multiplier:
                        # For adder rates (>30kg), need base rate + adders
                        if weight > 30:
                            # Get 30kg base rate
                            cursor.execute(f'''
                                SELECT {zone_column}
                                FROM dhl_express_rate_cards
                                WHERE service_type = ? 
                                AND rate_section = ?
                                AND weight_from = 30 
                                AND weight_to = 30
                            ''', (rate_type, section))
                            
                            base_result = cursor.fetchone()
                            if base_result and base_result[0]:
                                base_rate = float(base_result[0])
                                adder_rate = float(rate)
                                additional_kg = weight - 30
                                expected_amount = base_rate + (additional_kg * adder_rate)
                            else:
                                expected_amount = float(rate) * weight
                        else:
                            expected_amount = float(rate) * weight
                    else:
                        expected_amount = float(rate)
                    
                    variance = expected_amount - amount
                    
                    # Determine audit result
                    if abs(variance) <= expected_amount * 0.05:  # Within 5%
                        audit_result = 'PASS'
                    elif abs(variance) <= expected_amount * 0.15:  # Within 15%
                        audit_result = 'REVIEW'
                    else:
                        audit_result = 'FAIL'
                    
                    comments = [
                        f'{rate_type}: {origin_country} Zone {zone} → {dest_country}',
                        f'Weight: {weight}kg, Expected: ${expected_amount:.2f}, Variance: ${variance:.2f}'
                    ]
                    
                    return {
                        'expected_amount': expected_amount,
                        'variance': variance,
                        'audit_result': audit_result,
                        'comments': comments
                    }
                else:
                    return {
                        'expected_amount': amount,
                        'variance': 0,
                        'audit_result': 'REVIEW',
                        'comments': [f'No valid rate for Zone {zone}, Weight {weight}kg']
                    }
            else:
                # No direct rate found - try adder calculation for weights >30kg
                if weight > 30:
                    # Get 30kg base rate
                    cursor.execute(f'''
                        SELECT {zone_column}
                        FROM dhl_express_rate_cards
                        WHERE service_type = ? 
                        AND rate_section = ?
                        AND weight_from <= 30.0 
                        AND weight_to >= 30.0
                    ''', (rate_type, section))
                    
                    base_result = cursor.fetchone()
                    if base_result and base_result[0]:
                        # Get the appropriate multiplier rate for this weight range dynamically
                        cursor.execute(f'''
                            SELECT {zone_column}, weight_from, weight_to
                            FROM dhl_express_rate_cards
                            WHERE service_type = ? 
                            AND rate_section = 'Multiplier'
                            AND is_multiplier = 1
                            AND weight_from <= ? 
                            AND weight_to >= ?
                            AND {zone_column} IS NOT NULL
                            LIMIT 1
                        ''', (rate_type, weight, weight))
                        
                        adder_result = cursor.fetchone()
                        if adder_result and adder_result[0]:
                            base_rate = float(base_result[0])
                            adder_rate = float(adder_result[0])
                            multiplier_range = f"{adder_result[1]}-{adder_result[2]}kg"
                            additional_weight = weight - 30
                            # Calculate number of 0.5kg increments
                            increments = additional_weight / 0.5
                            adder_amount = adder_rate * increments
                            expected_amount = base_rate + adder_amount
                            
                            variance = expected_amount - amount
                            
                            # Determine audit result
                            if abs(variance) <= expected_amount * 0.05:  # Within 5%
                                audit_result = 'PASS'
                            elif abs(variance) <= expected_amount * 0.15:  # Within 15%
                                audit_result = 'REVIEW'
                            else:
                                audit_result = 'FAIL'
                            
                            comments = [
                                f'{rate_type}: {origin_country} Zone {zone} → {dest_country}',
                                f'Weight: {weight}kg, Range: {multiplier_range}, Expected: ${expected_amount:.2f} (Base: ${base_rate:.2f} + Adder: ${adder_amount:.2f}), Variance: ${variance:.2f}'
                            ]
                            
                            return {
                                'expected_amount': expected_amount,
                                'variance': variance,
                                'audit_result': audit_result,
                                'comments': comments
                            }
                
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': [f'No rate entry for Zone {zone}, Weight {weight}kg']
                }
                
        except Exception as e:
            return {
                'expected_amount': amount,
                'variance': 0,
                'audit_result': 'ERROR',
                'comments': [f'Regular audit error: {str(e)}']
            }
    
    def _audit_3rd_party_rate(self, line: Dict, conn) -> Dict:
        """Audit 3rd party charge using our new logic"""
        cursor = conn.cursor()
        
        weight = line.get('weight', 0)
        amount = line.get('amount', 0)
        awb = line.get('awb_number')
        
        try:
            # Get origin and destination countries
            cursor.execute('''
                SELECT shipper_details, receiver_details
                FROM dhl_express_invoices
                WHERE awb_number = ?
                LIMIT 1
            ''', (awb,))
            
            result = cursor.fetchone()
            if not result:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': ['No shipment details found for 3rd party audit']
                }
            
            shipper_details, receiver_details = result
            
            # Extract country codes
            origin_country = extract_country_code(shipper_details)
            dest_country = extract_country_code(receiver_details)
            
            if not origin_country or not dest_country:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': ['Could not extract country codes for 3rd party audit']
                }
            
            # Step 1: Get zones from 3rd party mapping
            cursor.execute('''
                SELECT zone FROM dhl_express_3rd_party_zones 
                WHERE country_code = ?
            ''', (origin_country,))
            origin_result = cursor.fetchone()
            
            cursor.execute('''
                SELECT zone FROM dhl_express_3rd_party_zones 
                WHERE country_code = ?
            ''', (dest_country,))
            dest_result = cursor.fetchone()
            
            if not origin_result or not dest_result:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': [f'3rd party zones not found for {origin_country} → {dest_country}']
                }
            
            origin_zone = origin_result[0]
            dest_zone = dest_result[0]
            
            # Step 2: Get rate zone from matrix
            cursor.execute('''
                SELECT rate_zone FROM dhl_express_3rd_party_matrix 
                WHERE origin_zone = ? AND destination_zone = ?
            ''', (origin_zone, dest_zone))
            matrix_result = cursor.fetchone()
            
            if not matrix_result:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': [f'No 3rd party matrix entry for Zone {origin_zone} × Zone {dest_zone}']
                }
            
            rate_zone = matrix_result[0]
            
            # Step 3: Get rate for weight and zone
            zone_column = f"zone_{rate_zone.lower()}"
            cursor.execute(f'''
                SELECT {zone_column} FROM dhl_express_3rd_party_rates 
                WHERE weight_kg = ?
            ''', (weight,))
            rate_result = cursor.fetchone()
            
            if not rate_result or rate_result[0] is None:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': [f'No 3rd party rate found for {weight}kg Zone {rate_zone}']
                }
            
            expected_amount = float(rate_result[0])
            variance = expected_amount - amount
            
            # Determine audit result
            if abs(variance) <= expected_amount * 0.05:  # Within 5%
                audit_result = 'PASS'
            elif abs(variance) <= expected_amount * 0.15:  # Within 15%
                audit_result = 'REVIEW'
            else:
                audit_result = 'FAIL'
            
            comments = [
                f'3rd Party: {origin_country} Zone {origin_zone} → {dest_country} Zone {dest_zone} = Zone {rate_zone}',
                f'Weight: {weight}kg, Expected: ${expected_amount:.2f}, Variance: ${variance:.2f}'
            ]
            
            return {
                'expected_amount': expected_amount,
                'variance': variance,
                'audit_result': audit_result,
                'comments': comments
            }
            
        except Exception as e:
            return {
                'expected_amount': amount,
                'variance': 0,
                'audit_result': 'ERROR',
                'comments': [f'3rd party audit error: {str(e)}']
            }
    
    def _audit_au_domestic_rate(self, line: Dict, conn) -> Dict:
        """Audit AU domestic rate using AU domestic rate cards"""
        cursor = conn.cursor()
        
        weight = line.get('weight', 0)
        amount = line.get('amount', 0)
        awb = line.get('awb_number')
        product_desc = line.get('description', '').upper()
        
        try:
            # Get origin and destination cities/states for AU domestic
            cursor.execute('''
                SELECT shipper_details, receiver_details
                FROM dhl_express_invoices
                WHERE awb_number = ?
                LIMIT 1
            ''', (awb,))
            
            result = cursor.fetchone()
            if not result:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': ['No shipment details found for AU domestic audit']
                }
            
            shipper_details, receiver_details = result
            
            # Extract city codes or zones from addresses (simplified approach)
            origin_zone = get_au_domestic_zone(shipper_details)
            dest_zone = get_au_domestic_zone(receiver_details)
            
            if not origin_zone or not dest_zone:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': [f'Could not determine AU domestic zones from addresses']
                }
            
            # Get rate zone from matrix
            cursor.execute('''
                SELECT rate_zone FROM dhl_express_au_domestic_matrix 
                WHERE origin_zone = ? AND destination_zone = ?
            ''', (origin_zone, dest_zone))
            matrix_result = cursor.fetchone()
            
            if not matrix_result:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': [f'No AU domestic matrix entry for Zone {origin_zone} → Zone {dest_zone}']
                }
            
            rate_zone = matrix_result[0]
            
            # Get rate for weight and zone
            zone_column = f"zone_{rate_zone.lower()}"
            
            # Check if the column exists in the rates table
            cursor.execute("PRAGMA table_info(dhl_express_au_domestic_rates)")
            columns = [col[1] for col in cursor.fetchall()]
            
            if zone_column not in columns:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': [f'AU domestic rate zone {rate_zone} not found in rate table']
                }
            
            # Find the rate for this weight
            cursor.execute(f'''
                SELECT {zone_column} FROM dhl_express_au_domestic_rates 
                WHERE weight_kg >= ? 
                ORDER BY weight_kg ASC
                LIMIT 1
            ''', (weight,))
            rate_result = cursor.fetchone()
            
            if not rate_result or rate_result[0] is None:
                # Try finding the closest weight
                cursor.execute(f'''
                    SELECT {zone_column}, weight_kg FROM dhl_express_au_domestic_rates 
                    WHERE {zone_column} IS NOT NULL
                    ORDER BY ABS(weight_kg - ?) ASC
                    LIMIT 1
                ''', (weight,))
                rate_result = cursor.fetchone()
                
                if not rate_result:
                    return {
                        'expected_amount': amount,
                        'variance': 0,
                        'audit_result': 'REVIEW',
                        'comments': [f'No AU domestic rate found for {weight}kg Zone {rate_zone}']
                    }
            
            expected_amount = float(rate_result[0])
            variance = expected_amount - amount
            
            # Determine audit result
            if abs(variance) <= expected_amount * 0.05:  # Within 5%
                audit_result = 'PASS'
            elif abs(variance) <= expected_amount * 0.15:  # Within 15%
                audit_result = 'REVIEW'
            else:
                audit_result = 'FAIL'
            
            comments = [
                f'AU Domestic: Zone {origin_zone} → Zone {dest_zone} = Rate Zone {rate_zone}',
                f'Weight: {weight}kg, Expected: ${expected_amount:.2f}, Variance: ${variance:.2f}'
            ]
            
            return {
                'expected_amount': expected_amount,
                'variance': variance,
                'audit_result': audit_result,
                'comments': comments
            }
            
        except Exception as e:
            return {
                'expected_amount': amount,
                'variance': 0,
                'audit_result': 'ERROR',
                'comments': [f'AU domestic audit error: {str(e)}']
            }
    
    # Additional utility methods for the audit engine
    
    def get_invoice_summary(self) -> Dict:
        """Get summary of loaded DHL Express invoices."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Basic statistics
        cursor.execute('SELECT COUNT(DISTINCT invoice_no) FROM dhl_express_invoices')
        total_invoices = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM dhl_express_invoices')
        total_lines = cursor.fetchone()[0]
        
        cursor.execute('SELECT SUM(amount) FROM dhl_express_invoices')
        total_amount = cursor.fetchone()[0] or 0
        
        # Product breakdown
        cursor.execute('''
            SELECT dhl_product_description, COUNT(*), SUM(amount)
            FROM dhl_express_invoices
            GROUP BY dhl_product_description
            ORDER BY SUM(amount) DESC
        ''')
        product_breakdown = cursor.fetchall()
        
        # Route breakdown
        cursor.execute('''
            SELECT origin_code, destination_code, COUNT(DISTINCT awb_number), SUM(amount)
            FROM dhl_express_invoices
            GROUP BY origin_code, destination_code
            ORDER BY SUM(amount) DESC
            LIMIT 10
        ''')
        route_breakdown = cursor.fetchall()
        
        conn.close()
        
        return {
            'total_invoices': total_invoices,
            'total_lines': total_lines,
            'total_amount': total_amount,
            'product_breakdown': [
                {
                    'product': p[0] or 'Unknown',
                    'count': p[1],
                    'amount': p[2]
                }
                for p in product_breakdown
            ],
            'route_breakdown': [
                {
                    'origin': r[0] or 'Unknown',
                    'destination': r[1] or 'Unknown',
                    'shipment_count': r[2],
                    'amount': r[3]
                }
                for r in route_breakdown
            ]
        }
    
    def audit_batch(self, invoice_list: List[str]) -> Dict:
        """Audit a batch of invoices."""
        start_time = datetime.now()
        results = []
        
        conn = sqlite3.connect(self.db_path)
        
        for invoice_id in invoice_list:
            try:
                result = self.audit_invoice(invoice_id, conn)
                results.append({
                    'invoice_id': invoice_id,
                    'status': result['status'],
                    'variance': result['total_variance'],
                    'variance_percent': result['variance_percent']
                })
            except Exception as e:
                results.append({
                    'invoice_id': invoice_id,
                    'status': 'ERROR',
                    'message': str(e)
                })
                
        conn.close()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        return {
            'total_invoices': len(invoice_list),
            'completed': len(results),
            'passed': len([r for r in results if r.get('status') == 'PASS']),
            'reviewed': len([r for r in results if r.get('status') == 'REVIEW']),
            'failed': len([r for r in results if r.get('status') == 'FAIL']),
            'errors': len([r for r in results if r.get('status') == 'ERROR']),
            'duration_seconds': duration,
            'results': results
        }
    
    def load_invoices_from_csv(self, file_path: str) -> Dict:
        """Load DHL Express invoices from CSV file."""
        file_path = Path(file_path)
        if not file_path.exists():
            return {'success': False, 'error': f'File not found: {file_path}'}
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            total_records = 0
            inserted_records = 0
            duplicate_records = 0
            error_records = 0
            errors = []
            
            with open(file_path, 'r', encoding='utf-8') as csvfile:
                # Read first line to check if it's a header
                first_line = csvfile.readline().strip()
                csvfile.seek(0)
                
                # Skip header if present
                if 'Invoice No' in first_line or 'invoice_no' in first_line:
                    next(csvfile)
                    
                reader = csv.reader(csvfile)
                
                for row_num, row in enumerate(reader, start=2 if 'Invoice No' in first_line else 1):
                    total_records += 1
                    
                    try:
                        # Skip empty rows
                        if not any(cell.strip() for cell in row):
                            continue
                            
                        # Ensure we have enough columns
                        if len(row) < 22:
                            row.extend([''] * (22 - len(row)))
                        
                        # Parse the row data
                        invoice_data = {
                            'invoice_no': row[0].strip() if row[0] else '',
                            'invoice_date': parse_date(row[1]) if row[1] else None,
                            'company_name': row[2].strip() if row[2] else '',
                            'account_number': row[3].strip() if row[3] else '',
                            'line_number': int(row[4]) if row[4] and row[4].strip().isdigit() else 0,
                            'item_id': row[5].strip() if row[5] else '',
                            'dhl_product_description': row[6].strip() if row[6] else '',
                            'pal_col': int(row[7]) if row[7] and row[7].strip().isdigit() else 0,
                            'amount': float(row[8]) if row[8] and row[8].strip() else 0.0,
                            'weight_charge': float(row[9]) if row[9] and row[9].strip() else 0.0,
                            'discount_amount': float(row[10]) if row[10] and row[10].strip() else 0.0,
                            'discount_code': row[11].strip() if row[11] else '',
                            'tax_amount': float(row[12]) if row[12] and row[12].strip() else 0.0,
                            'tax_code': row[13].strip() if row[13] else '',
                            'awb_number': row[14].strip() if row[14] else '',
                            'weight': float(row[15]) if row[15] and row[15].strip() else 0.0,
                            'shipper_reference': row[16].strip() if row[16] else '',
                            'shipment_date': parse_date(row[17]) if row[17] else None,
                            'origin_code': row[18].strip() if row[18] else '',
                            'destination_code': row[19].strip() if row[19] else '',
                            'shipper_details': row[20].strip() if len(row) > 20 and row[20] else '',
                            'receiver_details': row[21].strip() if len(row) > 21 and row[21] else ''
                        }
                        
                        # Insert into database
                        cursor.execute('''
                            INSERT OR IGNORE INTO dhl_express_invoices 
                            (invoice_no, invoice_date, company_name, account_number, line_number,
                             item_id, dhl_product_description, pal_col, amount, weight_charge,
                             discount_amount, discount_code, tax_amount, tax_code, awb_number,
                             weight, shipper_reference, shipment_date, origin_code, destination_code,
                             shipper_details, receiver_details)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            invoice_data['invoice_no'], invoice_data['invoice_date'],
                            invoice_data['company_name'], invoice_data['account_number'],
                            invoice_data['line_number'], invoice_data['item_id'],
                            invoice_data['dhl_product_description'], invoice_data['pal_col'],
                            invoice_data['amount'], invoice_data['weight_charge'],
                            invoice_data['discount_amount'], invoice_data['discount_code'],
                            invoice_data['tax_amount'], invoice_data['tax_code'],
                            invoice_data['awb_number'], invoice_data['weight'],
                            invoice_data['shipper_reference'], invoice_data['shipment_date'],
                            invoice_data['origin_code'], invoice_data['destination_code'],
                            invoice_data['shipper_details'], invoice_data['receiver_details']
                        ))
                        
                        if cursor.rowcount > 0:
                            inserted_records += 1
                        else:
                            duplicate_records += 1
                            
                    except Exception as e:
                        error_records += 1
                        errors.append(f"Row {row_num}: {str(e)}")
                        continue
            
            conn.commit()
            conn.close()
            
            return {
                'success': True,
                'total_records': total_records,
                'inserted_records': inserted_records,
                'duplicate_records': duplicate_records,
                'error_records': error_records,
                'errors': errors[:10]  # Limit to first 10 errors
            }
            
        except Exception as e:
            return {'success': False, 'error': str(e)}
    
    def get_unaudited_invoices(self) -> List[str]:
        """Get list of invoice numbers that haven't been audited yet."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get all unique invoice numbers from invoices table
        cursor.execute('''
            SELECT DISTINCT invoice_no 
            FROM dhl_express_invoices 
            WHERE invoice_no NOT IN (
                SELECT DISTINCT invoice_no 
                FROM dhl_express_audit_results 
                WHERE invoice_no IS NOT NULL
            )
            ORDER BY invoice_no
        ''')
        
        unaudited_invoices = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        return unaudited_invoices
    
    def get_audit_status_summary(self) -> Dict:
        """Get summary of audit status for all invoices."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Total invoices in system
        cursor.execute('SELECT COUNT(DISTINCT invoice_no) FROM dhl_express_invoices')
        total_invoices = cursor.fetchone()[0]
        
        # Audited invoices
        cursor.execute('SELECT COUNT(DISTINCT invoice_no) FROM dhl_express_audit_results')
        audited_invoices = cursor.fetchone()[0]
        
        # Audit results breakdown
        cursor.execute('''
            SELECT audit_status, COUNT(*) 
            FROM dhl_express_audit_results 
            GROUP BY audit_status
        ''')
        status_breakdown = dict(cursor.fetchall())
        
        # Recent audit activity
        cursor.execute('''
            SELECT COUNT(*) 
            FROM dhl_express_audit_results 
            WHERE created_timestamp >= datetime('now', '-24 hours')
        ''')
        recent_audits = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'total_invoices': total_invoices,
            'audited_invoices': audited_invoices,
            'unaudited_invoices': total_invoices - audited_invoices,
            'audit_status_breakdown': {
                'PASS': status_breakdown.get('PASS', 0),
                'REVIEW': status_breakdown.get('REVIEW', 0),
                'FAIL': status_breakdown.get('FAIL', 0),
                'ERROR': status_breakdown.get('ERROR', 0)
            },
            'recent_audits_24h': recent_audits
        }
    
    def audit_all_unaudited_invoices(self) -> Dict:
        """Audit all invoices that haven't been audited yet."""
        unaudited_invoices = self.get_unaudited_invoices()
        
        if not unaudited_invoices:
            return {
                'success': True,
                'message': 'No unaudited invoices found',
                'total_invoices': 0,
                'completed': 0,
                'passed': 0,
                'reviewed': 0,
                'failed': 0,
                'errors': 0,
                'duration_seconds': 0,
                'results': []
            }
        
        return self.audit_batch(unaudited_invoices)
