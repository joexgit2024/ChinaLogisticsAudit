"""Service charge calculation helpers for DHL Express audit."""

from typing import Dict
import sqlite3
from dhl_express_audit_constants import (
    FUZZY_SERVICE_MAPPINGS, VARIANT_LOOKUP_SERVICE_CODES,
    BONDED_STORAGE_BASE_CHARGE, BONDED_STORAGE_PER_KG_CHARGE
)
from dhl_express_audit_utils import is_domestic_shipment


def match_service_description(product_desc: str, conn) -> str:
    """Match invoice service description to service code using exact and fuzzy matching."""
    cursor = conn.cursor()
    
    # Clean the product description
    product_desc_clean = product_desc.upper().strip()
    
    # Step 1: Try exact match with service names
    cursor.execute('''
        SELECT service_code, service_name FROM dhl_express_services_surcharges
    ''')
    services = cursor.fetchall()
    
    # First try exact match (case-insensitive)
    for service_code, service_name in services:
        if service_name and service_name.upper() == product_desc_clean:
            return service_code
    
    # Step 2: Try partial matches - check if product description contains service name
    for service_code, service_name in services:
        if service_name and service_name.upper() in product_desc_clean:
            return service_code
    
    # Step 3: Try reverse - check if service name contains product description
    for service_code, service_name in services:
        if service_name and product_desc_clean in service_name.upper():
            return service_code
    
    # Step 4: Try fuzzy matching with common variations
    # Check fuzzy mappings
    for description, code in FUZZY_SERVICE_MAPPINGS.items():
        if description in product_desc_clean:
            return code
    
    # Step 5: Fallback - use first word as before
    return product_desc.split()[0] if product_desc else 'UNKNOWN'


def get_bonded_storage_charge(line: Dict, conn) -> Dict:
    """Special handling for BONDED STORAGE: MAX(18.00 per shipment, weight × 0.35 per kg)."""
    cursor = conn.cursor()
    
    # Get weight for calculation
    weight = line.get('weight', 0)
    line_item_weight = weight  # Keep track of original line item weight
    weight_source = "line item"
    
    # BONDED STORAGE charges often show 0kg on the invoice line item
    # but the calculation should be based on the actual shipment weight
    if weight == 0:
        awb = line.get('awb_number')
        if awb:
            # Search for the actual shipment weight from freight invoices with same AWB
            cursor.execute('''
                SELECT weight, invoice_no FROM dhl_express_invoices 
                WHERE awb_number = ? AND weight > 0
                ORDER BY weight DESC
                LIMIT 1
            ''', (awb,))
            shipment_weight_result = cursor.fetchone()
            if shipment_weight_result and shipment_weight_result[0]:
                weight = float(shipment_weight_result[0])
                freight_invoice = shipment_weight_result[1]
                weight_source = f"HAWB {awb} freight invoice {freight_invoice}"
    
    # BONDED STORAGE formula: MAX(18.00 per shipment, weight × 0.35 per kg)
    weight_based_charge = weight * BONDED_STORAGE_PER_KG_CHARGE
    expected_amount = max(BONDED_STORAGE_BASE_CHARGE, weight_based_charge)
    
    variance = expected_amount - line['amount']
    
    # Build comment with weight source explanation
    if line_item_weight == 0 and weight > 0:
        weight_explanation = f" (invoice shows {line_item_weight}kg, using {weight}kg from {weight_source})"
    else:
        weight_explanation = ""
    
    # Determine audit result
    if abs(variance) < 0.01:  # Small tolerance for rounding
        audit_result = 'PASS'
        comment = f'BONDED STORAGE: MAX(${BONDED_STORAGE_BASE_CHARGE:.2f}, {weight}kg × ${BONDED_STORAGE_PER_KG_CHARGE:.2f}) = ${expected_amount:.2f} ✓{weight_explanation}'
    elif variance > 0:
        audit_result = 'FAIL'
        comment = f'BONDED STORAGE: MAX(${BONDED_STORAGE_BASE_CHARGE:.2f}, {weight}kg × ${BONDED_STORAGE_PER_KG_CHARGE:.2f}) = ${expected_amount:.2f} (DHL undercharged by ${variance:.2f}){weight_explanation}'
    else:
        audit_result = 'FAIL'
        comment = f'BONDED STORAGE: MAX(${BONDED_STORAGE_BASE_CHARGE:.2f}, {weight}kg × ${BONDED_STORAGE_PER_KG_CHARGE:.2f}) = ${expected_amount:.2f} (DHL overcharged by ${abs(variance):.2f}){weight_explanation}'
    
    return {
        'expected_amount': expected_amount,
        'variance': variance,
        'audit_result': audit_result,
        'comments': [comment]
    }


def get_expected_service_charge(line: Dict, service_code: str, conn) -> Dict:
    """Get expected service charge from premium services table."""
    cursor = conn.cursor()
    
    # First try to find in premium services table
    try:
        cursor.execute('''
            SELECT charge_amount FROM dhl_express_premium_services
            WHERE premium_type LIKE ? 
            ORDER BY id DESC
            LIMIT 1
        ''', (f'%{service_code}%',))
        
        result = cursor.fetchone()
        if result:
            expected_amount = float(result[0])
            variance = expected_amount - line['amount']
            
            # Apply the same rule as freight charges: 
            # PASS if customer is undercharged (variance > 0)
            audit_result = 'PASS'
            comment = f'Premium service charge: ${expected_amount:.2f}'
            
            if variance > 0:
                # Customer was undercharged
                comment += f' (DHL undercharged by ${variance:.2f})'
            elif variance < 0 and abs(variance) > 1:
                # Customer was overcharged beyond tolerance
                audit_result = 'FAIL'
                comment += f' (DHL overcharged by ${abs(variance):.2f})'
                
            return {
                'expected_amount': expected_amount,
                'variance': variance,
                'audit_result': audit_result,
                'comments': [comment]
            }
    except Exception as e:
        print(f"Error in premium services lookup: {e}")
    
    # If premium services didn't find anything, try services table
    try:
        # For merged entry service codes, prioritize variants over basic entries
        needs_variant_lookup = service_code in VARIANT_LOOKUP_SERVICE_CODES
        
        if needs_variant_lookup:
            # Skip exact match for codes that have enhanced variants
            result = None
        else:
            # First try exact service code match for other codes
            cursor.execute('''
                SELECT charge_amount, charge_type, minimum_charge FROM dhl_express_services_surcharges
                WHERE service_code = ?
                LIMIT 1
            ''', (service_code,))
            result = cursor.fetchone()
        
        # If no exact match or we need variant lookup, try original_service_code (for merged entries)
        if not result:
            # Get the shipment's service description for smart variant selection
            awb = line.get('awb_number')
            shipment_service = ''
            if awb:
                cursor.execute('''
                    SELECT dhl_product_description FROM dhl_express_invoices 
                    WHERE awb_number = ? AND dhl_product_description NOT LIKE '%SURCHARGE%' 
                    AND dhl_product_description NOT LIKE '%PREMIUM%'
                    AND dhl_product_description NOT LIKE '%SIGNATURE%'
                    LIMIT 1
                ''', (awb,))
                service_result = cursor.fetchone()
                if service_result:
                    shipment_service = service_result[0].upper()
            
            # Get all variants for this original service code, ordered by specificity
            cursor.execute('''
                SELECT charge_amount, charge_type, minimum_charge, service_code, products_applicable
                FROM dhl_express_services_surcharges
                WHERE original_service_code = ?
                ORDER BY 
                    CASE 
                        WHEN products_applicable LIKE '%All Products%' THEN 2
                        ELSE 1
                    END
            ''', (service_code,))
            variants = cursor.fetchall()
            
            # Select the best matching variant
            selected_variant = None
            for variant in variants:
                charge_amount, charge_type, minimum_charge, variant_code, products_applicable = variant
                
                # Check if shipment service matches specific product categories
                if products_applicable and shipment_service:
                    products_upper = products_applicable.upper()
                    
                    # Special logic for domestic vs international determination
                    if 'DOMESTIC' in products_upper or 'INTERNATIONAL' in products_upper:
                        # Get shipper and receiver details for domestic check
                        cursor.execute('''
                            SELECT shipper_details, receiver_details
                            FROM dhl_express_invoices
                            WHERE awb_number = ?
                            LIMIT 1
                        ''', (awb,))
                        address_result = cursor.fetchone()
                        
                        if address_result:
                            shipper_details, receiver_details = address_result
                            is_domestic = is_domestic_shipment(shipper_details, receiver_details)
                            
                            if 'DOMESTIC' in products_upper and is_domestic:
                                selected_variant = variant
                                break
                            elif 'INTERNATIONAL' in products_upper and not is_domestic:
                                selected_variant = variant
                                break
                    else:
                        # Check for other specific matches
                        if any(product.strip() in shipment_service for product in products_upper.split(',') if product.strip()):
                            selected_variant = variant
                            break
                
                # If no specific match found, prefer "All Products" variant
                if products_applicable and 'All Products' in products_applicable:
                    selected_variant = variant
            
            if selected_variant:
                result = selected_variant[:3]  # charge_amount, charge_type, minimum_charge
            
            # If still no variant found, fall back to basic entry
            if not result and not needs_variant_lookup:
                cursor.execute('''
                    SELECT charge_amount, charge_type, minimum_charge FROM dhl_express_services_surcharges
                    WHERE service_code = ?
                    LIMIT 1
                ''', (service_code,))
                result = cursor.fetchone()
        
        if result:
            charge_amount, charge_type, minimum_charge = result
            
            # Get weight for per kg calculations
            weight = line.get('weight', 0)
            
            # For service charges with 0 weight, get the shipment weight from freight lines
            # This applies to per kg charges and weight-dependent per shipment charges (like OVERWEIGHT PIECE)
            if weight == 0 and (charge_type in ['per kg', 'rate per kg'] or service_code == 'YY'):
                awb = line.get('awb_number')
                if awb:
                    cursor.execute('''
                        SELECT MAX(weight) FROM dhl_express_invoices 
                        WHERE awb_number = ? AND weight > 0
                    ''', (awb,))
                    shipment_weight_result = cursor.fetchone()
                    if shipment_weight_result and shipment_weight_result[0]:
                        weight = float(shipment_weight_result[0])
            
            # Calculate expected amount based on charge type
            if charge_type in ['per kg', 'rate per kg'] and charge_amount:
                # Per kg charge with potential minimum
                calculated_amount = float(charge_amount) * weight
                
                # Apply minimum charge if specified
                if minimum_charge and calculated_amount < float(minimum_charge):
                    expected_amount = float(minimum_charge)
                    comment = f'Service charge {service_code}: ${charge_amount:.2f}/kg × {weight}kg = ${calculated_amount:.2f}, minimum ${minimum_charge:.2f} applied'
                else:
                    expected_amount = calculated_amount
                    comment = f'Service charge {service_code}: ${charge_amount:.2f}/kg × {weight}kg = ${expected_amount:.2f}'
            elif charge_type == 'per shipment' and service_code == 'YY':
                # OVERWEIGHT PIECE - check weight criteria (>70kg)
                if weight > 70:
                    expected_amount = float(charge_amount) if charge_amount else 0
                    comment = f'Service charge {service_code}: ${expected_amount:.2f} (weight {weight}kg > 70kg threshold)'
                else:
                    # Weight doesn't exceed threshold, charge should not apply
                    expected_amount = 0
                    comment = f'Service charge {service_code}: $0.00 (weight {weight}kg ≤ 70kg threshold, charge not applicable)'
            else:
                # Fixed amount charge
                expected_amount = float(charge_amount) if charge_amount else 0
                comment = f'Service charge {service_code}: ${expected_amount:.2f} (fixed)'
            
            variance = expected_amount - line['amount']
            
            audit_result = 'PASS'
            
            if abs(variance) < 0.01:  # Small tolerance for rounding
                audit_result = 'PASS'
            elif variance > 0:
                comment += f' (DHL undercharged by ${variance:.2f})'
                audit_result = 'FAIL'
            elif variance < 0:
                audit_result = 'FAIL'
                comment += f' (DHL overcharged by ${abs(variance):.2f})'
                
            return {
                'expected_amount': expected_amount,
                'variance': variance,
                'audit_result': audit_result,
                'comments': [comment]
            }
    except Exception as e:
        print(f"Error in service charge lookup: {e}")
    
    # If it's a direct signature, use 7.90 as fallback
    if service_code == 'SF' or ('DIRECT SIGNATURE' in line.get('description', '').upper()):
        return {
            'expected_amount': 7.90,
            'variance': 7.90 - line['amount'],
            'audit_result': 'PASS',
            'comments': ['Service charge SF - DIRECT SIGNATURE accepted']
        }
        
    # If no service charge found, return neutral result
    return {
        'expected_amount': line['amount'],
        'variance': 0,
        'audit_result': 'REVIEW',
        'comments': [f'Service charge not found: {service_code}']
    }
