"""
Air Freight Audit functionality
"""
from app.models.rate_card import get_applicable_rate
from app.database import get_db_connection

def audit_air_invoice(invoice_id):
    """
    Audit an air freight invoice by comparing charges against rate card.
    
    Args:
        invoice_id: ID of the invoice to audit
        
    Returns:
        dict: Audit results
    """
    conn = get_db_connection()
    
    # Get invoice details
    invoice = conn.execute('''
        SELECT * FROM invoices WHERE id = ?
    ''', (invoice_id,)).fetchone()
    
    if not invoice:
        conn.close()
        return {
            'success': False,
            'message': 'Invoice not found'
        }
    
    # Check if this is an air freight invoice
    shipping_mode = invoice['shipping_mode']
    if shipping_mode is None or 'air' not in shipping_mode.lower():
        conn.close()
        return {
            'success': False,
            'message': 'Not an air freight invoice'
        }
    
    # Get shipment details for origin/destination
    origin_country = invoice['shipper_country']
    destination_country = invoice['consignee_country']
    
    # Get weight and charges
    weight_kg = invoice['weight'] or 0.0
    if weight_kg <= 0:
        weight_kg = invoice['bill_weight'] or 0.0
    if weight_kg <= 0:
        weight_kg = invoice['ship_weight'] or 0.0
        
    total_charges = invoice['total_charges'] or 0.0
    
    # If we don't have enough information, we can't audit
    if not origin_country or not destination_country or weight_kg <= 0:
        conn.close()
        return {
            'success': False,
            'message': 'Missing required information for audit (origin, destination, or weight)'
        }
    
    # Lookup rate from rate card
    rate = get_applicable_rate(origin_country, destination_country, weight_kg)
    
    if not rate:
        conn.close()
        return {
            'success': False,
            'message': 'No applicable rate found in rate card'
        }
    
    # Compare actual charges to expected charges
    expected_cost = rate['total_cost'] or 0.0
    actual_charges = total_charges or 0.0
    variance = actual_charges - expected_cost
    variance_pct = (variance / expected_cost) * 100 if expected_cost > 0 else 0
    
    # Create audit result
    audit_result = {
        'success': True,
        'invoice_id': invoice_id,
        'invoice_number': invoice['invoice_number'],
        'shipping_mode': shipping_mode,
        'origin': origin_country,
        'destination': destination_country,
        'weight_kg': weight_kg,
        'actual_charges': actual_charges,
        'expected_charges': expected_cost,
        'variance': variance,
        'variance_pct': variance_pct,
        'lane_id': rate.get('lane_id', 'N/A'),
        'rate_details': rate
    }
    
    # Determine audit status
    if abs(variance_pct) <= 5:
        # Within 5% tolerance
        audit_result['status'] = 'approved'
        audit_result['message'] = 'Invoice amount within tolerance of expected rate'
    elif variance_pct < -5:
        # More than 5% below expected
        audit_result['status'] = 'review'
        audit_result['message'] = 'Invoice amount significantly lower than expected rate'
    else:
        # More than 5% above expected
        audit_result['status'] = 'flagged'
        audit_result['message'] = 'Invoice amount significantly higher than expected rate'
    
    # Store audit result in database
    try:
        # Update invoice audit status
        conn.execute('''
            UPDATE invoices 
            SET audit_status = ?,
                audit_notes = ?
            WHERE id = ?
        ''', (audit_result['status'], audit_result['message'], invoice_id))
        
        # Store detailed audit result
        conn.execute('''
            INSERT OR REPLACE INTO audit_results (
                invoice_id, expected_amount, variance_amount, variance_percent,
                rate_card_entry_id, audit_details, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        ''', (
            invoice_id, 
            expected_cost,
            variance,
            variance_pct,
            rate['id'],
            str(audit_result),
            
        ))
        
        conn.commit()
    except Exception as e:
        print(f"Error storing audit result: {e}")
        # Continue anyway as we want to return the result
    
    conn.close()
    return audit_result
