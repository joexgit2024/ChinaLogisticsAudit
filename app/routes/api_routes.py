"""
API Routes for DHL Invoice Audit Application

Overall Purpose:
----------------
This blueprint handles API endpoints for invoice auditing, providing
programmatic access to audit functionality and status updates.

Where This File is Used:
------------------------
- Registered as a blueprint in the main Flask application (app.py)
- Provides REST API endpoints for invoice audit operations
- Used by frontend JavaScript and external integrations
"""

from flask import Blueprint, jsonify
from app.database import get_db_connection

# Import auth decorator
try:
    from auth_routes import require_auth_api
except ImportError:
    # Fallback if auth is not available
    def require_auth_api(f):
        return f

# Create blueprint
api_bp = Blueprint('api', __name__)


@api_bp.route('/api/audit/<int:invoice_id>', methods=['POST'])
@require_auth_api
def audit_invoice(invoice_id, user_data=None):
    """Perform audit on a specific invoice."""
    try:
        conn = get_db_connection()
        
        # Get invoice
        invoice = conn.execute(
            'SELECT * FROM invoices WHERE id = ?', (invoice_id,)
        ).fetchone()
        if not invoice:
            return jsonify({'error': 'Invoice not found'}), 404
        
        # Simple audit logic (can be expanded)
        audit_status = 'approved'
        audit_notes = []
        
        # Check for high charges - total_charges is at position 43
        total_charges = (
            invoice[43]
            if len(invoice) > 43 and invoice[43] else 0.0
        )
        try:
            if float(total_charges) > 1000:
                audit_notes.append('High charge amount requires review')
                audit_status = 'review'
        except (ValueError, TypeError):
            pass
        
        # Check for missing data
        # shipper_name at position 11, consignee_name at position 17
        shipper_name = invoice[11] if len(invoice) > 11 else None
        consignee_name = invoice[17] if len(invoice) > 17 else None
        
        if not shipper_name or not consignee_name:
            audit_notes.append('Missing shipper or consignee information')
            audit_status = 'review'
        
        # Update audit status (audit_status is at position 81)
        conn.execute('''
            UPDATE invoices
            SET audit_status = ?
            WHERE id = ?
        ''', (audit_status, invoice_id))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'status': audit_status,
            'notes': audit_notes
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
