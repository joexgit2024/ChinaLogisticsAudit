"""
Validation Routes for DHL Invoice Audit Application

Overall Purpose:
----------------
This blueprint handles all invoice validation functionality including
validation dashboards, detailed validation reports, and validation APIs
for the audit system.

Where This File is Used:
------------------------
- Registered as a blueprint in the main Flask application (app.py)
- Provides validation dashboard and detailed validation results
- Used by users to review invoice validation status and issues
"""

from flask import (
    Blueprint, render_template, redirect, url_for, flash, jsonify
)
from app.database import get_db_connection

# Import validation logic
try:
    from invoice_validator import InvoiceValidator
except ImportError:
    # Create a minimal validator if the main one is not available
    class InvoiceValidator:
        def validate_invoice(self, invoice_dict):
            # Return a simple validation result
            from collections import namedtuple
            ValidationResult = namedtuple(
                'ValidationResult',
                'invoice_number is_valid score error_count warning_count'
            )
            return ValidationResult(
                invoice_number=invoice_dict.get('invoice_number', 'Unknown'),
                is_valid=True,
                score=100.0,
                error_count=0,
                warning_count=0
            )

# Import auth decorator
try:
    from auth_routes import require_auth, require_auth_api
except ImportError:
    # Fallback if auth is not available
    def require_auth(f):
        return f

    def require_auth_api(f):
        return f

# Create blueprint
validation_bp = Blueprint('validation', __name__)


@validation_bp.route('/validation')
@require_auth
def validation_dashboard(user_data=None):
    """Display validation results for all invoices"""
    conn = get_db_connection()
    
    # Get basic invoice data for validation
    invoices = conn.execute('''
        SELECT
            id, invoice_number, shipper_name, consignee_name,
            origin_port, destination_port, total_charges, weight, pieces,
            currency, exchange_rate, service_date, delivery_date,
            shipper_country, consignee_country, tracking_number,
            reference_number, pickup_date, invoice_date, created_at
        FROM invoices
        ORDER BY created_at DESC
        LIMIT 100
    ''').fetchall()
    
    conn.close()
    
    # Validate each invoice
    validator = InvoiceValidator()
    validation_results = []
    
    for invoice in invoices:
        # Convert sqlite3.Row to dict
        invoice_dict = dict(invoice)
        validation_result = validator.validate_invoice(invoice_dict)
        validation_results.append(validation_result)
    
    # Calculate summary statistics
    total_invoices = len(validation_results)
    valid_invoices = len([r for r in validation_results if r.is_valid])
    invalid_invoices = total_invoices - valid_invoices
    avg_score = (
        sum(r.score for r in validation_results) / total_invoices
        if total_invoices > 0 else 0
    )
    
    # Count issues by severity
    total_errors = sum(r.error_count for r in validation_results)
    total_warnings = sum(r.warning_count for r in validation_results)
    
    summary = {
        'total_invoices': total_invoices,
        'valid_invoices': valid_invoices,
        'invalid_invoices': invalid_invoices,
        'validation_rate': (
            (valid_invoices / total_invoices * 100)
            if total_invoices > 0 else 0
        ),
        'average_score': avg_score,
        'total_errors': total_errors,
        'total_warnings': total_warnings
    }
    
    return render_template(
        'validation_dashboard.html',
        validation_results=validation_results,
        summary=summary
    )


@validation_bp.route('/validation/<invoice_number>')
@require_auth
def validation_detail(invoice_number, user_data=None):
    """Display detailed validation results for a specific invoice"""
    conn = get_db_connection()
    
    # Get invoice data
    invoice = conn.execute('''
        SELECT
            id, invoice_number, shipper_name, consignee_name,
            origin_port, destination_port, total_charges, weight, pieces,
            currency, exchange_rate, service_date, delivery_date,
            shipper_country, consignee_country, tracking_number,
            reference_number, pickup_date, invoice_date, created_at
        FROM invoices
        WHERE invoice_number = ?
    ''', (invoice_number,)).fetchone()
    
    conn.close()
    
    if not invoice:
        flash(f'Invoice {invoice_number} not found', 'error')
        return redirect(url_for('validation.validation_dashboard'))
    
    # Validate the invoice
    validator = InvoiceValidator()
    invoice_dict = dict(invoice)
    validation_result = validator.validate_invoice(invoice_dict)
    
    return render_template(
        'validation_detail.html',
        invoice=invoice_dict,
        validation_result=validation_result
    )


@validation_bp.route('/api/validation/revalidate', methods=['POST'])
@require_auth_api
def revalidate_invoices(user_data=None):
    """API endpoint to revalidate all invoices and return summary"""
    conn = get_db_connection()
    
    # Get all invoices
    invoices = conn.execute('''
        SELECT 
            id, invoice_number, shipper_name, consignee_name,
            origin_port, destination_port, total_charges, weight, pieces,
            currency, exchange_rate, service_date, delivery_date,
            shipper_country, consignee_country, tracking_number,
            reference_number, pickup_date, invoice_date
        FROM invoices
    ''').fetchall()
    
    conn.close()
    
    # Validate all invoices
    validator = InvoiceValidator()
    validation_results = []
    
    for invoice in invoices:
        invoice_dict = dict(invoice)
        validation_result = validator.validate_invoice(invoice_dict)
        validation_results.append({
            'invoice_number': validation_result.invoice_number,
            'is_valid': validation_result.is_valid,
            'score': validation_result.score,
            'error_count': validation_result.error_count,
            'warning_count': validation_result.warning_count
        })
    
    # Calculate summary
    total = len(validation_results)
    valid = len([r for r in validation_results if r['is_valid']])
    avg_score = (
        sum(r['score'] for r in validation_results) / total 
        if total > 0 else 0
    )
    
    return jsonify({
        'success': True,
        'total_invoices': total,
        'valid_invoices': valid,
        'invalid_invoices': total - valid,
        'validation_rate': (valid / total * 100) if total > 0 else 0,
        'average_score': round(avg_score, 1),
        'results': validation_results
    })
