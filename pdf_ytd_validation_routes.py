#!/usr/bin/env python3
"""
PDF-YTD Validation Routes for Web Interface
===========================================
"""

from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
import sqlite3
import json
from simple_pdf_validator import SimplePDFValidator

# Import authentication
try:
    from auth_routes import require_auth, require_auth_api
except ImportError:
    def require_auth(f):
        return f
    def require_auth_api(f):
        return f

# Create blueprint
pdf_ytd_bp = Blueprint('pdf_ytd', __name__)

# Initialize validator
validator = SimplePDFValidator()

@pdf_ytd_bp.route('/pdf-ytd-validation')
@require_auth
def validation_dashboard(user_data=None):
    """PDF-YTD validation dashboard"""
    
    # Get all validation results
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Check if validation table exists
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='pdf_ytd_validation_simple'
    """)
    
    if not cursor.fetchone():
        # Create table
        cursor.execute('''
            CREATE TABLE pdf_ytd_validation_simple (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_no TEXT UNIQUE,
                ytd_total REAL,
                pdf_total REAL,
                currency TEXT,
                variance_percent REAL,
                validation_status TEXT,
                validation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        conn.commit()
    
    cursor.execute('''
        SELECT invoice_no, validation_status, ytd_total, pdf_total,
               currency, variance_percent, validation_timestamp
        FROM pdf_ytd_validation_simple 
        ORDER BY validation_timestamp DESC
        LIMIT 50
    ''')
    
    validations = []
    for row in cursor.fetchall():
        validations.append({
            'invoice_no': row[0],
            'status': row[1],
            'ytd_total': row[2],
            'pdf_total': row[3],
            'currency': row[4],
            'variance_percent': row[5],
            'needs_review': row[5] > 10 if row[5] else False,  # Auto-flag if variance > 10%
            'timestamp': row[6]
        })
    
    conn.close()
    
    return render_template('pdf_ytd_validation.html', validations=validations)

@pdf_ytd_bp.route('/pdf-ytd-validation/validate/<invoice_no>', methods=['POST'])
@require_auth_api  
def validate_invoice(invoice_no, user_data=None):
    """Validate a specific invoice (AJAX)"""
    
    try:
        result = validator.validate_invoice_simple(invoice_no)
        
        if 'error' in result:
            return jsonify({
                'success': False,
                'error': result['error']
            }), 400
        
        return jsonify({
            'success': True,
            'result': {
                'invoice_no': result['invoice_no'],
                'status': result['status'],
                'ytd_total': result.get('validation_summary', {}).get('ytd_total', 0),
                'pdf_total': result.get('validation_summary', {}).get('pdf_total', 0),
                'variance_percent': result.get('validation_summary', {}).get('variance_percent', 0),
                'currency': result.get('pdf_data', {}).get('currency', 'USD')
            }
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Validation error: {str(e)}'
        }), 500

@pdf_ytd_bp.route('/pdf-ytd-validation/details/<invoice_no>')
@require_auth
def validation_details(invoice_no, user_data=None):
    """Show detailed validation results"""
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT * FROM pdf_ytd_validation_simple WHERE invoice_no = ?
    ''', (invoice_no,))
    
    row = cursor.fetchone()
    
    if not row:
        flash('Validation details not found')
        return redirect(url_for('pdf_ytd.validation_dashboard'))
    
    # Simple data structure for the working validator
    validation_data = {
        'invoice_no': row[1],
        'ytd_total_amount': row[2],
        'pdf_total_amount': row[3],
        'pdf_currency': row[4],
        'total_variance_percent': row[5],
        'validation_status': row[6],
        'validation_timestamp': row[7],
        'ytd_charge_breakdown': {},  # Could be enhanced later
        'pdf_detailed_charges': [],  # Could be enhanced later
        'manual_review_required': row[5] > 10 if row[5] else False,
        'validation_summary': {
            'variance_percentage': row[5],
            'amount_difference': abs(row[2] - row[3]) if row[2] and row[3] else 0,
            'status': row[6]
        }
    }
    
    conn.close()
    
    return render_template('pdf_ytd_details.html', data=validation_data)

# Register this blueprint in app.py:
# from pdf_ytd_validation_routes import pdf_ytd_bp
# app.register_blueprint(pdf_ytd_bp)
