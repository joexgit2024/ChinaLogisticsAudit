"""
Core Routes for DHL Invoice Audit Application

Overall Purpose:
----------------
This blueprint handles the main application routes including homepage,
dashboard, upload functionality, and basic navigation endpoints that are
central to the application's core functionality.

Where This File is Used:
------------------------
- Registered as a blueprint in the main Flask application (app.py)
- Provides core navigation and main user interface endpoints
- Used by users for primary application interaction and file uploads
"""

from flask import (
    Blueprint, render_template, request, redirect, url_for, flash
)
from app.database import get_db_connection
from enhanced_upload_routes import enhanced_upload_file, get_upload_status_api

# Import auth decorator
try:
    from auth_routes import require_auth
except ImportError:
    # Fallback if auth is not available
    def require_auth(f):
        return f

# Create blueprint
core_bp = Blueprint('core', __name__)


@core_bp.route('/')
@require_auth
def index(user_data=None):
    """Landing page showing application overview and main functions."""
    return render_template('index.html', user_data=user_data)


@core_bp.route('/dashboard')
@require_auth
def edi_invoice_dashboard(user_data=None):
    """EDI Invoice Dashboard showing invoice summary and audit results."""
    # Check if we have an invoice_no parameter for direct audit
    invoice_no = request.args.get('invoice_no')
    
    if invoice_no:
        # Redirect to enhanced audit for specific invoice
        return redirect(
            url_for(
                'enhanced_audit.enhanced_audit_detail',
                invoice_number=invoice_no
            )
        )
    
    try:
        conn = get_db_connection()
        
        # Get total invoices
        total_invoices = conn.execute(
            'SELECT COUNT(*) FROM invoices'
        ).fetchone()[0]
        
        # Get Ocean Rate Cards count
        ocean_rates_count = 0
        try:
            ocean_rates_count = conn.execute(
                'SELECT COUNT(*) FROM ocean_rate_cards'
            ).fetchone()[0]
        except Exception:
            # Table might not exist yet
            pass
        
        # Get recent invoices
        recent_invoices = conn.execute('''
         SELECT invoice_number, shipper_name, total_charges, created_at,
             audit_status
         FROM invoices
         ORDER BY created_at DESC
         LIMIT 10
     ''').fetchall()
        
        # Get audit summary
        audit_summary = conn.execute('''
            SELECT audit_status, COUNT(*) as count
            FROM invoices
            GROUP BY audit_status
        ''').fetchall()
        
        conn.close()
        
        return render_template(
            'edi_invoice_dashboard.html',
            total_invoices=total_invoices,
            ocean_rates_count=ocean_rates_count,
            recent_invoices=recent_invoices,
            audit_summary=audit_summary
        )
    except Exception as e:
        flash(f'Error loading dashboard: {str(e)}', 'error')
        return render_template(
            'edi_invoice_dashboard.html',
            total_invoices=0,
            ocean_rates_count=0,
            recent_invoices=[],
            audit_summary=[]
        )


@core_bp.route('/upload', methods=['GET', 'POST'])
@require_auth
def upload_file(user_data=None):
    """Enhanced upload and process EDI files with comprehensive validation."""
    return enhanced_upload_file()


@core_bp.route('/api/upload-status')
@require_auth
def upload_status(user_data=None):
    """API endpoint to get upload processing status."""
    return get_upload_status_api()
