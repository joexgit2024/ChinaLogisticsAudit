"""
Invoice Routes for DHL Invoice Audit Application

Overall Purpose:
----------------
This blueprint handles all invoice-related routes including invoice listing,
detail views, filtering, and invoice-specific operations for the audit system.

Where This File is Used:
------------------------
- Registered as a blueprint in the main Flask application (app.py)
- Provides invoice management and viewing functionality
- Used by users to browse, search, and view individual invoice details
"""

import time
from flask import (
    Blueprint, render_template, request, redirect, url_for, flash
)
from app.database import get_db_connection

# Import auth decorator
try:
    from auth_routes import require_auth
except ImportError:
    # Fallback if auth is not available
    def require_auth(f):
        return f

# Create blueprint
invoice_bp = Blueprint('invoice', __name__)


@invoice_bp.route('/invoices')
@require_auth
def invoices(user_data=None):
    """View all invoices with filtering and pagination."""
    page = request.args.get('page', 1, type=int)
    per_page = 20
    
    # Get filter parameters
    status = request.args.get('status', '')
    search = request.args.get('search', '')
    date_from = request.args.get('date_from', '')
    date_to = request.args.get('date_to', '')
    
    try:
        conn = get_db_connection()

        # Build query conditions
        query_conditions = []
        query_params = []

        if status:
            query_conditions.append('audit_status = ?')
            query_params.append(status)

        if search:
            search_term = f'%{search}%'
            query_conditions.append(
                '(invoice_number LIKE ? OR shipper_name LIKE ? OR '
                'consignee_name LIKE ?)'
            )
            query_params.extend([search_term, search_term, search_term])

        if date_from:
            query_conditions.append('service_date >= ?')
            query_params.append(date_from)

        if date_to:
            query_conditions.append('service_date <= ?')
            query_params.append(date_to)

        # Build the WHERE clause if conditions exist
        where_clause = (
            " WHERE " + " AND ".join(query_conditions)
            if query_conditions else ""
        )

        # Get total count with filters
        count_query = (
            f'SELECT COUNT(*) FROM invoices{where_clause}'
        )
        cursor = conn.execute(count_query, query_params)
        total = cursor.fetchone()[0]

        # Get invoices for current page with filters
        offset = (page - 1) * per_page
        query_params.extend([per_page, offset])

        query = f'''
            SELECT id, invoice_number, shipper_name, consignee_name,
                   total_charges, weight, origin_port, destination_port,
                   shipper_country, consignee_country,
                   service_date, audit_status, created_at
            FROM invoices
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ? OFFSET ?
        '''

        invoices_list = conn.execute(query, query_params).fetchall()

        conn.close()

        # Add current timestamp to prevent caching
        current_time = int(time.time())

        return render_template(
            'invoices.html',
            invoices=invoices_list,
            page=page,
            total=total,
            per_page=per_page,
            now=current_time,
            status=status,
            search=search,
            date_from=date_from,
            date_to=date_to
        )
    except Exception as e:
        flash(f'Error loading invoices: {str(e)}', 'error')
        return render_template(
            'invoices.html',
            invoices=[],
            page=1,
            total=0,
            per_page=per_page,
            now=int(time.time()),
            status=status,
            search=search,
            date_from=date_from,
            date_to=date_to
        )


@invoice_bp.route('/invoice/<int:invoice_id>')
@require_auth
def invoice_detail(invoice_id, user_data=None):
    """View detailed invoice information by database ID."""
    try:
        conn = get_db_connection()
        
        # Get invoice details
        invoice = conn.execute('''
            SELECT * FROM invoices WHERE id = ?
        ''', (invoice_id,)).fetchone()
        
        if not invoice:
            flash('Invoice not found', 'error')
            return redirect(url_for('invoice.invoices'))
        
        # Get charges with enhanced details
        charges = conn.execute('''
            SELECT charge_type, description, rate, quantity, unit, amount
            FROM charges
            WHERE invoice_id = ?
            ORDER BY charge_type
        ''', (invoice_id,)).fetchall()

        # Get line items with enhanced details
        line_items = conn.execute('''
            SELECT line_number, item_description, weight, quantity, unit_type,
                   dimensions, volume
            FROM line_items
            WHERE invoice_id = ?
            ORDER BY line_number
        ''', (invoice_id,)).fetchall()
        
        # Get reference numbers
        reference_numbers = conn.execute('''
            SELECT reference_type, reference_value
            FROM reference_numbers
            WHERE invoice_id = ?
            ORDER BY reference_type
        ''', (invoice_id,)).fetchall()
        
        # Get shipment records
        shipments = conn.execute('''
            SELECT origin_location, destination_location, pickup_date,
                   delivery_date, service_type, weight, dimensions,
                   package_count, status
            FROM shipments
            WHERE invoice_id = ?
        ''', (invoice_id,)).fetchall()
        
        # Get audit rules (this table doesn't have invoice_id, so skip)
        audit_rules = []
        
        # Calculate aggregated weight and pieces
        total_weight_kg = sum(item['weight'] or 0.0 for item in line_items)
        total_pieces = sum(item['quantity'] or 0 for item in line_items)
        
        conn.close()

        return render_template(
            'invoice_detail.html',
            invoice=invoice,
            charges=charges,
            line_items=line_items,
            reference_numbers=reference_numbers,
            shipments=shipments,
            audit_rules=audit_rules,
            total_weight_kg=total_weight_kg,
            total_pieces=total_pieces
        )
    except Exception as e:
        print(f"Invoice detail error: {e}")
        import traceback
        traceback.print_exc()
        flash(f'Error loading invoice: {str(e)}', 'error')
        return redirect(url_for('invoice.invoices'))


@invoice_bp.route('/invoice/number/<invoice_number>')
@require_auth
def invoice_detail_by_number(invoice_number, user_data=None):
    """View detailed invoice information by invoice number."""
    print(
        f"DEBUG: Invoice detail requested for invoice number: "
        f"{invoice_number} (Type: {type(invoice_number).__name__})"
    )
    
    try:
        if not invoice_number:
            flash('Invalid invoice number', 'error')
            return redirect(url_for('invoice.invoices'))
            
        conn = get_db_connection()
        
        # Get invoice details by invoice number
        query = 'SELECT * FROM invoices WHERE invoice_number = ?'
        params = (str(invoice_number),)
        print(f"DEBUG: Executing query: {query} with params: {params}")
        
        invoice = conn.execute(query, params).fetchone()
        
        if not invoice:
            print(f"DEBUG: No invoice found with number {invoice_number}")
            flash(f'Invoice {invoice_number} not found', 'error')
            return redirect(url_for('invoice.invoices'))
        
        # Get the invoice ID for related data lookup
        invoice_id = invoice['id']
        
        # Get charges with enhanced details
        charges = conn.execute('''
            SELECT charge_type, description, rate, quantity, unit, amount
            FROM charges
            WHERE invoice_id = ?
            ORDER BY charge_type
        ''', (invoice_id,)).fetchall()

        # Get line items with enhanced details
        line_items = conn.execute('''
            SELECT line_number, item_description, weight, quantity, unit_type,
                   dimensions, volume
            FROM line_items
            WHERE invoice_id = ?
            ORDER BY line_number
        ''', (invoice_id,)).fetchall()
        
        # Get reference numbers
        reference_numbers = conn.execute('''
            SELECT reference_type, reference_value
            FROM reference_numbers
            WHERE invoice_id = ?
            ORDER BY reference_type
        ''', (invoice_id,)).fetchall()
        
        # Get shipment records
        shipments = conn.execute('''
            SELECT origin_location, destination_location, pickup_date,
                   delivery_date, service_type, weight, dimensions,
                   package_count, status
            FROM shipments
            WHERE invoice_id = ?
        ''', (invoice_id,)).fetchall()
        
        # Get audit rules (this table doesn't have invoice_id, so skip)
        audit_rules = []
        
        # Calculate aggregated weight and pieces
        total_weight_kg = sum(item['weight'] or 0.0 for item in line_items)
        total_pieces = sum(item['quantity'] or 0 for item in line_items)

        print(f"DEBUG: Found {len(charges)} charges for this invoice")
        print(f"DEBUG: Found {len(line_items)} line items")
        print(f"DEBUG: Found {len(reference_numbers)} reference numbers")
        print(f"DEBUG: Found {len(shipments)} shipments")
        print(f"DEBUG: Found {len(audit_rules)} audit rules")
        print(f"DEBUG: Total weight: {total_weight_kg} kg, "
              f"Total pieces: {total_pieces}")
        
        conn.close()
        
        # Return the template with comprehensive invoice data
        print("DEBUG: Rendering invoice_detail.html template")
        return render_template(
            'invoice_detail.html',
            invoice=invoice,
            charges=charges,
            line_items=line_items,
            reference_numbers=reference_numbers,
            shipments=shipments,
            audit_rules=audit_rules,
            total_weight_kg=total_weight_kg,
            total_pieces=total_pieces
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"ERROR: {str(e)}")
        flash(f'Error loading invoice: {str(e)}', 'error')
        return redirect(url_for('invoice.invoices'))
