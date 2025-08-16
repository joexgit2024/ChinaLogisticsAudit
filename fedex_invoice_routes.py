#!/usr/bin/env python3
"""
FedEx Invoice Management Routes
==============================

Enterprise-level invoice management system with filtering, sorting, 
pagination, and detailed invoice views with tabbed organization.
"""

from flask import Blueprint, request, render_template, redirect, url_for, flash, jsonify
import sqlite3
import json
from datetime import datetime, timedelta
from decimal import Decimal
import logging

# Import authentication
try:
    from auth_routes import require_auth, require_auth_api
except ImportError:
    def require_auth(f): return f
    def require_auth_api(f): return f

logger = logging.getLogger(__name__)

fedex_invoice_bp = Blueprint('fedex_invoices', __name__)

@fedex_invoice_bp.route('/fedex/invoices')
@require_auth
def invoice_list(user_data=None):
    """Main invoice list with advanced filtering and sorting"""
    
    # Get filter parameters
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 25))
    search = request.args.get('search', '').strip()
    service_filter = request.args.get('service', '').strip()
    country_filter = request.args.get('country', '').strip()
    direction_filter = request.args.get('direction', '').strip()
    date_from = request.args.get('date_from', '').strip()
    date_to = request.args.get('date_to', '').strip()
    amount_min = request.args.get('amount_min', '').strip()
    amount_max = request.args.get('amount_max', '').strip()
    sort_by = request.args.get('sort_by', 'invoice_date')
    sort_order = request.args.get('sort_order', 'desc')
    
    conn = sqlite3.connect('fedex_audit.db')
    cursor = conn.cursor()
    
    # Build dynamic query with filters
    where_conditions = []
    params = []
    
    if search:
        where_conditions.append('''(
            invoice_no LIKE ? OR 
            awb_number LIKE ? OR 
            origin_country LIKE ? OR
            dest_country LIKE ? OR
            origin_loc LIKE ?
        )''')
        search_param = f'%{search}%'
        params.extend([search_param] * 5)
    
    if service_filter:
        where_conditions.append('service_type = ?')
        params.append(service_filter)
    
    if country_filter:
        where_conditions.append('(origin_country = ? OR dest_country = ?)')
        params.extend([country_filter, country_filter])
    
    if direction_filter:
        where_conditions.append('direction = ?')
        params.append(direction_filter)
    
    if date_from:
        where_conditions.append('invoice_date >= ?')
        params.append(date_from)
    
    if date_to:
        where_conditions.append('invoice_date <= ?')
        params.append(date_to)
    
    if amount_min:
        where_conditions.append('total_awb_amount_cny >= ?')
        params.append(float(amount_min))
    
    if amount_max:
        where_conditions.append('total_awb_amount_cny <= ?')
        params.append(float(amount_max))
    
    where_clause = ''
    if where_conditions:
        where_clause = 'WHERE ' + ' AND '.join(where_conditions)
    
    # Validate sort parameters
    valid_sort_columns = [
        'invoice_no', 'invoice_date', 'awb_number', 'service_type', 
        'direction', 'origin_country', 'dest_country', 'pieces',
        'actual_weight_kg', 'chargeable_weight_kg', 'total_awb_amount_cny'
    ]
    
    if sort_by not in valid_sort_columns:
        sort_by = 'invoice_date'
    
    if sort_order not in ['asc', 'desc']:
        sort_order = 'desc'
    
    order_clause = f'ORDER BY {sort_by} {sort_order.upper()}'
    
    # Get total count for pagination
    count_query = f'SELECT COUNT(*) FROM fedex_invoices {where_clause}'
    cursor.execute(count_query, params)
    total_records = cursor.fetchone()[0]
    
    # Calculate pagination
    total_pages = (total_records + per_page - 1) // per_page
    offset = (page - 1) * per_page
    
    # Get invoice data
    data_query = f'''
        SELECT 
            id, invoice_no, invoice_date, awb_number, service_type, service_abbrev,
            direction, pieces, actual_weight_kg, chargeable_weight_kg,
            origin_country, dest_country, origin_loc, ship_date, delivery_datetime,
            rated_amount_cny, fuel_surcharge_cny, other_surcharge_cny, 
            total_awb_amount_cny, exchange_rate
        FROM fedex_invoices 
        {where_clause} 
        {order_clause} 
        LIMIT ? OFFSET ?
    '''
    
    cursor.execute(data_query, params + [per_page, offset])
    invoices = cursor.fetchall()
    
    # Get filter options for dropdowns
    cursor.execute('SELECT DISTINCT service_type FROM fedex_invoices WHERE service_type IS NOT NULL ORDER BY service_type')
    service_types = [row[0] for row in cursor.fetchall()]
    
    cursor.execute('SELECT DISTINCT origin_country FROM fedex_invoices WHERE origin_country IS NOT NULL UNION SELECT DISTINCT dest_country FROM fedex_invoices WHERE dest_country IS NOT NULL ORDER BY 1')
    countries = [row[0] for row in cursor.fetchall()]
    
    cursor.execute('SELECT DISTINCT direction FROM fedex_invoices WHERE direction IS NOT NULL ORDER BY direction')
    directions = [row[0] for row in cursor.fetchall()]
    
    # Get summary statistics
    stats_query = f'''
        SELECT 
            COUNT(*) as total_invoices,
            SUM(pieces) as total_pieces,
            SUM(actual_weight_kg) as total_weight,
            SUM(total_awb_amount_cny) as total_amount,
            AVG(total_awb_amount_cny) as avg_amount,
            MIN(invoice_date) as date_range_start,
            MAX(invoice_date) as date_range_end
        FROM fedex_invoices 
        {where_clause}
    '''
    
    cursor.execute(stats_query, params)
    stats = cursor.fetchone()
    
    conn.close()
    
    # Prepare template data
    template_data = {
        'invoices': invoices,
        'total_records': total_records,
        'total_pages': total_pages,
        'current_page': page,
        'per_page': per_page,
        'search': search,
        'service_filter': service_filter,
        'country_filter': country_filter,
        'direction_filter': direction_filter,
        'date_from': date_from,
        'date_to': date_to,
        'amount_min': amount_min,
        'amount_max': amount_max,
        'sort_by': sort_by,
        'sort_order': sort_order,
        'service_types': service_types,
        'countries': countries,
        'directions': directions,
        'stats': {
            'total_invoices': stats[0] or 0,
            'total_pieces': stats[1] or 0,
            'total_weight': stats[2] or 0,
            'total_amount': stats[3] or 0,
            'avg_amount': stats[4] or 0,
            'date_range_start': stats[5],
            'date_range_end': stats[6]
        }
    }
    
    return render_template('fedex_invoice_list.html', **template_data)

@fedex_invoice_bp.route('/fedex/invoice/<invoice_no>')
@require_auth
def invoice_detail(invoice_no, user_data=None):
    """Detailed invoice view with tabbed organization"""
    
    conn = sqlite3.connect('fedex_audit.db')
    cursor = conn.cursor()
    
    # Get invoice data
    cursor.execute('SELECT * FROM fedex_invoices WHERE invoice_no = ?', (invoice_no,))
    row = cursor.fetchone()
    
    if not row:
        flash(f'Invoice {invoice_no} not found', 'error')
        return redirect(url_for('fedex_invoices.invoice_list'))
    
    # Get column names
    cursor.execute('PRAGMA table_info(fedex_invoices)')
    columns = [col[1] for col in cursor.fetchall()]
    
    # Create invoice data dictionary
    invoice_data = dict(zip(columns, row))
    
    # Parse raw JSON if available
    raw_data = None
    if invoice_data.get('raw_json'):
        try:
            raw_data = json.loads(invoice_data['raw_json'])
        except:
            raw_data = None
    
    # Get related invoices (same AWB or similar route)
    cursor.execute('''
        SELECT invoice_no, invoice_date, service_type, direction, total_awb_amount_cny
        FROM fedex_invoices 
        WHERE (awb_number = ? OR (origin_country = ? AND dest_country = ?)) 
        AND invoice_no != ?
        ORDER BY invoice_date DESC
        LIMIT 10
    ''', (invoice_data['awb_number'], invoice_data['origin_country'], 
          invoice_data['dest_country'], invoice_no))
    
    related_invoices = cursor.fetchall()
    
    # Check for audit results (placeholder for future implementation)
    has_audit_result = False
    audit_status = 'pending'
    
    # Check for invoice images (placeholder for future implementation)  
    has_invoice_image = False
    
    conn.close()
    
    return render_template('fedex_invoice_detail.html',
                         invoice_data=invoice_data,
                         invoice_no=invoice_no,
                         raw_data=raw_data,
                         related_invoices=related_invoices,
                         has_audit_result=has_audit_result,
                         audit_status=audit_status,
                         has_invoice_image=has_invoice_image)

@fedex_invoice_bp.route('/fedex/invoices/export')
@require_auth
def export_invoices(user_data=None):
    """Export filtered invoice data to CSV"""
    # Implementation for CSV export
    # This can be added later as needed
    flash('Export functionality will be implemented soon', 'info')
    return redirect(url_for('fedex_invoices.invoice_list'))

@fedex_invoice_bp.route('/fedex/api/invoice-summary/<invoice_no>')
@require_auth_api
def invoice_summary_api(invoice_no, user_data=None):
    """API endpoint for quick invoice summary"""
    
    conn = sqlite3.connect('fedex_audit.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT invoice_no, invoice_date, service_type, direction,
               origin_country, dest_country, pieces, actual_weight_kg,
               total_awb_amount_cny
        FROM fedex_invoices 
        WHERE invoice_no = ?
    ''', (invoice_no,))
    
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        return jsonify({'error': 'Invoice not found'}), 404
    
    return jsonify({
        'invoice_no': row[0],
        'invoice_date': row[1],
        'service_type': row[2],
        'direction': row[3],
        'origin_country': row[4],
        'dest_country': row[5],
        'pieces': row[6],
        'weight_kg': row[7],
        'total_amount_cny': row[8]
    })

@fedex_invoice_bp.route('/fedex/api/invoices/stats')
@require_auth_api
def invoices_stats_api(user_data=None):
    """API endpoint for invoice statistics"""
    
    conn = sqlite3.connect('fedex_audit.db')
    cursor = conn.cursor()
    
    # Get overall statistics
    cursor.execute('''
        SELECT 
            COUNT(*) as total_invoices,
            SUM(pieces) as total_pieces,
            SUM(actual_weight_kg) as total_weight,
            SUM(total_awb_amount_cny) as total_amount,
            COUNT(DISTINCT service_type) as service_types,
            COUNT(DISTINCT origin_country) as origin_countries,
            COUNT(DISTINCT dest_country) as dest_countries
        FROM fedex_invoices
    ''')
    
    stats = cursor.fetchone()
    
    # Get service breakdown
    cursor.execute('''
        SELECT service_type, COUNT(*), SUM(total_awb_amount_cny)
        FROM fedex_invoices 
        WHERE service_type IS NOT NULL
        GROUP BY service_type
        ORDER BY COUNT(*) DESC
    ''')
    
    service_breakdown = cursor.fetchall()
    
    # Get monthly trends
    cursor.execute('''
        SELECT 
            strftime('%Y-%m', invoice_date) as month,
            COUNT(*) as invoice_count,
            SUM(total_awb_amount_cny) as total_amount
        FROM fedex_invoices 
        WHERE invoice_date IS NOT NULL
        GROUP BY strftime('%Y-%m', invoice_date)
        ORDER BY month DESC
        LIMIT 12
    ''')
    
    monthly_trends = cursor.fetchall()
    
    conn.close()
    
    return jsonify({
        'overall_stats': {
            'total_invoices': stats[0] or 0,
            'total_pieces': stats[1] or 0,
            'total_weight': stats[2] or 0,
            'total_amount': stats[3] or 0,
            'service_types': stats[4] or 0,
            'origin_countries': stats[5] or 0,
            'dest_countries': stats[6] or 0
        },
        'service_breakdown': [
            {'service': row[0], 'count': row[1], 'amount': row[2]}
            for row in service_breakdown
        ],
        'monthly_trends': [
            {'month': row[0], 'count': row[1], 'amount': row[2]}
            for row in monthly_trends
        ]
    })
