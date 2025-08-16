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

# Import the FedEx audit engine
from fedex_audit_engine import FedExAuditEngine

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

@fedex_invoice_bp.route('/fedex/invoice/<invoice_no>/<awb_number>')
@require_auth
def invoice_detail(invoice_no, awb_number, user_data=None):
    """Detailed invoice view with tabbed organization for specific AWB"""
    
    conn = sqlite3.connect('fedex_audit.db')
    cursor = conn.cursor()
    
    # Get invoice data for specific AWB
    cursor.execute('SELECT * FROM fedex_invoices WHERE invoice_no = ? AND awb_number = ?', (invoice_no, awb_number))
    row = cursor.fetchone()
    
    if not row:
        flash(f'Invoice {invoice_no} with AWB {awb_number} not found', 'error')
        return redirect(url_for('fedex_invoices.invoice_list'))
    
    # Get column names
    cursor.execute('PRAGMA table_info(fedex_invoices)')
    columns = [col[1] for col in cursor.fetchall()]
    
    # Create invoice data dictionary
    invoice_data = dict(zip(columns, row))
    
    # Get all AWBs for this invoice (for navigation)
    cursor.execute('SELECT awb_number FROM fedex_invoices WHERE invoice_no = ? ORDER BY awb_number', (invoice_no,))
    all_awbs = [row[0] for row in cursor.fetchall()]
    current_awb_index = all_awbs.index(awb_number) if awb_number in all_awbs else 0
    
    # Determine previous and next AWB for navigation
    prev_awb = all_awbs[current_awb_index - 1] if current_awb_index > 0 else None
    next_awb = all_awbs[current_awb_index + 1] if current_awb_index < len(all_awbs) - 1 else None
    
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
                         awb_number=awb_number,
                         all_awbs=all_awbs,
                         current_awb_index=current_awb_index + 1,  # 1-based for display
                         total_awbs=len(all_awbs),
                         prev_awb=prev_awb,
                         next_awb=next_awb,
                         raw_data=raw_data,
                         related_invoices=related_invoices,
                         has_audit_result=has_audit_result,
                         audit_status=audit_status,
                         has_invoice_image=has_invoice_image)

@fedex_invoice_bp.route('/fedex/invoice/<invoice_no>')
@require_auth 
def invoice_detail_redirect(invoice_no, user_data=None):
    """Redirect old invoice URLs to first AWB for backward compatibility"""
    
    conn = sqlite3.connect('fedex_audit.db')
    cursor = conn.cursor()
    
    # Get first AWB for this invoice
    cursor.execute('SELECT awb_number FROM fedex_invoices WHERE invoice_no = ? ORDER BY awb_number LIMIT 1', (invoice_no,))
    row = cursor.fetchone()
    conn.close()
    
    if not row:
        flash(f'Invoice {invoice_no} not found', 'error')
        return redirect(url_for('fedex_invoices.invoice_list'))
    
    # Redirect to specific AWB URL
    return redirect(url_for('fedex_invoices.invoice_detail', invoice_no=invoice_no, awb_number=row[0]))

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


# =====================================================
# FedEx Batch Audit Routes
# =====================================================

@fedex_invoice_bp.route('/fedex/batch-audit')
# @require_auth  # Temporarily disabled for testing
def batch_audit_dashboard(user_data=None):
    """Display FedEx batch audit dashboard"""
    engine = FedExAuditEngine()
    
    # Get audit status summary
    audit_summary = engine.get_audit_status_summary()
    unaudited_invoices = engine.get_unaudited_invoices()
    
    return render_template('fedex_batch_audit.html', 
                         summary=audit_summary,
                         unaudited_count=len(unaudited_invoices),
                         unaudited_invoices=unaudited_invoices[:10])  # Show first 10

@fedex_invoice_bp.route('/fedex/batch-audit/status')
def get_batch_audit_status():
    """Get current FedEx batch audit status (API endpoint)"""
    engine = FedExAuditEngine()
    summary = engine.get_audit_status_summary()
    unaudited_invoices = engine.get_unaudited_invoices()
    
    return jsonify({
        'success': True,
        'summary': summary,
        'unaudited_invoices': len(unaudited_invoices),
        'sample_unaudited': unaudited_invoices[:5]
    })

@fedex_invoice_bp.route('/fedex/batch-audit/run', methods=['POST'])
def run_batch_audit():
    """Run FedEx batch audit on all unaudited invoices"""
    try:
        engine = FedExAuditEngine()
        
        # Get request parameters
        data = request.get_json() or {}
        audit_type = data.get('audit_type', 'all_unaudited')  # 'all_unaudited' or 'specific_invoices'
        specific_invoices = data.get('invoice_list', [])
        
        if audit_type == 'specific_invoices' and specific_invoices:
            # Audit specific invoices
            result = engine.audit_batch(specific_invoices)
        else:
            # Audit all unaudited invoices
            result = engine.audit_all_unaudited_invoices()
        
        return jsonify({
            'success': True,
            'batch_result': result
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@fedex_invoice_bp.route('/fedex/batch-audit/rerun', methods=['POST'])
def rerun_batch_audit():
    """Re-run FedEx batch audit on specific invoices"""
    try:
        engine = FedExAuditEngine()
        
        data = request.get_json() or {}
        invoice_list = data.get('invoice_list', [])
        
        if not invoice_list:
            return jsonify({
                'success': False,
                'error': 'No invoices provided for re-audit'
            }), 400
        
        # Clear existing audit results for these invoices
        conn = sqlite3.connect('fedex_audit.db')
        cursor = conn.cursor()
        
        for item in invoice_list:
            invoice_no = item.get('invoice_no')
            awb_number = item.get('awb_number')
            if invoice_no and awb_number:
                cursor.execute('''
                    DELETE FROM fedex_audit_results 
                    WHERE invoice_no = ? AND awb_number = ?
                ''', (invoice_no, awb_number))
        
        conn.commit()
        conn.close()
        
        # Re-run audit
        result = engine.audit_batch(invoice_list)
        
        return jsonify({
            'success': True,
            'batch_result': result
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@fedex_invoice_bp.route('/fedex/batch-audit/rerun-all', methods=['POST'])
# @require_auth_api  # Temporarily disabled for testing
def rerun_all_audits(user_data=None):
    """Re-run FedEx batch audit on ALL invoices"""
    try:
        logger.info("Starting rerun_all_audits")
        engine = FedExAuditEngine()
        logger.info("Created FedExAuditEngine")
        
        # Get ALL invoices from the database
        conn = sqlite3.connect('fedex_audit.db')
        cursor = conn.cursor()
        logger.info("Connected to database")
        
        # Get all invoices
        cursor.execute('''
            SELECT invoice_no, awb_number, service_type, service_abbrev,
                   direction, pieces, actual_weight_kg, chargeable_weight_kg,
                   origin_country, dest_country, origin_loc, ship_date,
                   delivery_datetime, exchange_rate, rated_amount_cny,
                   discount_amount_cny, fuel_surcharge_cny, other_surcharge_cny,
                   vat_amount_cny, total_awb_amount_cny, invoice_date
            FROM fedex_invoices
            ORDER BY invoice_date DESC
        ''')
        logger.info("Executed query to get invoices")
        
        invoices = []
        for row in cursor.fetchall():
            invoices.append({
                'invoice_no': row[0],
                'awb_number': row[1],
                'service_type': row[2],
                'service_abbrev': row[3],
                'direction': row[4],
                'pieces': row[5],
                'actual_weight_kg': row[6],
                'chargeable_weight_kg': row[7],
                'origin_country': row[8],
                'dest_country': row[9],
                'origin_loc': row[10],
                'ship_date': row[11],
                'delivery_datetime': row[12],
                'exchange_rate': row[13],
                'rated_amount_cny': row[14],
                'discount_amount_cny': row[15],
                'fuel_surcharge_cny': row[16],
                'other_surcharge_cny': row[17],
                'vat_amount_cny': row[18],
                'total_awb_amount_cny': row[19],
                'invoice_date': row[20]
            })
        
        # Clear ALL existing audit results
        cursor.execute('DELETE FROM fedex_audit_results')
        conn.commit()
        conn.close()
        
        if not invoices:
            return jsonify({
                'success': False,
                'error': 'No invoices found to audit'
            }), 400
        
        # Re-run audit on all invoices
        result = engine.audit_batch(invoices)
        
        return jsonify({
            'success': True,
            'message': f'Successfully re-audited {len(invoices)} invoices',
            'batch_result': result,
            'total_processed': len(invoices)
        })
        
    except Exception as e:
        logger.error(f"Error in rerun_all_audits: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@fedex_invoice_bp.route('/fedex/batch-audit/results')
# @require_auth  # Temporarily disabled for testing
def batch_audit_results(user_data=None):
    """Display FedEx batch audit results with filtering and pagination"""
    # Get query parameters
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 25, type=int)
    status_filter = request.args.get('status', 'all')
    sort_by = request.args.get('sort_by', 'audit_timestamp')
    sort_order = request.args.get('sort_order', 'desc')
    
    engine = FedExAuditEngine()
    
    # Calculate offset
    offset = (page - 1) * per_page
    
    # Get audit results
    results_data = engine.get_audit_results(
        limit=per_page, 
        offset=offset, 
        status_filter=status_filter
    )
    
    # Calculate pagination info
    total_count = results_data['total_count']
    total_pages = (total_count + per_page - 1) // per_page
    
    pagination_info = {
        'page': page,
        'per_page': per_page,
        'total': total_count,
        'total_pages': total_pages,
        'has_prev': page > 1,
        'has_next': page < total_pages,
        'prev_num': page - 1 if page > 1 else None,
        'next_num': page + 1 if page < total_pages else None
    }
    
    # Get summary statistics
    summary = engine.get_audit_status_summary()
    
    return render_template('fedex_batch_audit_results.html',
                         results=results_data['results'],
                         pagination=pagination_info,
                         summary=summary,
                         current_filter={'status': status_filter, 'sort_by': sort_by, 'sort_order': sort_order})

@fedex_invoice_bp.route('/fedex/batch-audit/export')
def export_audit_results():
    """Export FedEx audit results to Excel"""
    try:
        from io import BytesIO
        import pandas as pd
        from flask import send_file, make_response
        
        # Get query parameters
        status_filter = request.args.get('status', 'all')
        
        engine = FedExAuditEngine()
        results_data = engine.get_audit_results(status_filter=status_filter)
        
        if not results_data['results']:
            flash('No audit results found to export', 'warning')
            return redirect(url_for('fedex_invoices.batch_audit_results'))
        
        # Helpers to safely extract values
        def _to_float(v, default=0.0):
            try:
                if v is None:
                    return default
                if isinstance(v, (int, float)):
                    return float(v)
                s = str(v).replace(',', '').strip()
                return float(s)
            except Exception:
                return default

        def _variance_pct(row):
            vp = row.get('variance_percentage') if isinstance(row, dict) else None
            if isinstance(vp, (int, float)):
                return float(vp)
            # Try to compute from invoiced/expected
            inv = _to_float(row.get('invoiced_amount') if isinstance(row, dict) else None, None)
            exp = _to_float(row.get('expected_amount') if isinstance(row, dict) else None, None)
            if inv is not None and exp not in (None, 0):
                return (inv - exp) / exp * 100.0
            return 0.0

        # Create DataFrames for different sheets
        summary_data = engine.get_audit_status_summary()
        
        # Main results
        results_df = pd.DataFrame([
            {
                'Invoice No': r.get('invoice_no', '') if isinstance(r, dict) else '',
                'AWB Number': r.get('awb_number', '') if isinstance(r, dict) else '',
                'Status': r.get('status', '') if isinstance(r, dict) else '',
                'Route': r.get('route', '') if isinstance(r, dict) else '',
                'Weight (kg)': _to_float(r.get('actual_weight') if isinstance(r, dict) else None, ''),
                'Chargeable Weight (kg)': _to_float(r.get('chargeable_weight') if isinstance(r, dict) else None, ''),
                'Invoiced Amount (CNY)': _to_float(r.get('invoiced_amount') if isinstance(r, dict) else None, 0.0),
                'Expected Amount (CNY)': _to_float(r.get('expected_amount') if isinstance(r, dict) else None, 0.0),
                'Variance (CNY)': _to_float(r.get('variance') if isinstance(r, dict) else None, 0.0),
                'Variance %': f"{_variance_pct(r):.2f}%",
                'Audit Date': r.get('audit_timestamp', '') if isinstance(r, dict) else ''
            }
            for r in results_data['results']
        ])
        
        # Summary sheet
        summary_df = pd.DataFrame([
            {
                'Metric': 'Total Invoices',
                'Value': summary_data['total_invoices']
            },
            {
                'Metric': 'Total AWBs',
                'Value': summary_data['total_awbs']
            },
            {
                'Metric': 'Audited AWBs',
                'Value': summary_data['audited_awbs']
            },
            {
                'Metric': 'Pass Count',
                'Value': summary_data['pass_count']
            },
            {
                'Metric': 'Review Count',
                'Value': summary_data['review_count']
            },
            {
                'Metric': 'Fail Count',
                'Value': summary_data['fail_count']
            },
            {
                'Metric': 'Total Amount (CNY)',
                'Value': summary_data['total_amount']
            },
            {
                'Metric': 'Total Expected (CNY)',
                'Value': summary_data['total_expected']
            },
            {
                'Metric': 'Total Variance (CNY)',
                'Value': summary_data['total_variance']
            },
            {
                'Metric': 'Audit Completion Rate (%)',
                'Value': summary_data['audit_completion_rate']
            }
        ])
        
        # Create Excel file
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            results_df.to_excel(writer, sheet_name='Audit Results', index=False)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            
            # Format the worksheets
            workbook = writer.book
            
            # Format results sheet
            results_sheet = writer.sheets['Audit Results']
            for column in results_sheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                results_sheet.column_dimensions[column_letter].width = adjusted_width
        
        buffer.seek(0)
        
        # Create response
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'FedEx_Audit_Results_{timestamp}.xlsx'
        
        response = make_response(send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        ))
        
        return response
        
    except Exception as e:
        flash(f'Error exporting audit results: {str(e)}', 'error')
    return redirect(url_for('fedex_invoices.batch_audit_results'))

@fedex_invoice_bp.route('/fedex/batch-audit/export-detailed')
def export_detailed_audit_results():
    """Export detailed FedEx audit results with breakdown"""
    try:
        from io import BytesIO
        import pandas as pd
        from flask import send_file, make_response
        
        # Get query parameters
        status_filter = request.args.get('status', 'all')
        
        engine = FedExAuditEngine()
        results_data = engine.get_audit_results(status_filter=status_filter)
        
        if not results_data['results']:
            flash('No audit results found to export', 'warning')
            return redirect(url_for('fedex_invoices.batch_audit_results'))
        
        # Create detailed DataFrame
        detailed_data = []
        for r in results_data['results']:
            # Normalize audit_details (can be dict or JSON string)
            audit_details = {}
            if isinstance(r, dict):
                ad = r.get('audit_details')
                if isinstance(ad, dict):
                    audit_details = ad
                elif isinstance(ad, str):
                    try:
                        audit_details = json.loads(ad)
                    except Exception:
                        audit_details = {}
            
            def _get(key, default=''):
                return r.get(key, default) if isinstance(r, dict) else default

            def _variance_value():
                if isinstance(r, dict):
                    # Direct access to variance field from audit results
                    variance = r.get('variance')
                    if variance is not None:
                        try:
                            return float(variance)
                        except Exception:
                            pass
                return 0.0

            def _pct():
                try:
                    # Direct access to variance_percentage field from audit results
                    vp = r.get('variance_percentage')
                    if vp is not None:
                        return f"{float(vp):.2f}%"
                except Exception:
                    pass
                return "0.00%"
            detailed_data.append({
                'Invoice No': _get('invoice_no'),
                'AWB Number': _get('awb_number'),
                'Status': _get('status'),
                'Origin Country': audit_details.get('origin_country', ''),
                'Destination Country': audit_details.get('dest_country', ''),
                'Service Type': audit_details.get('service_type', ''),
                'Actual Weight (kg)': audit_details.get('actual_weight', ''),
                'Chargeable Weight (kg)': audit_details.get('chargeable_weight', ''),
                'Zone Applied': _get('zone_applied'),
                'Rate (USD)': _get('rate_applied'),
                'Base Cost (CNY)': _get('base_cost_expected'),
                'Fuel Surcharge (CNY)': _get('fuel_surcharge_expected'),
                'VAT (CNY)': _get('vat_expected'),
                'Expected Total (CNY)': _get('expected_amount', 0.0),
                'Invoiced Amount (CNY)': _get('invoiced_amount', 0.0),
                'Variance (CNY)': _variance_value(),
                'Variance %': _pct(),
                'Audit Date': _get('audit_timestamp')
            })
        
        detailed_df = pd.DataFrame(detailed_data)
        
        # Create Excel file
        buffer = BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            detailed_df.to_excel(writer, sheet_name='Detailed Audit Results', index=False)
            
            # Format the worksheet
            sheet = writer.sheets['Detailed Audit Results']
            for column in sheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                sheet.column_dimensions[column_letter].width = adjusted_width
        
        buffer.seek(0)
        
        # Create response
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'FedEx_Detailed_Audit_Results_{timestamp}.xlsx'
        
        response = make_response(send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        ))
        
        return response
        
    except Exception as e:
        flash(f'Error exporting detailed audit results: {str(e)}', 'error')
    return redirect(url_for('fedex_invoices.batch_audit_results'))
