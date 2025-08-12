#!/usr/bin/env python3
"""
DHL YTD Upload Routes
====================

Flask routes for handling DHL Year-to-Date invoice report uploads.
"""

from flask import Blueprint, request, render_template, redirect, url_for, flash, jsonify
import os
from werkzeug.utils import secure_filename
from dhl_ytd_processor import DHLYTDProcessor
import logging
from datetime import datetime

# Import authentication
try:
    from auth_routes import require_auth, require_auth_api
except ImportError:
    # Fallback if auth is not available
    def require_auth(f):
        return f
    def require_auth_api(f):
        return f

# Set up logging
logger = logging.getLogger(__name__)

# Create blueprint
dhl_ytd_bp = Blueprint('dhl_ytd', __name__)

# Configuration
ALLOWED_EXTENSIONS = {'csv'}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB

def allowed_file(filename):
    """Check if file has allowed extension"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@dhl_ytd_bp.route('/dhl-ytd-upload', methods=['GET', 'POST'])
@require_auth
def upload_ytd_file(user_data=None):
    """Upload and process DHL YTD CSV files"""
    if request.method == 'GET':
        # Get recent uploads for display
        processor = DHLYTDProcessor()
        
        # Get recent upload history
        import sqlite3
        conn = sqlite3.connect(processor.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT batch_id, filename, total_records, processed_records, 
                   failed_records, duplicate_records, processing_status, 
                   created_at, error_message
            FROM dhl_ytd_uploads 
            ORDER BY created_at DESC 
            LIMIT 10
        ''')
        
        uploads = []
        for row in cursor.fetchall():
            uploads.append({
                'batch_id': row[0],
                'filename': row[1],
                'total_records': row[2] or 0,
                'processed_records': row[3] or 0,
                'failed_records': row[4] or 0,
                'duplicate_records': row[5] or 0,
                'status': row[6],
                'created_at': row[7],
                'error_message': row[8]
            })
        
        conn.close()
        
        return render_template('dhl_ytd_upload.html', uploads=uploads)
    
    # Handle POST request
    if 'file' not in request.files:
        flash('No file selected', 'error')
        return redirect(request.url)
    
    file = request.files['file']
    
    if file.filename == '':
        flash('No file selected', 'error')
        return redirect(request.url)
    
    if not allowed_file(file.filename):
        flash('Only CSV files are allowed', 'error')
        return redirect(request.url)
    
    # Check file size
    file.seek(0, os.SEEK_END)
    file_size = file.tell()
    file.seek(0)
    
    if file_size > MAX_FILE_SIZE:
        flash('File too large. Maximum size is 50MB', 'error')
        return redirect(request.url)
    
    try:
        # Save uploaded file
        filename = secure_filename(file.filename)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        unique_filename = f"dhl_ytd_{timestamp}_{filename}"
        
        upload_dir = os.path.join(os.getcwd(), 'uploads')
        os.makedirs(upload_dir, exist_ok=True)
        
        file_path = os.path.join(upload_dir, unique_filename)
        file.save(file_path)
        
        # Process the file
        processor = DHLYTDProcessor()
        batch_id = f"dhl_ytd_{timestamp}"
        
        result = processor.process_csv_file(file_path, batch_id)
        
        # Clean up uploaded file after processing
        try:
            os.remove(file_path)
        except OSError:
            pass
        
        # Show results
        flash(f'File processed successfully! {result["processed_records"]} records imported, '
              f'{result["duplicate_records"]} duplicates skipped, '
              f'{result["failed_records"]} failed', 'success')
        
        if result['errors']:
            flash(f'Some errors occurred: {"; ".join(result["errors"][:3])}...', 'warning')
        
        return redirect(url_for('dhl_ytd.upload_ytd_file'))
        
    except Exception as e:
        logger.error(f"Error processing YTD upload: {e}")
        flash(f'Error processing file: {str(e)}', 'error')
        
        # Clean up file if it exists
        try:
            if 'file_path' in locals():
                os.remove(file_path)
        except OSError:
            pass
        
        return redirect(request.url)

@dhl_ytd_bp.route('/dhl-ytd-status/<batch_id>')
@require_auth_api
def get_upload_status(batch_id, user_data=None):
    """Get upload processing status via AJAX"""
    processor = DHLYTDProcessor()
    status = processor.get_upload_status(batch_id)
    
    if status:
        return jsonify(status)
    else:
        return jsonify({'error': 'Batch not found'}), 404

@dhl_ytd_bp.route('/dhl-ytd-data')
@require_auth
def view_ytd_data(user_data=None):
    """View DHL YTD data with filtering and search"""
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 50))
    search = request.args.get('search', '').strip()
    currency_filter = request.args.get('currency', '').strip()
    date_from = request.args.get('date_from', '').strip()
    date_to = request.args.get('date_to', '').strip()
    
    processor = DHLYTDProcessor()
    
    import sqlite3
    conn = sqlite3.connect(processor.db_path)
    cursor = conn.cursor()
    
    # Build query with filters
    where_conditions = []
    params = []
    
    if search:
        where_conditions.append('''(
            invoice_no LIKE ? OR 
            shipper_name LIKE ? OR 
            consignee_name LIKE ? OR
            commodity LIKE ? OR
            masterbill LIKE ?
        )''')
        search_param = f'%{search}%'
        params.extend([search_param] * 5)
    
    if currency_filter:
        where_conditions.append('invoice_currency = ?')
        params.append(currency_filter)
    
    if date_from:
        where_conditions.append('invoice_creation_date >= ?')
        params.append(date_from)
    
    if date_to:
        where_conditions.append('invoice_creation_date <= ?')
        params.append(date_to)
    
    where_clause = ''
    if where_conditions:
        where_clause = 'WHERE ' + ' AND '.join(where_conditions)
    
    # Get total count
    cursor.execute(f'''
        SELECT COUNT(*) FROM dhl_ytd_invoices {where_clause}
    ''', params)
    total_records = cursor.fetchone()[0]
    
    # Get page data
    offset = (page - 1) * per_page
    cursor.execute(f'''
        SELECT invoice_no, invoice_type, invoice_currency, invoice_creation_date,
               shipper_name, consignee_name, commodity, transportation_mode,
               total_charges_without_duty_tax, total_charges_with_duty_tax,
               total_charges_without_duty_tax_usd, total_charges_with_duty_tax_usd,
               masterbill, housebill, processed_date, upload_batch_id
        FROM dhl_ytd_invoices {where_clause}
        ORDER BY invoice_creation_date DESC, invoice_no
        LIMIT ? OFFSET ?
    ''', params + [per_page, offset])
    
    records = []
    for row in cursor.fetchall():
        records.append({
            'invoice_no': row[0],
            'invoice_type': row[1],
            'invoice_currency': row[2],
            'invoice_creation_date': row[3],
            'shipper_name': row[4],
            'consignee_name': row[5],
            'commodity': row[6],
            'transportation_mode': row[7],
            'total_without_tax': row[8],
            'total_with_tax': row[9],
            'total_without_tax_usd': row[10],
            'total_with_tax_usd': row[11],
            'masterbill': row[12],
            'housebill': row[13],
            'processed_date': row[14],
            'upload_batch_id': row[15]
        })
    
    # Get available currencies for filter
    cursor.execute('SELECT DISTINCT invoice_currency FROM dhl_ytd_invoices WHERE invoice_currency IS NOT NULL ORDER BY invoice_currency')
    currencies = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    # Pagination info
    total_pages = (total_records + per_page - 1) // per_page
    has_prev = page > 1
    has_next = page < total_pages
    
    return render_template('dhl_ytd_data.html', 
                         records=records,
                         page=page,
                         per_page=per_page,
                         total_records=total_records,
                         total_pages=total_pages,
                         has_prev=has_prev,
                         has_next=has_next,
                         search=search,
                         currency_filter=currency_filter,
                         date_from=date_from,
                         date_to=date_to,
                         currencies=currencies)

@dhl_ytd_bp.route('/dhl-ytd-invoice/<invoice_no>')
@require_auth
def view_ytd_invoice_detail(invoice_no, user_data=None):
    """View detailed information for a specific YTD invoice"""
    processor = DHLYTDProcessor()
    
    import sqlite3
    conn = sqlite3.connect(processor.db_path)
    cursor = conn.cursor()
    
    cursor.execute('SELECT * FROM dhl_ytd_invoices WHERE invoice_no = ?', (invoice_no,))
    row = cursor.fetchone()
    
    if not row:
        flash(f'Invoice {invoice_no} not found in YTD data', 'error')
        return redirect(url_for('dhl_ytd.view_ytd_data'))
    
    # Get column names
    cursor.execute('PRAGMA table_info(dhl_ytd_invoices)')
    columns = [col[1] for col in cursor.fetchall()]
    
    # Create invoice data dictionary
    invoice_data = dict(zip(columns, row))
    
    # Check if invoice image exists for this invoice number
    cursor.execute('SELECT COUNT(*) FROM invoice_images WHERE invoice_number = ? AND invoice_type = ?', 
                   (invoice_no, 'DGF'))
    has_invoice_image = cursor.fetchone()[0] > 0
    
    conn.close()
    
    return render_template('dhl_ytd_invoice_detail.html', 
                         invoice_data=invoice_data,
                         invoice_no=invoice_no,
                         has_invoice_image=has_invoice_image)

@dhl_ytd_bp.route('/dhl-ytd-statistics')
@require_auth
def ytd_statistics(user_data=None):
    """Show YTD data statistics and summary"""
    processor = DHLYTDProcessor()
    
    import sqlite3
    conn = sqlite3.connect(processor.db_path)
    cursor = conn.cursor()
    
    # Basic statistics
    cursor.execute('SELECT COUNT(*) FROM dhl_ytd_invoices')
    total_invoices = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT upload_batch_id) FROM dhl_ytd_invoices')
    total_batches = cursor.fetchone()[0]
    
    # Currency breakdown
    cursor.execute('''
        SELECT invoice_currency, COUNT(*), 
               SUM(total_charges_with_duty_tax),
               SUM(total_charges_with_duty_tax_usd)
        FROM dhl_ytd_invoices 
        WHERE invoice_currency IS NOT NULL
        GROUP BY invoice_currency
        ORDER BY COUNT(*) DESC
    ''')
    
    currency_stats = []
    for row in cursor.fetchall():
        currency_stats.append({
            'currency': row[0],
            'count': row[1],
            'total_amount': row[2] or 0,
            'total_amount_usd': row[3] or 0
        })
    
    # Transportation mode breakdown
    cursor.execute('''
        SELECT transportation_mode, COUNT(*), 
               AVG(total_charges_with_duty_tax_usd)
        FROM dhl_ytd_invoices 
        WHERE transportation_mode IS NOT NULL
        GROUP BY transportation_mode
        ORDER BY COUNT(*) DESC
    ''')
    
    transport_stats = []
    for row in cursor.fetchall():
        transport_stats.append({
            'mode': row[0],
            'count': row[1],
            'avg_amount_usd': row[2] or 0
        })
    
    # Monthly breakdown
    cursor.execute('''
        SELECT strftime('%Y-%m', invoice_creation_date) as month,
               COUNT(*),
               SUM(total_charges_with_duty_tax_usd)
        FROM dhl_ytd_invoices 
        WHERE invoice_creation_date IS NOT NULL
        GROUP BY month
        ORDER BY month DESC
        LIMIT 12
    ''')
    
    monthly_stats = []
    for row in cursor.fetchall():
        monthly_stats.append({
            'month': row[0],
            'count': row[1],
            'total_usd': row[2] or 0
        })
    
    # Top shippers/consignees
    cursor.execute('''
        SELECT shipper_name, COUNT(*), SUM(total_charges_with_duty_tax_usd)
        FROM dhl_ytd_invoices 
        WHERE shipper_name IS NOT NULL
        GROUP BY shipper_name
        ORDER BY COUNT(*) DESC
        LIMIT 10
    ''')
    
    top_shippers = []
    for row in cursor.fetchall():
        top_shippers.append({
            'name': row[0],
            'count': row[1],
            'total_usd': row[2] or 0
        })
    
    cursor.execute('''
        SELECT consignee_name, COUNT(*), SUM(total_charges_with_duty_tax_usd)
        FROM dhl_ytd_invoices 
        WHERE consignee_name IS NOT NULL
        GROUP BY consignee_name
        ORDER BY COUNT(*) DESC
        LIMIT 10
    ''')
    
    top_consignees = []
    for row in cursor.fetchall():
        top_consignees.append({
            'name': row[0],
            'count': row[1],
            'total_usd': row[2] or 0
        })
    
    conn.close()
    
    return render_template('dhl_ytd_statistics.html',
                         total_invoices=total_invoices,
                         total_batches=total_batches,
                         currency_stats=currency_stats,
                         transport_stats=transport_stats,
                         monthly_stats=monthly_stats,
                         top_shippers=top_shippers,
                         top_consignees=top_consignees)
