"""
Flask routes for DHL Ocean Rate Card management
Provides web interface for uploading, viewing, and searching ocean freight rates
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, send_file
from werkzeug.utils import secure_filename
import os
import sqlite3
from datetime import datetime
import logging
from ocean_rate_card_processor import OceanRateCardProcessor

# Import authentication
try:
    from auth_routes import require_auth, require_auth_api
except ImportError:
    # Fallback if auth is not available
    def require_auth(f):
        return f
    def require_auth_api(f):
        return f

# Create blueprint
ocean_rate_bp = Blueprint('ocean_rate', __name__, url_prefix='/ocean-rates')

# Configure upload folder
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@ocean_rate_bp.route('/')
@require_auth
def dashboard(user_data=None):
    """Ocean rate card dashboard"""
    processor = OceanRateCardProcessor()
    
    # Get upload statistics
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Get total records
    cursor.execute('SELECT COUNT(*) FROM ocean_rate_cards')
    total_rates = cursor.fetchone()[0]
    
    # Get total uploads
    cursor.execute('SELECT COUNT(*) FROM ocean_rate_card_uploads')
    total_uploads = cursor.fetchone()[0]
    
    # Get recent uploads
    cursor.execute('''
        SELECT upload_id, filename, upload_date, processed_records, failed_records, status
        FROM ocean_rate_card_uploads 
        ORDER BY upload_date DESC 
        LIMIT 10
    ''')
    recent_uploads = cursor.fetchall()
    
    # Get origin/destination statistics
    cursor.execute('''
        SELECT origin_country, COUNT(*) as count
        FROM ocean_rate_cards 
        GROUP BY origin_country 
        ORDER BY count DESC 
        LIMIT 10
    ''')
    top_origins = cursor.fetchall()
    
    cursor.execute('''
        SELECT destination_country, COUNT(*) as count
        FROM ocean_rate_cards 
        GROUP BY destination_country 
        ORDER BY count DESC 
        LIMIT 10
    ''')
    top_destinations = cursor.fetchall()
    
    # Get service types
    cursor.execute('''
        SELECT service, COUNT(*) as count
        FROM ocean_rate_cards 
        GROUP BY service 
        ORDER BY count DESC
    ''')
    service_types = cursor.fetchall()
    
    conn.close()
    
    return render_template('ocean_rate_dashboard.html',
                         total_rates=total_rates,
                         total_uploads=total_uploads,
                         recent_uploads=recent_uploads,
                         top_origins=top_origins,
                         top_destinations=top_destinations,
                         service_types=service_types)

@ocean_rate_bp.route('/upload', methods=['GET', 'POST'])
@require_auth
def upload_rate_card(user_data=None):
    """Upload ocean rate card Excel file"""
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file selected', 'error')
            return redirect(request.url)
        
        file = request.files['file']
        if file.filename == '':
            flash('No file selected', 'error')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            try:
                filename = secure_filename(file.filename)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"ocean_rate_card_{timestamp}_{filename}"
                
                if not os.path.exists(UPLOAD_FOLDER):
                    os.makedirs(UPLOAD_FOLDER)
                
                file_path = os.path.join(UPLOAD_FOLDER, filename)
                file.save(file_path)
                
                # Process the file
                processor = OceanRateCardProcessor()
                result = processor.process_excel_file(file_path)
                
                if result['failed_records'] == 0:
                    flash(f'Successfully processed {result["processed_records"]} rate records!', 'success')
                else:
                    flash(f'Processed {result["processed_records"]} records with {result["failed_records"]} failures', 'warning')
                
                return redirect(url_for('ocean_rate.view_upload', upload_id=result['upload_id']))
                
            except Exception as e:
                flash(f'Error processing file: {str(e)}', 'error')
                logging.error(f"Ocean rate card upload error: {str(e)}")
        else:
            flash('Please upload a valid Excel file (.xlsx, .xls)', 'error')
    
    return render_template('ocean_rate_upload.html')

@ocean_rate_bp.route('/uploads')
@require_auth
def list_uploads(user_data=None):
    """List all ocean rate card uploads"""
    processor = OceanRateCardProcessor()
    uploads = processor.get_upload_summary()
    
    return render_template('ocean_rate_uploads.html', uploads=uploads)

@ocean_rate_bp.route('/upload/<upload_id>')
@require_auth
def view_upload(upload_id, user_data=None):
    """View details of a specific upload"""
    processor = OceanRateCardProcessor()
    upload_info = processor.get_upload_summary(upload_id)
    
    if not upload_info:
        flash('Upload not found', 'error')
        return redirect(url_for('ocean_rate.dashboard'))
    
    upload = upload_info[0]
    
    # Get sample rate records from this upload
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT 
            r.lane_id, r.lane_description, r.service,
            r.origin_country, r.lane_origin, r.destination_country, r.lane_destination,
            r.dtd_transit_days, f.total_20ft, f.total_40ft, f.total_40hc
        FROM ocean_rate_cards r
        LEFT JOIN ocean_fcl_charges f ON r.id = f.rate_card_id
        WHERE r.upload_id = ?
        ORDER BY r.lane_id
        LIMIT 50
    ''', (upload_id,))
    
    sample_rates = cursor.fetchall()
    conn.close()
    
    return render_template('ocean_rate_upload_detail.html', 
                         upload=upload, 
                         sample_rates=sample_rates)

@ocean_rate_bp.route('/search')
@require_auth
def search_rates(user_data=None):
    """Search ocean freight rates"""
    origin = request.args.get('origin', '')
    destination = request.args.get('destination', '')
    service = request.args.get('service', '')
    container_type = request.args.get('container_type', '')
    
    processor = OceanRateCardProcessor()
    rates = []
    
    # Get dropdown options from database
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Get available origins
    cursor.execute('''
        SELECT DISTINCT origin_country, lane_origin 
        FROM ocean_rate_cards 
        WHERE origin_country IS NOT NULL 
        ORDER BY origin_country, lane_origin
    ''')
    origins = cursor.fetchall()
    
    # Get available destinations
    cursor.execute('''
        SELECT DISTINCT destination_country, lane_destination 
        FROM ocean_rate_cards 
        WHERE destination_country IS NOT NULL 
        ORDER BY destination_country, lane_destination
    ''')
    destinations = cursor.fetchall()
    
    # Get available services
    cursor.execute('''
        SELECT DISTINCT service 
        FROM ocean_rate_cards 
        WHERE service IS NOT NULL 
        ORDER BY service
    ''')
    services = cursor.fetchall()
    
    conn.close()
    
    if origin or destination or service:
        rates = processor.search_rates(origin=origin, 
                                     destination=destination, 
                                     service=service,
                                     container_type=container_type)
    
    return render_template('ocean_rate_search.html', 
                         rates=rates,
                         origins=origins,
                         destinations=destinations,
                         services=services,
                         search_params={
                             'origin': origin,
                             'destination': destination,
                             'service': service,
                             'container_type': container_type
                         })

@ocean_rate_bp.route('/rate/<int:rate_id>')
@require_auth
def view_rate_detail(rate_id, user_data=None):
    """View detailed rate information"""
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Get main rate info
    cursor.execute('''
        SELECT * FROM ocean_rate_cards WHERE id = ?
    ''', (rate_id,))
    
    rate = cursor.fetchone()
    if not rate:
        flash('Rate not found', 'error')
        return redirect(url_for('ocean_rate.dashboard'))
    
    # Get column names
    cursor.execute('PRAGMA table_info(ocean_rate_cards)')
    main_columns = [col[1] for col in cursor.fetchall()]
    
    # Get FCL charges
    cursor.execute('SELECT * FROM ocean_fcl_charges WHERE rate_card_id = ?', (rate_id,))
    fcl_charges = cursor.fetchone()
    
    cursor.execute('PRAGMA table_info(ocean_fcl_charges)')
    fcl_columns = [col[1] for col in cursor.fetchall()]
    
    # Get LCL rates
    cursor.execute('SELECT * FROM ocean_lcl_rates WHERE rate_card_id = ?', (rate_id,))
    lcl_rates = cursor.fetchone()
    
    cursor.execute('PRAGMA table_info(ocean_lcl_rates)')
    lcl_columns = [col[1] for col in cursor.fetchall()]
    
    conn.close()
    
    # Convert to dictionaries
    rate_dict = dict(zip(main_columns, rate)) if rate else {}
    fcl_dict = dict(zip(fcl_columns, fcl_charges)) if fcl_charges else {}
    lcl_dict = dict(zip(lcl_columns, lcl_rates)) if lcl_rates else {}
    
    return render_template('ocean_rate_detail.html',
                         rate=rate_dict,
                         fcl_charges=fcl_dict,
                         lcl_rates=lcl_dict)

@ocean_rate_bp.route('/api/search')
@require_auth_api
def api_search(user_data=None):
    """API endpoint for rate search"""
    origin = request.args.get('origin', '')
    destination = request.args.get('destination', '')
    service = request.args.get('service', '')
    
    processor = OceanRateCardProcessor()
    rates = processor.search_rates(origin=origin, 
                                 destination=destination, 
                                 service=service)
    
    return jsonify({'rates': rates})

@ocean_rate_bp.route('/api/stats')
@require_auth_api
def api_stats(user_data=None):
    """API endpoint for dashboard statistics"""
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Get various statistics
    stats = {}
    
    cursor.execute('SELECT COUNT(*) FROM ocean_rate_cards')
    stats['total_rates'] = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM ocean_rate_card_uploads')
    stats['total_uploads'] = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT origin_country) FROM ocean_rate_cards')
    stats['origin_countries'] = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(DISTINCT destination_country) FROM ocean_rate_cards')
    stats['destination_countries'] = cursor.fetchone()[0]
    
    cursor.execute('''
        SELECT AVG(f.total_40ft) 
        FROM ocean_fcl_charges f 
        WHERE f.total_40ft IS NOT NULL AND f.total_40ft > 0
    ''')
    avg_40ft = cursor.fetchone()[0]
    stats['avg_40ft_rate'] = round(avg_40ft, 2) if avg_40ft else 0
    
    conn.close()
    
    return jsonify(stats)

@ocean_rate_bp.route('/export')
@require_auth
def export_rates(user_data=None):
    """Export ocean rates to Excel"""
    # This would generate an Excel export of the rates
    # For now, return a simple message
    flash('Export functionality coming soon!', 'info')
    return redirect(url_for('ocean_rate.dashboard'))
