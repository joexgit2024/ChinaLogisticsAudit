#!/usr/bin/env python3
"""
DGF Quote Management Routes
Flask routes for managing AIR, FCL, and LCL quotes separately
"""

from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash, send_file
import os
import json
from datetime import datetime
import pandas as pd
from werkzeug.utils import secure_filename
import sqlite3
from dgf_quote_tables import DGFQuoteTables
from dgf_quote_processor import DGFQuoteProcessor

dgf_quotes_bp = Blueprint('dgf_quotes', __name__, url_prefix='/dgf-quotes')

# Configuration
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@dgf_quotes_bp.route('/')
def dashboard():
    """DGF quotes dashboard with separate sections for AIR, FCL, and LCL."""
    try:
        processor = DGFQuoteProcessor()
        summary = processor.get_quotes_summary()
        
        # Get recent uploads
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Recent AIR quotes
        cursor.execute('''
            SELECT quote_id, origin_airport_code, destination_airport_code, 
                   rate_per_kg, currency, validity_start, validity_end, uploaded_at
            FROM dgf_air_quotes 
            ORDER BY uploaded_at DESC LIMIT 5
        ''')
        recent_air = cursor.fetchall()
        
        # Recent FCL quotes
        cursor.execute('''
            SELECT quote_id, origin_port_code, destination_port_code, 
                   container_type, rate_per_container, currency, validity_start, validity_end, uploaded_at
            FROM dgf_fcl_quotes 
            ORDER BY uploaded_at DESC LIMIT 5
        ''')
        recent_fcl = cursor.fetchall()
        
        # Recent LCL quotes
        cursor.execute('''
            SELECT quote_id, origin_port_code, destination_port_code, 
                   rate_per_cbm, currency, validity_start, validity_end, uploaded_at
            FROM dgf_lcl_quotes 
            ORDER BY uploaded_at DESC LIMIT 5
        ''')
        recent_lcl = cursor.fetchall()
        
        conn.close()
        
        return render_template('dgf_quotes/dashboard.html',
                             summary=summary,
                             recent_air=recent_air,
                             recent_fcl=recent_fcl,
                             recent_lcl=recent_lcl)
    
    except Exception as e:
        flash(f'Error loading dashboard: {str(e)}', 'error')
        return render_template('dgf_quotes/dashboard.html',
                             summary={}, recent_air=[], recent_fcl=[], recent_lcl=[])

@dgf_quotes_bp.route('/upload', methods=['GET', 'POST'])
def upload():
    """Upload quotes file containing AIR, FCL, and LCL sheets."""
    if request.method == 'POST':
        try:
            if 'quote_file' not in request.files:
                flash('No file selected', 'error')
                return redirect(request.url)
            
            file = request.files['quote_file']
            if file.filename == '':
                flash('No file selected', 'error')
                return redirect(request.url)
            
            if file and allowed_file(file.filename):
                # Save uploaded file
                filename = secure_filename(file.filename)
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"{timestamp}_{filename}"
                file_path = os.path.join(UPLOAD_FOLDER, filename)
                
                # Ensure upload directory exists
                os.makedirs(UPLOAD_FOLDER, exist_ok=True)
                file.save(file_path)
                
                # Process the file
                processor = DGFQuoteProcessor()
                uploaded_by = request.form.get('uploaded_by', 'web_user')
                results = processor.process_quote_file(file_path, uploaded_by)
                
                # Generate success/error messages
                total_success = sum(r['success'] for r in results.values())
                total_errors = sum(r['errors'] for r in results.values())
                
                if total_success > 0:
                    flash(f'Successfully processed {total_success} quotes', 'success')
                
                if total_errors > 0:
                    flash(f'{total_errors} errors occurred during processing', 'warning')
                
                # Show detailed results
                for quote_type, result in results.items():
                    if result['success'] > 0:
                        flash(f"{quote_type.upper()}: {result['success']} quotes processed", 'info')
                    for message in result['messages']:
                        flash(f"{quote_type.upper()}: {message}", 'info')
                
                return redirect(url_for('dgf_quotes.dashboard'))
            else:
                flash('Invalid file type. Please upload an Excel file (.xlsx or .xls)', 'error')
        
        except Exception as e:
            flash(f'Error processing file: {str(e)}', 'error')
    
    return render_template('dgf_quotes/upload.html')

@dgf_quotes_bp.route('/air')
def air_quotes():
    """View and manage AIR quotes."""
    try:
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Get filter parameters
        status = request.args.get('status', 'ACTIVE')
        origin = request.args.get('origin', '')
        destination = request.args.get('destination', '')
        
        # Build query
        where_conditions = ['1=1']
        params = []
        
        if status:
            where_conditions.append('status = ?')
            params.append(status)
        
        if origin:
            where_conditions.append('(origin_country LIKE ? OR origin_city LIKE ? OR origin_airport_code LIKE ?)')
            params.extend([f'%{origin}%', f'%{origin}%', f'%{origin}%'])
        
        if destination:
            where_conditions.append('(destination_country LIKE ? OR destination_city LIKE ? OR destination_airport_code LIKE ?)')
            params.extend([f'%{destination}%', f'%{destination}%', f'%{destination}%'])
        
        query = f'''
            SELECT quote_id, origin_country, origin_city, origin_airport_code,
                   destination_country, destination_city, destination_airport_code,
                   rate_per_kg, currency, validity_start, validity_end, status, uploaded_at
            FROM dgf_air_quotes
            WHERE {' AND '.join(where_conditions)}
            ORDER BY uploaded_at DESC
            LIMIT 100
        '''
        
        cursor.execute(query, params)
        quotes = cursor.fetchall()
        
        # Get summary stats
        cursor.execute('SELECT COUNT(*) FROM dgf_air_quotes WHERE status = "ACTIVE"')
        active_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM dgf_air_quotes')
        total_count = cursor.fetchone()[0]
        
        conn.close()
        
        return render_template('dgf_quotes/air_quotes.html',
                             quotes=quotes,
                             active_count=active_count,
                             total_count=total_count,
                             filters={'status': status, 'origin': origin, 'destination': destination})
    
    except Exception as e:
        flash(f'Error loading air quotes: {str(e)}', 'error')
        return render_template('dgf_quotes/air_quotes.html',
                             quotes=[], active_count=0, total_count=0, filters={})

@dgf_quotes_bp.route('/fcl')
def fcl_quotes():
    """View and manage FCL quotes."""
    try:
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Get filter parameters
        status = request.args.get('status', 'ACTIVE')
        origin = request.args.get('origin', '')
        destination = request.args.get('destination', '')
        container_type = request.args.get('container_type', '')
        
        # Build query
        where_conditions = ['1=1']
        params = []
        
        if status:
            where_conditions.append('status = ?')
            params.append(status)
        
        if origin:
            where_conditions.append('(origin_country LIKE ? OR origin_port LIKE ? OR origin_port_code LIKE ?)')
            params.extend([f'%{origin}%', f'%{origin}%', f'%{origin}%'])
        
        if destination:
            where_conditions.append('(destination_country LIKE ? OR destination_port LIKE ? OR destination_port_code LIKE ?)')
            params.extend([f'%{destination}%', f'%{destination}%', f'%{destination}%'])
        
        if container_type:
            where_conditions.append('container_type LIKE ?')
            params.append(f'%{container_type}%')
        
        query = f'''
            SELECT quote_id, origin_country, origin_port, origin_port_code,
                   destination_country, destination_port, destination_port_code,
                   container_type, rate_per_container, currency, validity_start, validity_end, status, uploaded_at
            FROM dgf_fcl_quotes
            WHERE {' AND '.join(where_conditions)}
            ORDER BY uploaded_at DESC
            LIMIT 100
        '''
        
        cursor.execute(query, params)
        quotes = cursor.fetchall()
        
        # Get summary stats
        cursor.execute('SELECT COUNT(*) FROM dgf_fcl_quotes WHERE status = "ACTIVE"')
        active_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM dgf_fcl_quotes')
        total_count = cursor.fetchone()[0]
        
        # Get container types for filter
        cursor.execute('SELECT DISTINCT container_type FROM dgf_fcl_quotes WHERE container_type IS NOT NULL ORDER BY container_type')
        container_types = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        
        return render_template('dgf_quotes/fcl_quotes.html',
                             quotes=quotes,
                             active_count=active_count,
                             total_count=total_count,
                             container_types=container_types,
                             filters={'status': status, 'origin': origin, 'destination': destination, 'container_type': container_type})
    
    except Exception as e:
        flash(f'Error loading FCL quotes: {str(e)}', 'error')
        return render_template('dgf_quotes/fcl_quotes.html',
                             quotes=[], active_count=0, total_count=0, container_types=[], filters={})

@dgf_quotes_bp.route('/lcl')
def lcl_quotes():
    """View and manage LCL quotes."""
    try:
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Get filter parameters
        status = request.args.get('status', 'ACTIVE')
        origin = request.args.get('origin', '')
        destination = request.args.get('destination', '')
        
        # Build query
        where_conditions = ['1=1']
        params = []
        
        if status:
            where_conditions.append('status = ?')
            params.append(status)
        
        if origin:
            where_conditions.append('(origin_country LIKE ? OR origin_port LIKE ? OR origin_port_code LIKE ?)')
            params.extend([f'%{origin}%', f'%{origin}%', f'%{origin}%'])
        
        if destination:
            where_conditions.append('(destination_country LIKE ? OR destination_port LIKE ? OR destination_port_code LIKE ?)')
            params.extend([f'%{destination}%', f'%{destination}%', f'%{destination}%'])
        
        query = f'''
            SELECT quote_id, origin_country, origin_port, origin_port_code,
                   destination_country, destination_port, destination_port_code,
                   rate_per_cbm, rate_per_ton, currency, validity_start, validity_end, status, uploaded_at
            FROM dgf_lcl_quotes
            WHERE {' AND '.join(where_conditions)}
            ORDER BY uploaded_at DESC
            LIMIT 100
        '''
        
        cursor.execute(query, params)
        quotes = cursor.fetchall()
        
        # Get summary stats
        cursor.execute('SELECT COUNT(*) FROM dgf_lcl_quotes WHERE status = "ACTIVE"')
        active_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM dgf_lcl_quotes')
        total_count = cursor.fetchone()[0]
        
        conn.close()
        
        return render_template('dgf_quotes/lcl_quotes.html',
                             quotes=quotes,
                             active_count=active_count,
                             total_count=total_count,
                             filters={'status': status, 'origin': origin, 'destination': destination})
    
    except Exception as e:
        flash(f'Error loading LCL quotes: {str(e)}', 'error')
        return render_template('dgf_quotes/lcl_quotes.html',
                             quotes=[], active_count=0, total_count=0, filters={})

@dgf_quotes_bp.route('/api/quote/<quote_type>/<quote_id>')
def get_quote_details(quote_type, quote_id):
    """Get detailed quote information via API."""
    try:
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        if quote_type == 'air':
            cursor.execute('SELECT * FROM dgf_air_quotes WHERE quote_id = ?', (quote_id,))
        elif quote_type == 'fcl':
            cursor.execute('SELECT * FROM dgf_fcl_quotes WHERE quote_id = ?', (quote_id,))
        elif quote_type == 'lcl':
            cursor.execute('SELECT * FROM dgf_lcl_quotes WHERE quote_id = ?', (quote_id,))
        else:
            return jsonify({'error': 'Invalid quote type'}), 400
        
        quote = cursor.fetchone()
        conn.close()
        
        if quote:
            # Convert to dictionary (you'd need to map column names properly)
            return jsonify({'quote': quote})
        else:
            return jsonify({'error': 'Quote not found'}), 404
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@dgf_quotes_bp.route('/api/stats')
def get_stats():
    """Get quote statistics via API."""
    try:
        processor = DGFQuoteProcessor()
        summary = processor.get_quotes_summary()
        return jsonify(summary)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@dgf_quotes_bp.route('/export/<quote_type>')
def export_quotes(quote_type):
    """Export quotes to Excel."""
    try:
        conn = sqlite3.connect('dhl_audit.db')
        
        if quote_type == 'air':
            df = pd.read_sql_query('SELECT * FROM dgf_air_quotes ORDER BY uploaded_at DESC', conn)
        elif quote_type == 'fcl':
            df = pd.read_sql_query('SELECT * FROM dgf_fcl_quotes ORDER BY uploaded_at DESC', conn)
        elif quote_type == 'lcl':
            df = pd.read_sql_query('SELECT * FROM dgf_lcl_quotes ORDER BY uploaded_at DESC', conn)
        else:
            flash('Invalid quote type', 'error')
            return redirect(url_for('dgf_quotes.dashboard'))
        
        conn.close()
        
        # Export to Excel
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'dgf_{quote_type}_quotes_{timestamp}.xlsx'
        filepath = os.path.join(UPLOAD_FOLDER, filename)
        
        df.to_excel(filepath, index=False)
        
        return send_file(filepath, as_attachment=True, download_name=filename)
    
    except Exception as e:
        flash(f'Error exporting quotes: {str(e)}', 'error')
        return redirect(url_for('dgf_quotes.dashboard'))

# Initialize tables when the blueprint is imported
try:
    DGFQuoteTables()
except Exception as e:
    print(f"Warning: Could not initialize DGF quote tables: {str(e)}")
