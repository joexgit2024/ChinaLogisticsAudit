"""
Air Freight Rate Card Management Routes

Overall Purpose:
----------------
This file defines the Flask blueprint and all route handlers for managing air
freight rate cards in the DHL Invoice Audit Application. It provides endpoints for:
        - Viewing the air rate card dashboard and statistics
        - Listing, uploading, and viewing individual rate cards
        - API endpoint for rate lookups
        - Utility functions for dropdown/filter data

Where This File is Used:
------------------------
- The blueprint `rate_card_bp` is registered in the main Flask application
    (typically in `app.py` or the main app factory).
- Its routes are accessible under `/air-rates`, `/rate-cards`, and
    `/api/rate-lookup` URLs.
- Used by the web UI for air freight rate card management and by API consumers
    for rate lookup.
"""

from flask import (
    Blueprint, request, render_template, redirect, url_for, flash, jsonify,
    current_app
)
import os
from app.models.rate_card import (
    create_rate_card_table, import_rate_card, get_rate_cards, get_rate_card,
    get_rate_entries, get_applicable_rate
)
from werkzeug.utils import secure_filename

rate_card_bp = Blueprint('rate_card', __name__)

# Ensure tables exist
create_rate_card_table()


@rate_card_bp.route('/air-rates')
def air_rates():
    """Air rate card dashboard with statistics."""
    from app.database import get_db_connection
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get total rate entries
    cursor.execute('SELECT COUNT(*) FROM air_rate_entries')
    total_rates = cursor.fetchone()[0]

    # Get total rate cards
    cursor.execute('SELECT COUNT(*) FROM air_rate_cards')
    total_rate_cards = cursor.fetchone()[0]

    # Get recent rate cards
    cursor.execute(
        'SELECT id, card_name, validity_start, validity_end, uploaded_at '
        'FROM air_rate_cards '
        'ORDER BY uploaded_at DESC '
        'LIMIT 10'
    )
    recent_cards = cursor.fetchall()
    
    # Get origin/destination statistics
    cursor.execute(
        'SELECT origin_country, COUNT(*) as count '
        'FROM air_rate_entries '
        "WHERE origin_country IS NOT NULL AND origin_country != '' "
        'GROUP BY origin_country '
        'ORDER BY count DESC '
        'LIMIT 10'
    )
    top_origins = cursor.fetchall()

    cursor.execute(
        'SELECT destination_country, COUNT(*) as count '
        'FROM air_rate_entries '
        "WHERE destination_country IS NOT NULL AND destination_country != '' "
        'GROUP BY destination_country '
        'ORDER BY count DESC '
        'LIMIT 10'
    )
    top_destinations = cursor.fetchall()
    
    # Get service types
    cursor.execute(
        'SELECT service, COUNT(*) as count '
        'FROM air_rate_entries '
        "WHERE service IS NOT NULL AND service != '' "
        'GROUP BY service '
        'ORDER BY count DESC'
    )
    service_types = cursor.fetchall()

    # Get lane statistics
    cursor.execute('SELECT COUNT(DISTINCT lane_id) FROM air_rate_entries')
    total_lanes = cursor.fetchone()[0]

    conn.close()

    return render_template(
        'air_rate_dashboard.html',
        total_rates=total_rates,
        total_rate_cards=total_rate_cards,
        total_lanes=total_lanes,
        recent_cards=recent_cards,
        top_origins=top_origins,
        top_destinations=top_destinations,
        service_types=service_types
    )

@rate_card_bp.route('/rate-cards')
def list_rate_cards():
    """List all uploaded rate cards."""
    rate_cards = get_rate_cards()
    return render_template('rate_cards.html', rate_cards=rate_cards)

@rate_card_bp.route('/rate-cards/upload', methods=['GET', 'POST'])
def upload_rate_card():
    """Handle rate card upload and processing."""
    if request.method == 'POST':
        if 'rate_card_file' not in request.files:
            flash('No file selected', 'error')
            return redirect(request.url)
            
        file = request.files['rate_card_file']
        card_name = request.form.get('card_name', '')
        
        if file.filename == '':
            flash('No file selected', 'error')
            return redirect(request.url)
            
        if not file.filename.endswith(('.xls', '.xlsx')):
            flash('Only Excel files (.xls, .xlsx) are supported', 'error')
            return redirect(request.url)
        
        # Save the file
        filename = secure_filename(file.filename)
        file_path = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        # Process the rate card
        result = import_rate_card(file_path, card_name=card_name if card_name else None)
        
        if result['success']:
            flash(result['message'], 'success')
            return redirect(url_for('rate_card.view_rate_card', rate_card_id=result['rate_card_id']))
        else:
            flash(result['message'], 'error')
            return redirect(request.url)
    
    return render_template('upload_rate_card.html')

def get_filter_dropdown_data():
    """Get distinct values for filter dropdowns."""
    from app.database import get_db_connection
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get origin countries
    cursor.execute('''
        SELECT DISTINCT origin_country 
        FROM air_rate_entries 
        WHERE origin_country IS NOT NULL 
        ORDER BY origin_country
    ''')
    origin_countries = [row[0] for row in cursor.fetchall()]
    
    # Get destination countries
    cursor.execute('''
        SELECT DISTINCT destination_country 
        FROM air_rate_entries 
        WHERE destination_country IS NOT NULL 
        ORDER BY destination_country
    ''')
    destination_countries = [row[0] for row in cursor.fetchall()]
    
    # Get service types
    cursor.execute('''
        SELECT DISTINCT service 
        FROM air_rate_entries 
        WHERE service IS NOT NULL 
        ORDER BY service
    ''')
    services = [row[0] for row in cursor.fetchall()]
    
    conn.close()
    
    return {
        'origin_countries': origin_countries,
        'destination_countries': destination_countries,
        'services': services
    }

@rate_card_bp.route('/rate-cards/<int:rate_card_id>')
def view_rate_card(rate_card_id):
    """View a specific rate card and its entries."""
    rate_card = get_rate_card(rate_card_id)
    
    if not rate_card:
        flash('Rate card not found', 'error')
        return redirect(url_for('rate_card.list_rate_cards'))
    
    # Get filter parameters
    origin_country = request.args.get('origin_country', '')
    destination_country = request.args.get('destination_country', '')
    service_filter = request.args.get('service_filter', '')
    
    filters = {}
    if origin_country:
        filters['origin_country'] = origin_country
    if destination_country:
        filters['destination_country'] = destination_country
    if service_filter:
        filters['service'] = service_filter
    
    entries = get_rate_entries(rate_card_id, filters)
    
    # Get dropdown data
    dropdown_data = get_filter_dropdown_data()
    
    return render_template('view_rate_card.html', 
                         rate_card=rate_card, 
                         entries=entries,
                         origin_country=origin_country,
                         destination_country=destination_country,
                         service_filter=service_filter,
                         dropdown_data=dropdown_data)

@rate_card_bp.route('/api/rate-lookup')
def rate_lookup_api():
    """API endpoint for looking up applicable rates."""
    origin = request.args.get('origin', '')
    destination = request.args.get('destination', '')
    weight = request.args.get('weight', 0)
    service_type = request.args.get('service_type', '')
    rate_card_id = request.args.get('rate_card_id', None)
    
    try:
        weight_kg = float(weight)
    except (ValueError, TypeError):
        return jsonify({
            'error': 'Invalid weight parameter'
        }), 400
        
    if rate_card_id:
        try:
            rate_card_id = int(rate_card_id)
        except (ValueError, TypeError):
            rate_card_id = None
    
    if not origin or not destination:
        return jsonify({
            'error': 'Origin and destination are required'
        }), 400
        
    rate = get_applicable_rate(origin, destination, weight_kg, service_type, rate_card_id)
    
    if not rate:
        return jsonify({
            'found': False,
            'message': 'No applicable rate found for the specified criteria'
        })
    
    # Format the response with safe numeric values
    response = {
        'found': True,
        'lane_id': rate.get('lane_id', 'N/A'),
        'origin': rate.get('origin_country', 'N/A'),
        'destination': rate.get('destination_country', 'N/A'),
        'service': rate.get('service', 'N/A'),
        'weight_kg': float(weight_kg or 0),
        'rate_tier': get_weight_tier_name(weight_kg),
        'base_rate': float(rate.get('applicable_rate', 0) or 0),
        'base_cost': float(rate.get('base_cost', 0) or 0),
        'fuel_surcharge': float(rate.get('fuel_surcharge', 0) or 0),
        'fuel_cost': float(rate.get('fuel_cost', 0) or 0),
        'origin_fees': float(rate.get('origin_fees', 0) or 0),
        'origin_fees_cost': float(rate.get('origin_fees_cost', 0) or 0),
        'destination_fees': float(rate.get('destination_fees', 0) or 0),
        'destination_fees_cost': float(rate.get('destination_fees_cost', 0) or 0),
        'pss_charge': float(rate.get('pss_charge', 0) or 0),
        'pss_cost': float(rate.get('pss_cost', 0) or 0),
        'min_charge': float(rate.get('min_charge', 0) or 0),
        'min_charge_applied': bool(rate.get('min_charge_applied', False)),
        'total_cost': float(rate.get('total_cost', 0) or 0),
        'currency': 'USD'
    }
    
    return jsonify(response)

def get_weight_tier_name(weight_kg):
    """Get the weight tier name based on weight."""
    try:
        weight_kg = float(weight_kg or 0)
    except (ValueError, TypeError):
        weight_kg = 0
        
    if weight_kg < 1000:
        return "< 1000 kg"
    elif weight_kg < 2000:
        return "1000-1999 kg"
    elif weight_kg <= 3000:
        return "2000-3000 kg"
    else:
        return "> 3000 kg"
