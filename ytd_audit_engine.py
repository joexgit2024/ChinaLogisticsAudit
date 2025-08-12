#!/usr/bin/env python3
"""
DHL YTD Rate Card Audit System
New audit functionality using YTD invoice data against air and ocean rate cards
Invoice ID is the key matching field as requested by user
"""

from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
import sqlite3
import logging
from datetime import datetime
from decimal import Decimal
import json

# Create blueprint for YTD audit system
ytd_audit_bp = Blueprint('ytd_audit', __name__, url_prefix='/ytd_audit')

class YTDAuditEngine:
    """Core engine for auditing YTD invoices against rate cards"""
    
    def __init__(self):
        self.db_path = 'dhl_audit.db'
        
    def get_ytd_invoice(self, invoice_no: str) -> dict:
        """Get YTD invoice data by invoice number"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get all YTD invoice data
        cursor.execute('''
            SELECT * FROM dhl_ytd_invoices WHERE invoice_no = ?
        ''', (invoice_no,))
        
        row = cursor.fetchone()
        if not row:
            conn.close()
            return None
            
        # Get column names
        cursor.execute("PRAGMA table_info(dhl_ytd_invoices)")
        columns = [col[1] for col in cursor.fetchall()]
        
        conn.close()
        
        # Convert to dictionary
        invoice_data = dict(zip(columns, row))
        return invoice_data
    
    def find_matching_air_rates(self, invoice_data: dict) -> list:
        """Find matching air freight rate cards based on origin/destination"""
        if invoice_data.get('transportation_mode') != 'Air':
            return []
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Extract housebill origin and destination for exact port code matching
        housebill_origin = invoice_data.get('housebill_origin', '')
        housebill_destination = invoice_data.get('housebill_destination', '')
        
        # Fallback to traditional fields if housebill fields not available
        origin_country_code = invoice_data.get('origin_port_country_code', '')
        destination_country_code = invoice_data.get('destination_port_country_code', '')
        origin_port = invoice_data.get('origin', '')
        destination_port = invoice_data.get('destination', '')
        
        # Search for matching air rates - prioritize exact housebill port code matches
        if housebill_origin and housebill_destination:
            # First try exact match with housebill port codes
            cursor.execute('''
                SELECT ar.*, arc.card_name, arc.validity_start, arc.validity_end
                FROM air_rate_entries ar
                JOIN air_rate_cards arc ON ar.rate_card_id = arc.id
                WHERE ar.origin_port_code = ? AND ar.destination_port_code = ?
                ORDER BY ar.lane_id
            ''', (housebill_origin, housebill_destination))
        else:
            # Fallback to fuzzy matching if housebill codes not available
            cursor.execute('''
                SELECT ar.*, arc.card_name, arc.validity_start, arc.validity_end
                FROM air_rate_entries ar
                JOIN air_rate_cards arc ON ar.rate_card_id = arc.id
                WHERE (ar.origin_country LIKE ? OR ar.lane_origin LIKE ? OR ar.origin_port_code LIKE ?)
                AND (ar.destination_country LIKE ? OR ar.lane_destination LIKE ? OR ar.destination_port_code LIKE ?)
                ORDER BY ar.lane_id
            ''', (f'%{origin_country_code}%', f'%{origin_port}%', f'%{origin_country_code}%',
                  f'%{destination_country_code}%', f'%{destination_port}%', f'%{destination_country_code}%'))
        
        matches = []
        for row in cursor.fetchall():
            # Convert row to dictionary
            cursor.execute("PRAGMA table_info(air_rate_entries)")
            air_columns = [col[1] for col in cursor.fetchall()]
            air_columns.extend(['card_name', 'validity_start', 'validity_end'])
            
            rate_data = dict(zip(air_columns, row))
            matches.append(rate_data)
        
        conn.close()
        return matches
    
    def find_matching_ocean_rates(self, invoice_data: dict) -> list:
        """Find matching ocean freight rate cards based on origin/destination"""
        if invoice_data.get('transportation_mode') != 'Sea':
            return []
            
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Extract origin and destination from YTD data (correct column names)
        origin_country_code = invoice_data.get('origin_port_country_code', '')
        destination_country_code = invoice_data.get('destination_port_country_code', '')
        origin_port = invoice_data.get('origin', '')
        destination_port = invoice_data.get('destination', '')
        
        # Search for matching ocean rates with FCL/LCL data
        cursor.execute('''
            SELECT or.*, fcl.total_20ft, fcl.total_40ft, fcl.total_40hc,
                   lcl.lcl_total_min_usd, lcl.lcl_total_usd_per_cbm
            FROM ocean_rate_cards or
            LEFT JOIN ocean_fcl_charges fcl ON or.id = fcl.rate_card_id
            LEFT JOIN ocean_lcl_rates lcl ON or.id = lcl.rate_card_id
            WHERE (or.origin_country LIKE ? OR or.lane_origin LIKE ? OR or.port_of_loading LIKE ?)
            AND (or.destination_country LIKE ? OR or.lane_destination LIKE ? OR or.port_of_discharge LIKE ?)
            ORDER BY or.lane_id
        ''', (f'%{origin_country_code}%', f'%{origin_port}%', f'%{origin_port}%',
              f'%{destination_country_code}%', f'%{destination_port}%', f'%{destination_port}%'))
        
        matches = []
        for row in cursor.fetchall():
            # Get column names for ocean rates
            cursor.execute("PRAGMA table_info(ocean_rate_cards)")
            ocean_columns = [col[1] for col in cursor.fetchall()]
            ocean_columns.extend(['fcl_20ft_total', 'fcl_40ft_total', 'fcl_40hc_total',
                                'lcl_min_total', 'lcl_cbm_rate'])
            
            rate_data = dict(zip(ocean_columns, row))
            matches.append(rate_data)
        
        conn.close()
        return matches
    
    def calculate_air_audit_variance(self, invoice_data: dict, rate_data: dict) -> dict:
        """Calculate variance between YTD actual charges and air rate card"""
        audit_result = {
            'invoice_no': invoice_data.get('invoice_no'),
            'rate_card': rate_data.get('card_name'),
            'lane_id': rate_data.get('lane_id'),
            'variances': [],
            'total_variance': 0.0,
            'audit_status': 'PASS'
        }
        
        # Get actual freight charges to detect duty/tax-only invoices
        actual_freight = float(invoice_data.get('freight_charges_usd', 0) or 0)
        actual_duties_taxes = float(invoice_data.get('duties_and_taxes_usd', 0) or 0)
        actual_customs_total = (float(invoice_data.get('origin_customs_charges_usd', 0) or 0) + 
                               float(invoice_data.get('destination_customs_charges_usd', 0) or 0))
        
        # **DUTY/TAX-ONLY INVOICE DETECTION**
        # If invoice has no freight charges but has duty/tax/customs charges, treat as pass-through
        if actual_freight == 0 and (actual_duties_taxes > 0 or actual_customs_total > 0):
            # This is a duty/tax-only invoice - freight already charged in separate invoice
            audit_result['audit_status'] = 'PASS'
            audit_result['is_duty_tax_only'] = True
            audit_result['total_variance'] = 0.0
            audit_result['status_reason'] = 'Duty/Tax/Customs charges only - Pass-through invoice (freight charged separately)'
            
            # Create pass-through variances for display
            audit_result['variances'] = [
                {
                    'charge_type': 'Freight Charges',
                    'actual': 0.0,
                    'expected': 0.0,
                    'variance': 0.0,
                    'variance_pct': 0.0
                },
                {
                    'charge_type': 'Duty Tax Charges',
                    'actual': actual_duties_taxes,
                    'expected': actual_duties_taxes,  # Pass-through
                    'variance': 0.0,
                    'variance_pct': 0.0
                },
                {
                    'charge_type': 'Customs Charges',
                    'actual': actual_customs_total,
                    'expected': actual_customs_total,  # Pass-through
                    'variance': 0.0,
                    'variance_pct': 0.0
                }
            ]
            
            return audit_result
        
        # Get weight for rate calculation (use correct YTD column name)
        weight_kg = invoice_data.get('shipment_weight_kg', 0) or invoice_data.get('total_shipment_chargeable_weight_kg', 0) or 0
        
        # Calculate expected charges based on weight tiers
        if weight_kg < 1000:
            expected_base_rate = rate_data.get('base_rate_lt1000kg', 0) or rate_data.get('ata_cost_lt1000kg', 0)
        elif weight_kg < 2000:
            expected_base_rate = rate_data.get('base_rate_1000to2000kg', 0) or rate_data.get('ata_cost_1000_1999kg', 0)
        elif weight_kg <= 3000:
            expected_base_rate = rate_data.get('base_rate_2000to3000kg', 0) or rate_data.get('ata_cost_2000_3000kg', 0)
        else:
            expected_base_rate = rate_data.get('base_rate_gt3000kg', 0) or rate_data.get('ata_cost_gt3000kg', 0)
        
        # Calculate expected freight cost
        expected_freight = float(expected_base_rate or 0) * float(weight_kg)
        
        # Add minimum charge check
        min_charge = float(rate_data.get('min_charge', 0) or rate_data.get('ata_min_charge', 0) or 0)
        if expected_freight < min_charge:
            expected_freight = min_charge
        
        # Add fuel surcharge
        fuel_rate = float(rate_data.get('fuel_surcharge', 0) or 0)
        expected_fuel = fuel_rate * float(weight_kg) if fuel_rate > 0 else 0
        
        # Add origin and destination fees
        expected_origin = float(rate_data.get('origin_fees', 0) or rate_data.get('origin_min_charge', 0) or 0)
        expected_destination = float(rate_data.get('destination_fees', 0) or rate_data.get('destination_min_charge', 0) or 0)
        expected_security = float(rate_data.get('security_surcharge', 0) or 0)
        expected_pickup = float(rate_data.get('pickup_charges', 0) or 0)
        expected_delivery = float(rate_data.get('delivery_charges', 0) or 0)
        expected_customs_origin = float(rate_data.get('origin_customs_charges', 0) or 0)
        expected_customs_destination = float(rate_data.get('destination_customs_charges', 0) or 0)
        expected_other = float(rate_data.get('other_charges', 0) or 0)
        
        # Compare with actual YTD charges (using correct column names)
        actual_freight = float(invoice_data.get('freight_charges_usd', 0) or 0)
        actual_fuel = float(invoice_data.get('fuel_surcharges_usd', 0) or 0)
        actual_origin = float(invoice_data.get('origin_handling_charges_usd', 0) or 0)
        actual_destination = float(invoice_data.get('destination_handling_charges_usd', 0) or 0)
        actual_security = float(invoice_data.get('security_surcharges_usd', 0) or 0)
        actual_pickup = float(invoice_data.get('pickup_charges_usd', 0) or 0)
        actual_delivery = float(invoice_data.get('delivery_charges_usd', 0) or 0)
        actual_customs_origin = float(invoice_data.get('origin_customs_charges_usd', 0) or 0)
        actual_customs_destination = float(invoice_data.get('destination_customs_charges_usd', 0) or 0)
        actual_other = float(invoice_data.get('other_charges_usd', 0) or 0)
        
        # Calculate variances
        freight_variance = actual_freight - expected_freight
        fuel_variance = actual_fuel - expected_fuel
        origin_variance = actual_origin - expected_origin
        destination_variance = actual_destination - expected_destination
        security_variance = actual_security - expected_security
        pickup_variance = actual_pickup - expected_pickup
        delivery_variance = actual_delivery - expected_delivery
        customs_origin_variance = actual_customs_origin - expected_customs_origin
        customs_destination_variance = actual_customs_destination - expected_customs_destination
        other_variance = actual_other - expected_other
        
        # Store detailed variances
        audit_result['variances'] = [
            {
                'charge_type': 'Freight',
                'actual': actual_freight,
                'expected': expected_freight,
                'variance': freight_variance,
                'variance_pct': (freight_variance / expected_freight * 100) if expected_freight > 0 else 0
            },
            {
                'charge_type': 'Fuel Surcharge',
                'actual': actual_fuel,
                'expected': expected_fuel,
                'variance': fuel_variance,
                'variance_pct': (fuel_variance / expected_fuel * 100) if expected_fuel > 0 else 0
            },
            {
                'charge_type': 'Security Surcharges',
                'actual': actual_security,
                'expected': expected_security,
                'variance': security_variance,
                'variance_pct': (security_variance / expected_security * 100) if expected_security > 0 else 0
            },
            {
                'charge_type': 'Origin Handling',
                'actual': actual_origin,
                'expected': expected_origin,
                'variance': origin_variance,
                'variance_pct': (origin_variance / expected_origin * 100) if expected_origin > 0 else 0
            },
            {
                'charge_type': 'Destination Handling',
                'actual': actual_destination,
                'expected': expected_destination,
                'variance': destination_variance,
                'variance_pct': (destination_variance / expected_destination * 100) if expected_destination > 0 else 0
            },
            {
                'charge_type': 'Pickup Charges',
                'actual': actual_pickup,
                'expected': expected_pickup,
                'variance': pickup_variance,
                'variance_pct': (pickup_variance / expected_pickup * 100) if expected_pickup > 0 else 0
            },
            {
                'charge_type': 'Delivery Charges',
                'actual': actual_delivery,
                'expected': expected_delivery,
                'variance': delivery_variance,
                'variance_pct': (delivery_variance / expected_delivery * 100) if expected_delivery > 0 else 0
            },
            {
                'charge_type': 'Customs Charges',
                'actual': actual_customs_origin + actual_customs_destination,
                'expected': expected_customs_origin + expected_customs_destination,
                'variance': customs_origin_variance + customs_destination_variance,
                'variance_pct': ((customs_origin_variance + customs_destination_variance) / 
                               (expected_customs_origin + expected_customs_destination) * 100) 
                               if (expected_customs_origin + expected_customs_destination) > 0 else 0
            },
            {
                'charge_type': 'Other Charges',
                'actual': actual_other,
                'expected': expected_other,
                'variance': other_variance,
                'variance_pct': (other_variance / expected_other * 100) if expected_other > 0 else 0
            }
        ]
        
        # Calculate total variance
        total_variance = (freight_variance + fuel_variance + security_variance + 
                         origin_variance + destination_variance + pickup_variance + 
                         delivery_variance + customs_origin_variance + 
                         customs_destination_variance + other_variance)
        audit_result['total_variance'] = total_variance
        
        # Set audit status based on variance threshold
        variance_threshold = 50.0  # $50 threshold
        if abs(total_variance) > variance_threshold:
            audit_result['audit_status'] = 'FAIL'
        elif abs(total_variance) > 10.0:
            audit_result['audit_status'] = 'WARNING'
        
        return audit_result
    
    def calculate_ocean_audit_variance(self, invoice_data: dict, rate_data: dict) -> dict:
        """Calculate variance between YTD actual charges and ocean rate card"""
        audit_result = {
            'invoice_no': invoice_data.get('invoice_no'),
            'rate_card': rate_data.get('lane_id'),
            'lane_description': rate_data.get('lane_description'),
            'variances': [],
            'total_variance': 0.0,
            'audit_status': 'PASS'
        }
        
        # Determine if FCL or LCL shipment (using correct YTD column names)
        fcl_lcl = invoice_data.get('fcl_lcl', '').upper()
        nb_20ft = int(invoice_data.get('nb_of_20ft_containers', 0) or 0)
        nb_40ft = int(invoice_data.get('nb_of_40ft_containers', 0) or 0)
        
        if 'FCL' in fcl_lcl or nb_20ft > 0 or nb_40ft > 0:
            # FCL audit
            if nb_20ft > 0:
                expected_total = float(rate_data.get('fcl_20ft_total', 0) or 0) * nb_20ft
            else:
                expected_total = float(rate_data.get('fcl_40ft_total', 0) or 0) * max(nb_40ft, 1)
            
            actual_total = float(invoice_data.get('total_charges_without_duty_tax_usd', 0) or 0)
            
            container_info = f"FCL ({nb_20ft}x20' + {nb_40ft}x40')"
            
            audit_result['variances'] = [{
                'charge_type': container_info,
                'actual': actual_total,
                'expected': expected_total,
                'variance': actual_total - expected_total,
                'variance_pct': ((actual_total - expected_total) / expected_total * 100) if expected_total > 0 else 0
            }]
            
        else:
            # LCL audit
            volume_cbm = float(invoice_data.get('total_shipment_volume_m3', 0) or 0)
            expected_min = float(rate_data.get('lcl_min_total', 0) or 0)
            expected_cbm_rate = float(rate_data.get('lcl_cbm_rate', 0) or 0)
            
            expected_total = max(expected_min, expected_cbm_rate * volume_cbm)
            actual_total = float(invoice_data.get('total_charges_without_duty_tax_usd', 0) or 0)
            
            audit_result['variances'] = [{
                'charge_type': f'LCL ({volume_cbm} CBM)',
                'actual': actual_total,
                'expected': expected_total,
                'variance': actual_total - expected_total,
                'variance_pct': ((actual_total - expected_total) / expected_total * 100) if expected_total > 0 else 0
            }]
        
        # Calculate total variance
        total_variance = sum(v['variance'] for v in audit_result['variances'])
        audit_result['total_variance'] = total_variance
        
        # Set audit status
        variance_threshold = 100.0  # $100 threshold for ocean
        if abs(total_variance) > variance_threshold:
            audit_result['audit_status'] = 'FAIL'
        elif abs(total_variance) > 25.0:
            audit_result['audit_status'] = 'WARNING'
        
        return audit_result
    
    def audit_invoice(self, invoice_no: str) -> dict:
        """Perform complete audit of a YTD invoice against applicable rate cards"""
        # Get YTD invoice data
        invoice_data = self.get_ytd_invoice(invoice_no)
        if not invoice_data:
            return {'error': 'Invoice not found in YTD data'}
        
        audit_results = {
            'invoice_no': invoice_no,
            'transportation_mode': invoice_data.get('transportation_mode'),
            'origin': f"{invoice_data.get('origin', 'Unknown')} ({invoice_data.get('origin_port_country_code', '')})",
            'destination': f"{invoice_data.get('destination', 'Unknown')} ({invoice_data.get('destination_port_country_code', '')})",
            'air_audits': [],
            'ocean_audits': [],
            'summary': {
                'total_matches': 0,
                'best_match': None,
                'overall_status': 'NO_RATES_FOUND'
            }
        }
        
        transportation_mode = invoice_data.get('transportation_mode', '').lower()
        
        # Air freight audit
        if 'air' in transportation_mode:
            air_rates = self.find_matching_air_rates(invoice_data)
            for rate in air_rates:
                audit_result = self.calculate_air_audit_variance(invoice_data, rate)
                audit_results['air_audits'].append(audit_result)
        
        # Ocean freight audit  
        if 'sea' in transportation_mode or 'ocean' in transportation_mode:
            ocean_rates = self.find_matching_ocean_rates(invoice_data)
            for rate in ocean_rates:
                audit_result = self.calculate_ocean_audit_variance(invoice_data, rate)
                audit_results['ocean_audits'].append(audit_result)
        
        # Calculate summary
        all_audits = audit_results['air_audits'] + audit_results['ocean_audits']
        audit_results['summary']['total_matches'] = len(all_audits)
        
        if all_audits:
            # Find best match (lowest absolute variance)
            best_audit = min(all_audits, key=lambda x: abs(x['total_variance']))
            audit_results['summary']['best_match'] = best_audit
            audit_results['summary']['overall_status'] = best_audit['audit_status']
        
        return audit_results


# Flask Routes
@ytd_audit_bp.route('/')
def dashboard():
    """YTD Audit Dashboard"""
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Get statistics
    cursor.execute("SELECT COUNT(*) FROM dhl_ytd_invoices")
    total_invoices = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM air_rate_cards")
    total_air_cards = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM ocean_rate_cards")  
    total_ocean_rates = cursor.fetchone()[0]
    
    cursor.execute("SELECT transportation_mode, COUNT(*) FROM dhl_ytd_invoices GROUP BY transportation_mode")
    mode_breakdown = cursor.fetchall()
    
    conn.close()
    
    return render_template('ytd_audit_dashboard.html',
                         total_invoices=total_invoices,
                         total_air_cards=total_air_cards,
                         total_ocean_rates=total_ocean_rates,
                         mode_breakdown=mode_breakdown)

@ytd_audit_bp.route('/search')
def search_invoices():
    """Search YTD invoices for audit"""
    search_term = request.args.get('q', '')
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    if search_term:
        cursor.execute('''
            SELECT invoice_no, transportation_mode, origin, destination,
                   origin_port_country_code, destination_port_country_code, total_charges_without_duty_tax_usd
            FROM dhl_ytd_invoices 
            WHERE invoice_no LIKE ? OR origin LIKE ? OR destination LIKE ? 
               OR origin_port_country_code LIKE ? OR destination_port_country_code LIKE ?
            ORDER BY invoice_no
            LIMIT 50
        ''', (f'%{search_term}%', f'%{search_term}%', f'%{search_term}%', 
              f'%{search_term}%', f'%{search_term}%'))
        invoices = cursor.fetchall()
    else:
        invoices = []
    
    conn.close()
    
    return render_template('ytd_audit_search.html', 
                         invoices=invoices, 
                         search_term=search_term)

@ytd_audit_bp.route('/audit/<invoice_no>')
def audit_invoice(invoice_no):
    """Audit specific invoice against rate cards using improved logic"""
    # Use the improved audit engine for better analysis
    from updated_ytd_audit_engine import UpdatedYTDAuditEngine
    
    try:
        engine = UpdatedYTDAuditEngine()
        results = engine.audit_invoice(invoice_no)
        
        if 'error' in results:
            flash(results['error'], 'error')
            return redirect(url_for('ytd_audit.search_invoices'))
        
        return render_template('ytd_audit_results_new.html', results=results)
        
    except Exception as e:
        flash(f'Error auditing invoice: {str(e)}', 'error')
        return redirect(url_for('ytd_audit.search_invoices'))

@ytd_audit_bp.route('/api/audit/<invoice_no>')
def api_audit_invoice(invoice_no):
    """API endpoint for invoice audit"""
    from updated_ytd_audit_engine import UpdatedYTDAuditEngine
    audit_engine = UpdatedYTDAuditEngine()
    results = audit_engine.audit_invoice(invoice_no)
    return jsonify(results)

@ytd_audit_bp.route('/batch_audit')
def batch_audit():
    """Batch audit multiple invoices"""
    # Provide default values for template variables
    return render_template('ytd_batch_audit.html',
                         summary={},
                         recent_batches=[],
                         status_counts={},
                         latest_batch_stats={},
                         total_results=0,
                         total_batch_runs=0)

@ytd_audit_bp.route('/api/batch_audit', methods=['POST'])
def api_batch_audit():
    """API endpoint for batch audit"""
    invoice_numbers = request.json.get('invoice_numbers', [])
    
    from updated_ytd_audit_engine import UpdatedYTDAuditEngine
    audit_engine = UpdatedYTDAuditEngine()
    results = []
    
    for invoice_no in invoice_numbers:
        result = audit_engine.audit_invoice(invoice_no.strip())
        results.append(result)
    
    return jsonify({'results': results, 'total_processed': len(results)})

if __name__ == '__main__':
    # Test the audit engine
    from updated_ytd_audit_engine import UpdatedYTDAuditEngine
    audit_engine = UpdatedYTDAuditEngine()
    
    # Test with a sample invoice
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    cursor.execute("SELECT invoice_no FROM dhl_ytd_invoices LIMIT 1")
    sample_invoice = cursor.fetchone()
    conn.close()
    
    if sample_invoice:
        print(f"Testing audit with invoice: {sample_invoice[0]}")
        result = audit_engine.audit_invoice(sample_invoice[0])
        print(json.dumps(result, indent=2, default=str))
    else:
        print("No YTD invoices found for testing")
