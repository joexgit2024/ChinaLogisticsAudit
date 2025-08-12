#!/usr/bin/env python3
"""
Updated YTD Audit Engine - Unified Audit Logic
==============================================

This engine now uses the sophisticated OceanFreightAuditEngine and AirFreightAuditEngine
to ensure consistent audit logic across individual and batch audits.
"""

import sqlite3
import json
from typing import Dict, List, Optional
from datetime import datetime
from flask import Blueprint, render_template, request, jsonify

# Import the sophisticated audit engines
from ocean_freight_audit_engine import OceanFreightAuditEngine
from air_freight_audit_engine import AirFreightAuditEngine

# Create Flask Blueprint for improved YTD audit
improved_ytd_audit_bp = Blueprint('improved_ytd_audit', __name__, url_prefix='/improved_ytd_audit')

class UpdatedYTDAuditEngine:
    """
    Unified YTD Audit Engine that delegates to specialized audit engines
    for consistent results across individual and batch audits.
    """
    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path
        # Initialize the specialized audit engines
        self.ocean_audit_engine = OceanFreightAuditEngine(db_path)
        self.air_audit_engine = AirFreightAuditEngine(db_path)
    
    def get_ytd_invoice(self, invoice_no: str) -> Optional[Dict]:
        """Get YTD invoice details by invoice number"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM dhl_ytd_invoices WHERE invoice_no = ?', (invoice_no,))
        invoice_row = cursor.fetchone()
        
        if not invoice_row:
            conn.close()
            return None
        
        cursor.execute('PRAGMA table_info(dhl_ytd_invoices)')
        columns = [col[1] for col in cursor.fetchall()]
        invoice_data = dict(zip(columns, invoice_row))
        
        conn.close()
        return invoice_data

    def audit_invoice(self, invoice_no: str) -> Dict:
        """
        Perform comprehensive audit of YTD invoice using specialized audit engines
        """
        invoice_data = self.get_ytd_invoice(invoice_no)
        if not invoice_data:
            return {'error': 'Invoice not found in YTD data'}
        
        # Determine transportation mode
        transportation_mode = invoice_data.get('transportation_mode', '').lower()
        
        # Prepare result structure
        result = {
            'invoice_no': invoice_no,
            'invoice_details': self._prepare_invoice_details(invoice_data),
            'audit_results': [],
            'summary': {
                'total_rate_cards_checked': 0,
                'best_match': None,
                'overall_status': 'NO_RATES_FOUND',
                'total_variance': 0
            }
        }
        
        try:
            if transportation_mode in ['air']:
                # Use AirFreightAuditEngine for air freight
                air_audit_result = self.air_audit_engine.audit_air_freight_invoice(invoice_no)
                result = self._convert_air_audit_result(air_audit_result, invoice_data)
                
            elif transportation_mode in ['sea', 'ocean', 'lcl', 'fcl', 'maritime']:
                # Use OceanFreightAuditEngine for ocean freight
                ocean_audit_result = self.ocean_audit_engine.audit_ocean_freight_invoice(invoice_no)
                result = self._convert_ocean_audit_result(ocean_audit_result, invoice_data)
                
            else:
                result['summary']['overall_status'] = 'UNSUPPORTED_MODE'
                result['error'] = f'Transportation mode "{transportation_mode}" is not supported'
                
        except Exception as e:
            result['error'] = f'Audit failed: {str(e)}'
            result['summary']['overall_status'] = 'ERROR'
        
        return result
    
    def _prepare_invoice_details(self, invoice_data: Dict) -> Dict:
        """Prepare invoice details for display"""
        # Determine which weight to use for display and calculations
        chargeable_weight = invoice_data.get('total_shipment_chargeable_weight_kg')
        actual_weight = invoice_data.get('shipment_weight_kg')
        
        # Convert to float, treating None, empty string, or 0 as invalid
        try:
            chargeable_kg = float(chargeable_weight) if chargeable_weight not in [None, '', '0', 0] else 0
        except (ValueError, TypeError):
            chargeable_kg = 0
            
        try:
            actual_kg = float(actual_weight) if actual_weight not in [None, '', '0'] else 0
        except (ValueError, TypeError):
            actual_kg = 0
        
        # Use chargeable weight if available and > 0, otherwise use actual weight
        display_weight = chargeable_kg if chargeable_kg > 0 else actual_kg
        
        return {
            'transportation_mode': invoice_data.get('transportation_mode'),
            'origin': invoice_data.get('origin'),
            'destination': invoice_data.get('destination'),
            'origin_country': invoice_data.get('origin_port_country_code'),
            'destination_country': invoice_data.get('destination_port_country_code'),
            'weight_kg': display_weight,
            'chargeable_weight_kg': chargeable_kg,
            'actual_weight_kg': actual_kg,
            'volume_m3': invoice_data.get('total_shipment_volume_m3'),
            'total_charges_usd': invoice_data.get('total_charges_without_duty_tax_usd'),
            'total_amount': invoice_data.get('total_charges_without_duty_tax_usd'),
            'invoice_date': invoice_data.get('invoice_date'),
            'currency': 'USD'
        }
    
    def _convert_air_audit_result(self, air_result: Dict, invoice_data: Dict) -> Dict:
        """Convert AirFreightAuditEngine result to unified format"""
        
        if air_result.get('audit_status') == 'error':
            return {
                'invoice_no': invoice_data.get('invoice_no'),
                'invoice_details': self._prepare_invoice_details(invoice_data),
                'audit_results': [],
                'summary': {
                    'total_rate_cards_checked': 0,
                    'best_match': None,
                    'overall_status': 'ERROR',
                    'total_variance': 0
                },
                'error': air_result.get('reason', 'Air freight audit failed')
            }
        
        # Extract charge breakdown from air audit result
        charge_breakdown = air_result.get('charge_breakdown', [])
        rate_card_info = air_result.get('rate_card_info', {})
        
        # Convert to unified audit result format
        audit_result = {
            'invoice_no': invoice_data.get('invoice_no'),
            'lane_id': rate_card_info.get('lane_id', 'N/A'),
            'lane_description': rate_card_info.get('lane_name', 'Air Freight Lane'),
            'service': 'Air Freight',
            'variances': self._convert_air_charge_breakdown(charge_breakdown),
            'total_expected': air_result.get('invoice_data', {}).get('total_expected_usd', 0),
            'total_actual': air_result.get('invoice_data', {}).get('total_actual_usd', 0),
            'total_variance': air_result.get('invoice_data', {}).get('total_variance_usd', 0),
            'audit_status': self._map_audit_status(air_result.get('audit_status')),
            'status_color': self._get_status_color(air_result.get('audit_status')),
            'status_reason': air_result.get('audit_reason', ''),
            'rate_card_info': {
                'card_name': rate_card_info.get('rate_card_name', 'Air Rate Card'),
                'validity_start': rate_card_info.get('validity_start', ''),
                'validity_end': rate_card_info.get('validity_end', '')
            }
        }
        
        return {
            'invoice_no': invoice_data.get('invoice_no'),
            'invoice_details': self._prepare_invoice_details(invoice_data),
            'audit_results': [audit_result],
            'summary': {
                'total_rate_cards_checked': 1,
                'best_match': audit_result,
                'overall_status': audit_result['audit_status'],
                'total_variance': audit_result['total_variance']
            }
        }
    
    def _convert_ocean_audit_result(self, ocean_result: Dict, invoice_data: Dict) -> Dict:
        """Convert OceanFreightAuditEngine result to unified format"""
        
        if ocean_result.get('audit_status') == 'error':
            return {
                'invoice_no': invoice_data.get('invoice_no'),
                'invoice_details': self._prepare_invoice_details(invoice_data),
                'audit_results': [],
                'summary': {
                    'total_rate_cards_checked': 0,
                    'best_match': None,
                    'overall_status': 'ERROR',
                    'total_variance': 0
                },
                'error': ocean_result.get('reason', 'Ocean freight audit failed')
            }
        
        # Extract data from ocean audit result
        charge_breakdown = ocean_result.get('charge_breakdown', [])
        rate_card_info = ocean_result.get('rate_card_info', {})
        
        # Convert to unified audit result format
        audit_result = {
            'invoice_no': invoice_data.get('invoice_no'),
            'lane_id': rate_card_info.get('rate_card_id', 'N/A'),
            'lane_description': rate_card_info.get('lane_name', 'Ocean Freight Lane'),
            'service': 'Ocean Freight',
            'variances': self._convert_ocean_charge_breakdown(charge_breakdown),
            'total_expected': ocean_result.get('invoice_data', {}).get('total_expected_usd', 0),
            'total_actual': ocean_result.get('invoice_data', {}).get('total_actual_usd', 0),
            'total_variance': ocean_result.get('invoice_data', {}).get('total_variance_usd', 0),
            'audit_status': self._map_audit_status(ocean_result.get('audit_status')),
            'status_color': self._get_status_color(ocean_result.get('audit_status')),
            'status_reason': ocean_result.get('audit_reason', ''),
            'rate_card_info': {
                'card_name': rate_card_info.get('rate_card_name', 'Ocean Rate Card'),
                'validity_start': rate_card_info.get('validity_start', ''),
                'validity_end': rate_card_info.get('validity_end', '')
            }
        }
        
        return {
            'invoice_no': invoice_data.get('invoice_no'),
            'invoice_details': self._prepare_invoice_details(invoice_data),
            'audit_results': [audit_result],
            'summary': {
                'total_rate_cards_checked': 1,
                'best_match': audit_result,
                'overall_status': audit_result['audit_status'],
                'total_variance': audit_result['total_variance']
            }
        }
    
    def _convert_air_charge_breakdown(self, charge_breakdown: List[Dict]) -> List[Dict]:
        """Convert air freight charge breakdown to unified format"""
        variances = []
        
        for charge in charge_breakdown:
            variances.append({
                'charge_type': charge.get('charge_type', 'Unknown'),
                'expected': charge.get('expected_amount', 0),
                'actual': charge.get('actual_amount', 0),
                'variance': charge.get('variance_amount', 0),
                'variance_pct': charge.get('variance_percentage', 0),
                'analysis': charge.get('analysis', '')
            })
        
        return variances
    
    def _convert_ocean_charge_breakdown(self, charge_breakdown: List[Dict]) -> List[Dict]:
        """Convert ocean freight charge breakdown to unified format"""
        variances = []
        
        for charge in charge_breakdown:
            variances.append({
                'charge_type': charge.get('charge_type', 'Unknown'),
                'expected': charge.get('expected_amount', 0),
                'actual': charge.get('actual_amount', 0),
                'variance': charge.get('variance_amount', 0),
                'variance_pct': charge.get('variance_percentage', 0),
                'analysis': charge.get('analysis', '')
            })
        
        return variances
    
    def _map_audit_status(self, status: str) -> str:
        """Map audit engine status to unified status"""
        status_mapping = {
            'approved': 'PASS',
            'review_required': 'WARNING', 
            'rejected': 'FAIL',
            'error': 'ERROR',
            'skipped': 'ERROR'
        }
        return status_mapping.get(status.lower() if status else '', 'ERROR')
    
    def _get_status_color(self, status: str) -> str:
        """Get status color for UI display"""
        color_mapping = {
            'approved': 'success',
            'review_required': 'warning',
            'rejected': 'danger',
            'error': 'danger',
            'skipped': 'secondary'
        }
        return color_mapping.get(status.lower() if status else '', 'secondary')


# Flask Routes
@improved_ytd_audit_bp.route('/')
def dashboard():
    """Improved YTD Audit Dashboard"""
    return render_template('ytd_audit_dashboard.html')

@improved_ytd_audit_bp.route('/audit/<invoice_no>')
def audit_invoice(invoice_no):
    """Audit specific invoice with unified logic using specialized engines"""
    engine = UpdatedYTDAuditEngine()
    result = engine.audit_invoice(invoice_no)
    
    if 'error' in result:
        return render_template('ytd_audit_results_new.html', error=result['error'])
    
    return render_template('ytd_audit_results_new.html', 
                         invoice_no=invoice_no, 
                         results=result)

@improved_ytd_audit_bp.route('/api/audit/<invoice_no>')
def api_audit_invoice(invoice_no):
    """API endpoint for invoice audit"""
    engine = UpdatedYTDAuditEngine()
    result = engine.audit_invoice(invoice_no)
    return jsonify(result)

# Test the updated engine
if __name__ == "__main__":
    engine = UpdatedYTDAuditEngine()
    result = engine.audit_invoice('D2163241')
    print(json.dumps(result, indent=2, default=str))
