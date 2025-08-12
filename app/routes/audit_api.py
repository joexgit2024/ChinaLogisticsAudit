"""
API Routes for invoice auditing
"""
from flask import Blueprint, request, jsonify
from app.database import get_db_connection
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.models.air_audit import audit_air_invoice

audit_api_bp = Blueprint('audit_api', __name__)

@audit_api_bp.route('/api/audit/air/<int:invoice_id>', methods=['POST'])
def audit_air_freight_invoice(invoice_id):
    """Audit an air freight invoice against rate card."""
    try:
        result = audit_air_invoice(invoice_id)
        return jsonify(result)
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error during audit: {str(e)}'
        }), 500

@audit_api_bp.route('/api/audit/batch', methods=['POST'])
def batch_audit():
    """Audit multiple invoices in batch."""
    try:
        data = request.get_json()
        
        if not data or 'invoice_ids' not in data:
            return jsonify({
                'success': False,
                'message': 'No invoice IDs provided'
            }), 400
            
        invoice_ids = data['invoice_ids']
        shipping_mode = data.get('shipping_mode', 'air')
        
        results = []
        
        for invoice_id in invoice_ids:
            try:
                if shipping_mode.lower() == 'air':
                    result = audit_air_invoice(invoice_id)
                else:
                    result = {
                        'success': False,
                        'message': f'Unsupported shipping mode: {shipping_mode}'
                    }
                
                results.append({
                    'invoice_id': invoice_id,
                    'result': result
                })
            except Exception as e:
                results.append({
                    'invoice_id': invoice_id,
                    'success': False,
                    'message': f'Error: {str(e)}'
                })
        
        return jsonify({
            'success': True,
            'results': results
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Error during batch audit: {str(e)}'
        }), 500
