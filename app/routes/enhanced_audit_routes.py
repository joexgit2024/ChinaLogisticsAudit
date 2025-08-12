"""
Enhanced Audit Routes
====================

Web interface for comprehensive invoice audit results
"""

from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
import sqlite3
from datetime import datetime
import json
import logging

# Create blueprint
audit_bp = Blueprint('enhanced_audit', __name__)
logger = logging.getLogger(__name__)

def get_db_connection():
    """Get database connection"""
    conn = sqlite3.connect('dhl_audit.db')
    conn.row_factory = sqlite3.Row
    return conn

@audit_bp.route('/enhanced-audit')
def enhanced_audit_dashboard():
    """Enhanced audit dashboard showing comprehensive results"""
    try:
        conn = get_db_connection()
        
        # Get audit summary
        summary = conn.execute('''
            SELECT 
                audit_status,
                COUNT(*) as count,
                AVG(variance_percent) as avg_variance,
                SUM(total_variance) as total_variance
            FROM detailed_audit_results 
            GROUP BY audit_status
        ''').fetchall()
        
        # Get recent audit results
        results = conn.execute('''
            SELECT * FROM detailed_audit_results 
            ORDER BY ABS(variance_percent) DESC
            LIMIT 20
        ''').fetchall()
        
        # Parse audit details for display
        for result in results:
            try:
                if result['audit_details']:
                    # Convert string representation to dict for template
                    audit_details = eval(result['audit_details'])
                    result = dict(result)
                    result['parsed_details'] = audit_details
            except:
                result = dict(result)
                result['parsed_details'] = {}
        
        conn.close()
        
        return render_template('enhanced_audit_dashboard.html', 
                             summary=summary, results=results)
    
    except Exception as e:
        logger.error(f"Error in enhanced audit dashboard: {e}")
        return f"Error loading audit dashboard: {e}", 500

@audit_bp.route('/enhanced-audit/<invoice_number>')
def enhanced_audit_detail(invoice_number):
    """Detailed audit view for a specific invoice"""
    try:
        conn = get_db_connection()
        
        # Get audit result
        audit_result = conn.execute('''
            SELECT * FROM detailed_audit_results 
            WHERE invoice_number = ?
        ''', (invoice_number,)).fetchone()
        
        if not audit_result:
            flash(f'Audit result not found for invoice {invoice_number}', 'error')
            return redirect(url_for('enhanced_audit.enhanced_audit_dashboard'))
        
        # Get original invoice data
        invoice = conn.execute('''
            SELECT * FROM invoices 
            WHERE invoice_number = ?
        ''', (invoice_number,)).fetchone()
        
        # Parse audit details
        audit_details = {}
        try:
            if audit_result['audit_details']:
                audit_details = eval(audit_result['audit_details'])
        except Exception as e:
            logger.error(f"Error parsing audit details: {e}")
        
        conn.close()
        
        return render_template('enhanced_audit_detail.html', 
                             audit_result=audit_result,
                             invoice=invoice,
                             audit_details=audit_details)
    
    except Exception as e:
        logger.error(f"Error in enhanced audit detail: {e}")
        return f"Error loading audit detail: {e}", 500

@audit_bp.route('/run-enhanced-audit', methods=['POST'])
def run_enhanced_audit():
    """Run the enhanced audit process"""
    try:
        from app.enhanced_invoice_auditor import run_comprehensive_audit
        
        # Run the audit
        results = run_comprehensive_audit()
        
        flash(f'Enhanced audit completed successfully! Audited {len(results)} invoices.', 'success')
        return redirect(url_for('enhanced_audit.enhanced_audit_dashboard'))
    
    except Exception as e:
        logger.error(f"Error running enhanced audit: {e}")
        flash(f'Error running enhanced audit: {e}', 'error')
        return redirect(url_for('enhanced_audit.enhanced_audit_dashboard'))

@audit_bp.route('/api/enhanced-audit-summary')
def api_enhanced_audit_summary():
    """API endpoint for audit summary data"""
    try:
        conn = get_db_connection()
        
        # Get summary data
        summary = {}
        
        # Status counts
        status_counts = conn.execute('''
            SELECT audit_status, COUNT(*) as count
            FROM detailed_audit_results 
            GROUP BY audit_status
        ''').fetchall()
        
        summary['status_counts'] = {row['audit_status']: row['count'] for row in status_counts}
        
        # Variance statistics
        variance_stats = conn.execute('''
            SELECT 
                COUNT(*) as total_invoices,
                AVG(variance_percent) as avg_variance,
                MIN(variance_percent) as min_variance,
                MAX(variance_percent) as max_variance,
                SUM(total_variance) as total_variance_amount
            FROM detailed_audit_results
            WHERE audit_status != 'skipped'
        ''').fetchone()
        
        summary['variance_stats'] = dict(variance_stats)
        
        # Top variances
        top_variances = conn.execute('''
            SELECT invoice_number, variance_percent, total_variance, audit_status
            FROM detailed_audit_results 
            WHERE audit_status != 'skipped'
            ORDER BY ABS(variance_percent) DESC
            LIMIT 10
        ''').fetchall()
        
        summary['top_variances'] = [dict(row) for row in top_variances]
        
        conn.close()
        return jsonify(summary)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
