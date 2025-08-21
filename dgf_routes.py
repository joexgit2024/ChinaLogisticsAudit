#!/usr/bin/env python3
"""
DGF Audit Routes for Web Interface
Flask routes for DGF air and sea freight audit system
"""

from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash, send_file
import os
import json
from datetime import datetime
import pandas as pd
from dgf_audit_system import DGFAuditSystem
import sqlite3

dgf_bp = Blueprint('dgf', __name__, url_prefix='/dgf')

@dgf_bp.route('/')
def dashboard():
    """DGF audit dashboard."""
    try:
        audit_system = DGFAuditSystem()
        
        # Get summary statistics
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Count quotes by mode
        cursor.execute('''
            SELECT mode, COUNT(*) as count 
            FROM dgf_spot_quotes 
            WHERE status = 'ACTIVE'
            GROUP BY mode
        ''')
        quote_stats = dict(cursor.fetchall())
        
        # Count invoices by mode and status
        cursor.execute('''
            SELECT mode, status, COUNT(*) as count
            FROM dgf_invoices
            GROUP BY mode, status
        ''')
        invoice_stats = {}
        for mode, status, count in cursor.fetchall():
            if mode not in invoice_stats:
                invoice_stats[mode] = {}
            invoice_stats[mode][status] = count
        
        # Get recent audit results
        cursor.execute('''
            SELECT 
                i.quote_id,
                i.mode,
                i.hbl_number,
                ar.overall_status,
                ar.audit_score,
                ar.net_variance,
                ar.audit_date
            FROM dgf_audit_results ar
            JOIN dgf_invoices i ON ar.invoice_id = i.id
            ORDER BY ar.audit_date DESC
            LIMIT 10
        ''')
        recent_audits = cursor.fetchall()
        
        # Get audit summary
        cursor.execute('''
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN overall_status = 'PASS' THEN 1 ELSE 0 END) as passed,
                SUM(CASE WHEN overall_status = 'WARNING' THEN 1 ELSE 0 END) as warnings,
                SUM(CASE WHEN overall_status = 'FAIL' THEN 1 ELSE 0 END) as failed,
                AVG(audit_score) as avg_score,
                SUM(overcharge_amount) as total_overcharge,
                SUM(undercharge_amount) as total_undercharge
            FROM dgf_audit_results
        ''')
        audit_summary = cursor.fetchone()
        
        conn.close()
        
        return render_template('dgf/dashboard.html', 
                             quote_stats=quote_stats,
                             invoice_stats=invoice_stats,
                             recent_audits=recent_audits,
                             audit_summary=audit_summary)
    
    except Exception as e:
        flash(f'Error loading dashboard: {str(e)}', 'error')
        return render_template('dgf/dashboard.html', 
                             quote_stats={}, invoice_stats={}, 
                             recent_audits=[], audit_summary=None)

@dgf_bp.route('/upload', methods=['GET', 'POST'])
def upload_files():
    """Upload DGF air and sea freight files."""
    if request.method == 'POST':
        try:
            audit_system = DGFAuditSystem()
            
            air_file = request.files.get('air_file')
            sea_file = request.files.get('sea_file')
            
            air_file_path = None
            sea_file_path = None
            
            if air_file and air_file.filename.endswith('.xlsx'):
                air_file_path = os.path.join('uploads/DGF AIR', air_file.filename)
                os.makedirs(os.path.dirname(air_file_path), exist_ok=True)
                air_file.save(air_file_path)
            
            if sea_file and sea_file.filename.endswith('.xlsx'):
                sea_file_path = os.path.join('uploads/DGF SEA', sea_file.filename)
                os.makedirs(os.path.dirname(sea_file_path), exist_ok=True)
                sea_file.save(sea_file_path)
            
            # Process files
            audit_system.load_and_process_dgf_files(air_file_path, sea_file_path)
            
            flash('Files uploaded and processed successfully!', 'success')
            return redirect(url_for('dgf.dashboard'))
            
        except Exception as e:
            flash(f'Error processing files: {str(e)}', 'error')
    
    return render_template('dgf/upload.html')

@dgf_bp.route('/quotes')
def view_quotes():
    """View DGF spot quotes."""
    try:
        mode = request.args.get('mode', 'ALL')
        
        conn = sqlite3.connect('dhl_audit.db')
        
        query = '''
            SELECT 
                quote_id,
                mode,
                lane,
                origin_port,
                destination_port,
                terms,
                rate_per_kg,
                rate_per_cbm,
                origin_handling_fee,
                dest_handling_fee,
                quote_date,
                status
            FROM dgf_spot_quotes
            WHERE status = 'ACTIVE'
        '''
        
        params = []
        if mode != 'ALL':
            query += ' AND mode = ?'
            params.append(mode)
        
        query += ' ORDER BY quote_date DESC'
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        quotes = df.to_dict('records')
        
        return render_template('dgf/quotes.html', quotes=quotes, selected_mode=mode)
    
    except Exception as e:
        flash(f'Error loading quotes: {str(e)}', 'error')
        return render_template('dgf/quotes.html', quotes=[], selected_mode='ALL')

@dgf_bp.route('/invoices')
def view_invoices():
    """View DGF invoices."""
    try:
        mode = request.args.get('mode', 'ALL')
        status = request.args.get('status', 'ALL')
        
        conn = sqlite3.connect('dhl_audit.db')
        
        query = '''
            SELECT 
                i.id,
                i.quote_id,
                i.mode,
                i.hbl_number,
                i.actual_arrival_date,
                i.pieces,
                i.gross_weight,
                i.chargeable_weight,
                i.volume_cbm,
                i.origin_port,
                i.destination_port,
                i.total_cny,
                i.status,
                ar.overall_status as audit_status,
                ar.audit_score,
                ar.net_variance
            FROM dgf_invoices i
            LEFT JOIN dgf_audit_results ar ON i.id = ar.invoice_id
            WHERE 1=1
        '''
        
        params = []
        if mode != 'ALL':
            query += ' AND i.mode = ?'
            params.append(mode)
        
        if status != 'ALL':
            query += ' AND i.status = ?'
            params.append(status)
        
        query += ' ORDER BY i.actual_arrival_date DESC'
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        invoices = df.to_dict('records')
        
        return render_template('dgf/invoices.html', 
                             invoices=invoices, 
                             selected_mode=mode,
                             selected_status=status)
    
    except Exception as e:
        flash(f'Error loading invoices: {str(e)}', 'error')
        return render_template('dgf/invoices.html', 
                             invoices=[], 
                             selected_mode='ALL',
                             selected_status='ALL')

@dgf_bp.route('/audit/run', methods=['POST'])
def run_audit():
    """Run audit on all invoices."""
    try:
        audit_system = DGFAuditSystem()
        results = audit_system.audit_all_invoices()
        
        return jsonify({
            'success': True,
            'message': 'Audit completed successfully',
            'results': results
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Audit failed: {str(e)}'
        }), 500

@dgf_bp.route('/audit/invoice/<int:invoice_id>')
def audit_single_invoice(invoice_id):
    """Audit a single invoice."""
    try:
        audit_system = DGFAuditSystem()
        result = audit_system.audit_invoice_against_quote(invoice_id)
        
        if 'error' in result:
            return jsonify({
                'success': False,
                'message': result['error']
            }), 404
        
        return jsonify({
            'success': True,
            'result': result
        })
    
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Audit failed: {str(e)}'
        }), 500

@dgf_bp.route('/audit/results')
def view_audit_results():
    """View detailed audit results."""
    try:
        mode = request.args.get('mode', 'ALL')
        status = request.args.get('status', 'ALL')
        
        conn = sqlite3.connect('dhl_audit.db')
        
        query = '''
            SELECT 
                i.quote_id,
                i.mode,
                i.hbl_number,
                i.actual_arrival_date,
                i.origin_port,
                i.destination_port,
                i.total_cny,
                ar.overall_status,
                ar.audit_score,
                ar.freight_variance_pct,
                ar.freight_variance_amount,
                ar.origin_variance_pct,
                ar.origin_variance_amount,
                ar.overcharge_amount,
                ar.undercharge_amount,
                ar.net_variance,
                ar.audit_date
            FROM dgf_audit_results ar
            JOIN dgf_invoices i ON ar.invoice_id = i.id
            WHERE 1=1
        '''
        
        params = []
        if mode != 'ALL':
            query += ' AND i.mode = ?'
            params.append(mode)
        
        if status != 'ALL':
            query += ' AND ar.overall_status = ?'
            params.append(status)
        
        query += ' ORDER BY ar.audit_date DESC'
        
        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        
        results = df.to_dict('records')
        
        return render_template('dgf/audit_results.html', 
                             results=results,
                             selected_mode=mode,
                             selected_status=status)
    
    except Exception as e:
        flash(f'Error loading audit results: {str(e)}', 'error')
        return render_template('dgf/audit_results.html', 
                             results=[],
                             selected_mode='ALL',
                             selected_status='ALL')

@dgf_bp.route('/reports/generate')
def generate_report():
    """Generate and download audit report."""
    try:
        audit_system = DGFAuditSystem()
        report_file = audit_system.generate_audit_report('dgf_audit_report.xlsx')
        
        return send_file(report_file, 
                        as_attachment=True,
                        download_name=f'DGF_Audit_Report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx')
    
    except Exception as e:
        flash(f'Error generating report: {str(e)}', 'error')
        return redirect(url_for('dgf.dashboard'))

@dgf_bp.route('/api/stats')
def get_stats():
    """Get audit statistics for dashboard charts."""
    try:
        conn = sqlite3.connect('dhl_audit.db')
        
        # Audit status distribution
        cursor = conn.cursor()
        cursor.execute('''
            SELECT overall_status, COUNT(*) as count
            FROM dgf_audit_results
            GROUP BY overall_status
        ''')
        status_dist = dict(cursor.fetchall())
        
        # Mode distribution
        cursor.execute('''
            SELECT i.mode, COUNT(*) as count
            FROM dgf_invoices i
            JOIN dgf_audit_results ar ON i.id = ar.invoice_id
            GROUP BY i.mode
        ''')
        mode_dist = dict(cursor.fetchall())
        
        # Monthly trend
        cursor.execute('''
            SELECT 
                strftime('%Y-%m', audit_date) as month,
                COUNT(*) as count,
                AVG(audit_score) as avg_score
            FROM dgf_audit_results
            WHERE audit_date >= date('now', '-12 months')
            GROUP BY strftime('%Y-%m', audit_date)
            ORDER BY month
        ''')
        monthly_trend = cursor.fetchall()
        
        # Variance distribution
        cursor.execute('''
            SELECT 
                CASE 
                    WHEN ABS(net_variance) < 100 THEN '< ¥100'
                    WHEN ABS(net_variance) < 500 THEN '¥100-500'
                    WHEN ABS(net_variance) < 1000 THEN '¥500-1000'
                    ELSE '> ¥1000'
                END as variance_range,
                COUNT(*) as count
            FROM dgf_audit_results
            GROUP BY variance_range
        ''')
        variance_dist = dict(cursor.fetchall())
        
        conn.close()
        
        return jsonify({
            'status_distribution': status_dist,
            'mode_distribution': mode_dist,
            'monthly_trend': [{'month': row[0], 'count': row[1], 'avg_score': row[2]} for row in monthly_trend],
            'variance_distribution': variance_dist
        })
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500
