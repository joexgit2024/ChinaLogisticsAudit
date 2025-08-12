"""
YTD Reports Routes - Advanced reporting and export functionality
"""

from flask import Blueprint, render_template, request, jsonify, send_file, flash, redirect, url_for
import sqlite3
import pandas as pd
import io
import zipfile
from datetime import datetime, timedelta
import json
import os
from auth_routes import require_auth

ytd_reports_bp = Blueprint('ytd_reports', __name__)

class YTDReportsManager:
    def __init__(self, db_path="dhl_audit.db"):
        self.db_path = db_path
    
    def get_db_connection(self):
        return sqlite3.connect(self.db_path)
    
    def get_available_months(self):
        """Get available months from invoice data"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT DISTINCT strftime('%Y-%m', invoice_creation_date) as month
            FROM dhl_ytd_invoices 
            WHERE invoice_creation_date IS NOT NULL
            ORDER BY month DESC
        """)
        
        months = [row[0] for row in cursor.fetchall()]
        conn.close()
        return months
    
    def get_shipment_summary(self, filters=None):
        """Get invoices grouped by shipment number with totals"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        # Build WHERE clause
        where_conditions = []
        params = []
        
        if filters:
            if filters.get('month'):
                where_conditions.append("strftime('%Y-%m', i.invoice_creation_date) = ?")
                params.append(filters['month'])
            
            if filters.get('transportation_mode'):
                where_conditions.append("i.transportation_mode = ?")
                params.append(filters['transportation_mode'])
            
            if filters.get('audit_status'):
                where_conditions.append("ar.audit_status = ?")
                params.append(filters['audit_status'])
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        query = f"""
            SELECT 
                i.cw1_shipment_number,
                COUNT(i.invoice_no) as invoice_count,
                SUM(i.total_charges_with_duty_tax_usd) as total_charges_usd,
                GROUP_CONCAT(i.invoice_no) as invoice_numbers,
                i.transportation_mode,
                i.origin,
                i.destination,
                MIN(i.invoice_creation_date) as first_invoice_date,
                MAX(i.invoice_creation_date) as last_invoice_date,
                -- Audit summary
                COUNT(ar.id) as audited_invoices,
                SUM(CASE WHEN ar.audit_status = 'approved' THEN 1 ELSE 0 END) as passed_audits,
                SUM(CASE WHEN ar.audit_status = 'rejected' THEN 1 ELSE 0 END) as failed_audits,
                SUM(CASE WHEN ar.audit_status = 'No Rate Card' THEN 1 ELSE 0 END) as no_rate_card,
                SUM(CASE WHEN ar.audit_status NOT IN ('approved', 'rejected', 'No Rate Card') AND ar.audit_status IS NOT NULL THEN 1 ELSE 0 END) as review_required,
                SUM(ar.total_variance) as total_variance,
                AVG(ar.variance_percent) as avg_variance_percent
            FROM dhl_ytd_invoices i
            LEFT JOIN ytd_audit_results ar ON i.invoice_no = ar.invoice_no
            {where_clause}
            GROUP BY i.cw1_shipment_number, i.transportation_mode, i.origin, i.destination
            HAVING i.cw1_shipment_number IS NOT NULL AND i.cw1_shipment_number != ''
            ORDER BY first_invoice_date DESC, total_charges_usd DESC
        """
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        
        # Convert to list of dictionaries
        shipments = []
        for row in results:
            shipments.append({
                'cw1_shipment_number': row[0],
                'invoice_count': row[1],
                'total_charges_usd': row[2] or 0,
                'invoice_numbers': row[3].split(',') if row[3] else [],
                'transportation_mode': row[4],
                'origin': row[5],
                'destination': row[6],
                'first_invoice_date': row[7],
                'last_invoice_date': row[8],
                'audited_invoices': row[9],
                'passed_audits': row[10],
                'failed_audits': row[11],
                'no_rate_card': row[12],
                'review_required': row[13],
                'total_variance': row[14] or 0,
                'avg_variance_percent': row[15] or 0
            })
        
        return shipments
    
    def get_detailed_invoice_data(self, filters=None):
        """Get detailed invoice data with audit results"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        # Build WHERE clause
        where_conditions = []
        params = []
        
        if filters:
            if filters.get('month'):
                where_conditions.append("strftime('%Y-%m', i.invoice_creation_date) = ?")
                params.append(filters['month'])
            
            if filters.get('transportation_mode'):
                where_conditions.append("i.transportation_mode = ?")
                params.append(filters['transportation_mode'])
            
            if filters.get('audit_status'):
                where_conditions.append("ar.audit_status = ?")
                params.append(filters['audit_status'])
            
            if filters.get('cw1_shipment_number'):
                where_conditions.append("i.cw1_shipment_number = ?")
                params.append(filters['cw1_shipment_number'])
        
        where_clause = ""
        if where_conditions:
            where_clause = "WHERE " + " AND ".join(where_conditions)
        
        query = f"""
            SELECT 
                i.invoice_no,
                i.cw1_shipment_number,
                i.invoice_creation_date,
                i.transportation_mode,
                i.origin,
                i.destination,
                i.shipper_name,
                i.consignee_name,
                i.total_charges_with_duty_tax_usd,
                i.invoice_currency,
                i.total_charges_with_duty_tax as total_original_currency,
                -- Audit results
                ar.audit_status,
                ar.total_expected_amount,
                ar.total_variance,
                ar.variance_percent,
                ar.rate_cards_checked,
                ar.matching_lanes,
                ar.best_match_rate_card,
                ar.audit_details,
                ar.created_at as audit_date
            FROM dhl_ytd_invoices i
            LEFT JOIN ytd_audit_results ar ON i.invoice_no = ar.invoice_no
            {where_clause}
            ORDER BY i.invoice_creation_date DESC, i.cw1_shipment_number, i.invoice_no
        """
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        
        # Convert to list of dictionaries
        invoices = []
        for row in results:
            invoices.append({
                'invoice_no': row[0],
                'cw1_shipment_number': row[1],
                'invoice_creation_date': row[2],
                'transportation_mode': row[3],
                'origin': row[4],
                'destination': row[5],
                'shipper_name': row[6],
                'consignee_name': row[7],
                'total_charges_usd': row[8] or 0,
                'invoice_currency': row[9],
                'total_original_currency': row[10] or 0,
                'audit_status': row[11],
                'total_expected_amount': row[12] or 0,
                'total_variance': row[13] or 0,
                'variance_percent': row[14] or 0,
                'rate_cards_checked': row[15] or 0,
                'matching_lanes': row[16] or 0,
                'best_match_rate_card': row[17],
                'audit_details': row[18],
                'audit_date': row[19]
            })
        
        return invoices
    
    def get_audit_exceptions(self, filters=None):
        """Get invoices with audit exceptions (no rate cards, review required, etc.)"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        # Build WHERE clause for exceptions - using actual database values
        where_conditions = ["ar.audit_status IN ('No Rate Card', 'rejected')"]
        params = []
        
        if filters:
            if filters.get('month'):
                where_conditions.append("strftime('%Y-%m', i.invoice_creation_date) = ?")
                params.append(filters['month'])
            
            if filters.get('transportation_mode'):
                where_conditions.append("i.transportation_mode = ?")
                params.append(filters['transportation_mode'])
            
            if filters.get('exception_type'):
                where_conditions = ["ar.audit_status = ?"]
                params = [filters['exception_type']]
                if filters.get('month'):
                    where_conditions.append("strftime('%Y-%m', i.invoice_creation_date) = ?")
                    params.append(filters['month'])
        
        where_clause = "WHERE " + " AND ".join(where_conditions)
        
        query = f"""
            SELECT 
                i.invoice_no,
                i.cw1_shipment_number,
                i.invoice_creation_date,
                i.transportation_mode,
                i.origin,
                i.destination,
                i.shipper_name,
                i.consignee_name,
                i.total_charges_with_duty_tax_usd,
                ar.audit_status,
                ar.audit_details,
                ar.rate_cards_checked,
                ar.matching_lanes,
                ar.created_at as audit_date
            FROM dhl_ytd_invoices i
            INNER JOIN ytd_audit_results ar ON i.invoice_no = ar.invoice_no
            {where_clause}
            ORDER BY ar.created_at DESC
        """
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        conn.close()
        
        # Convert to list of dictionaries
        exceptions = []
        for row in results:
            exceptions.append({
                'invoice_no': row[0],
                'cw1_shipment_number': row[1],
                'invoice_creation_date': row[2],
                'transportation_mode': row[3],
                'origin': row[4],
                'destination': row[5],
                'shipper_name': row[6],
                'consignee_name': row[7],
                'total_charges_usd': row[8] or 0,
                'audit_status': row[9],
                'audit_details': row[10],
                'rate_cards_checked': row[11] or 0,
                'matching_lanes': row[12] or 0,
                'audit_date': row[13]
            })
        
        return exceptions
    
    def export_data_to_excel(self, data_type, filters=None):
        """Export data to Excel with multiple sheets"""
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            if data_type == 'shipment_summary':
                # Shipment Summary
                shipments = self.get_shipment_summary(filters)
                df_shipments = pd.DataFrame(shipments)
                if not df_shipments.empty:
                    df_shipments.to_excel(writer, sheet_name='Shipment Summary', index=False)
                
                # Detailed invoices for each shipment
                invoices = self.get_detailed_invoice_data(filters)
                df_invoices = pd.DataFrame(invoices)
                if not df_invoices.empty:
                    df_invoices.to_excel(writer, sheet_name='Invoice Details', index=False)
                
                # Exceptions
                exceptions = self.get_audit_exceptions(filters)
                df_exceptions = pd.DataFrame(exceptions)
                if not df_exceptions.empty:
                    df_exceptions.to_excel(writer, sheet_name='Audit Exceptions', index=False)
            
            elif data_type == 'invoice_details':
                invoices = self.get_detailed_invoice_data(filters)
                df_invoices = pd.DataFrame(invoices)
                if not df_invoices.empty:
                    df_invoices.to_excel(writer, sheet_name='Invoice Details', index=False)
            
            elif data_type == 'exceptions':
                exceptions = self.get_audit_exceptions(filters)
                df_exceptions = pd.DataFrame(exceptions)
                if not df_exceptions.empty:
                    df_exceptions.to_excel(writer, sheet_name='Audit Exceptions', index=False)
        
        output.seek(0)
        return output
    
    def get_reports_summary(self):
        """Get summary statistics for reports dashboard"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        # Total shipments and invoices
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT cw1_shipment_number) as total_shipments,
                COUNT(*) as total_invoices,
                SUM(total_charges_with_duty_tax_usd) as total_value_usd
            FROM dhl_ytd_invoices
            WHERE cw1_shipment_number IS NOT NULL AND cw1_shipment_number != ''
        """)
        totals = cursor.fetchone()
        
        # Audit status breakdown
        cursor.execute("""
            SELECT 
                ar.audit_status,
                COUNT(*) as count,
                SUM(i.total_charges_with_duty_tax_usd) as total_value
            FROM ytd_audit_results ar
            JOIN dhl_ytd_invoices i ON ar.invoice_no = i.invoice_no
            GROUP BY ar.audit_status
            ORDER BY count DESC
        """)
        audit_status = cursor.fetchall()
        
        # Transportation mode breakdown
        cursor.execute("""
            SELECT 
                i.transportation_mode,
                COUNT(DISTINCT i.cw1_shipment_number) as shipment_count,
                COUNT(i.invoice_no) as invoice_count,
                SUM(i.total_charges_with_duty_tax_usd) as total_value
            FROM dhl_ytd_invoices i
            WHERE i.cw1_shipment_number IS NOT NULL AND i.cw1_shipment_number != ''
            GROUP BY i.transportation_mode
            ORDER BY shipment_count DESC
        """)
        transport_modes = cursor.fetchall()
        
        conn.close()
        
        return {
            'totals': {
                'total_shipments': totals[0] or 0,
                'total_invoices': totals[1] or 0,
                'total_value_usd': totals[2] or 0
            },
            'audit_status': [
                {
                    'status': row[0],
                    'count': row[1],
                    'total_value': row[2] or 0
                }
                for row in audit_status
            ],
            'transport_modes': [
                {
                    'mode': row[0],
                    'shipment_count': row[1],
                    'invoice_count': row[2],
                    'total_value': row[3] or 0
                }
                for row in transport_modes
            ]
        }

# Initialize manager
reports_manager = YTDReportsManager()

@ytd_reports_bp.route('/ytd-reports')
@require_auth
def reports_dashboard(user_data=None):
    """Main reports dashboard"""
    summary = reports_manager.get_reports_summary()
    available_months = reports_manager.get_available_months()
    
    return render_template('ytd_reports_dashboard.html',
                         summary=summary,
                         available_months=available_months)

@ytd_reports_bp.route('/ytd-reports/shipments')
@require_auth
def shipment_reports(user_data=None):
    """Shipment-based reports"""
    # Get filters from request
    filters = {}
    if request.args.get('month'):
        filters['month'] = request.args.get('month')
    if request.args.get('transportation_mode'):
        filters['transportation_mode'] = request.args.get('transportation_mode')
    if request.args.get('audit_status'):
        filters['audit_status'] = request.args.get('audit_status')
    
    shipments = reports_manager.get_shipment_summary(filters)
    available_months = reports_manager.get_available_months()
    
    # Get unique transportation modes
    conn = reports_manager.get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT transportation_mode FROM dhl_ytd_invoices WHERE transportation_mode IS NOT NULL ORDER BY transportation_mode")
    transport_modes = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    return render_template('ytd_shipment_reports.html',
                         shipments=shipments,
                         filters=filters,
                         available_months=available_months,
                         transport_modes=transport_modes)

@ytd_reports_bp.route('/ytd-reports/invoices')
@require_auth
def invoice_reports(user_data=None):
    """Invoice-based reports"""
    # Get filters from request
    filters = {}
    if request.args.get('month'):
        filters['month'] = request.args.get('month')
    if request.args.get('transportation_mode'):
        filters['transportation_mode'] = request.args.get('transportation_mode')
    if request.args.get('audit_status'):
        filters['audit_status'] = request.args.get('audit_status')
    if request.args.get('cw1_shipment_number'):
        filters['cw1_shipment_number'] = request.args.get('cw1_shipment_number')
    
    invoices = reports_manager.get_detailed_invoice_data(filters)
    available_months = reports_manager.get_available_months()
    
    # Get unique values for filters
    conn = reports_manager.get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT transportation_mode FROM dhl_ytd_invoices WHERE transportation_mode IS NOT NULL ORDER BY transportation_mode")
    transport_modes = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT DISTINCT audit_status FROM ytd_audit_results WHERE audit_status IS NOT NULL ORDER BY audit_status")
    audit_statuses = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    return render_template('ytd_invoice_reports.html',
                         invoices=invoices,
                         filters=filters,
                         available_months=available_months,
                         transport_modes=transport_modes,
                         audit_statuses=audit_statuses)

@ytd_reports_bp.route('/ytd-reports/exceptions')
@require_auth
def exception_reports(user_data=None):
    """Audit exception reports"""
    # Get filters from request
    filters = {}
    if request.args.get('month'):
        filters['month'] = request.args.get('month')
    if request.args.get('transportation_mode'):
        filters['transportation_mode'] = request.args.get('transportation_mode')
    if request.args.get('exception_type'):
        filters['exception_type'] = request.args.get('exception_type')
    
    exceptions = reports_manager.get_audit_exceptions(filters)
    available_months = reports_manager.get_available_months()
    
    # Get unique transportation modes
    conn = reports_manager.get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT transportation_mode FROM dhl_ytd_invoices WHERE transportation_mode IS NOT NULL ORDER BY transportation_mode")
    transport_modes = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    exception_types = ['No Rate Card', 'rejected']
    
    return render_template('ytd_exception_reports.html',
                         exceptions=exceptions,
                         filters=filters,
                         available_months=available_months,
                         transport_modes=transport_modes,
                         exception_types=exception_types)

@ytd_reports_bp.route('/ytd-reports/export')
@require_auth
def export_reports(user_data=None):
    """Export reports to Excel"""
    data_type = request.args.get('type', 'shipment_summary')
    
    # Get filters from request
    filters = {}
    if request.args.get('month'):
        filters['month'] = request.args.get('month')
    if request.args.get('transportation_mode'):
        filters['transportation_mode'] = request.args.get('transportation_mode')
    if request.args.get('audit_status'):
        filters['audit_status'] = request.args.get('audit_status')
    if request.args.get('cw1_shipment_number'):
        filters['cw1_shipment_number'] = request.args.get('cw1_shipment_number')
    if request.args.get('exception_type'):
        filters['exception_type'] = request.args.get('exception_type')
    
    try:
        excel_data = reports_manager.export_data_to_excel(data_type, filters)
        
        # Generate filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filter_suffix = ""
        if filters.get('month'):
            filter_suffix += f"_{filters['month']}"
        if filters.get('transportation_mode'):
            filter_suffix += f"_{filters['transportation_mode']}"
        
        filename = f"ytd_{data_type}_report{filter_suffix}_{timestamp}.xlsx"
        
        return send_file(
            excel_data,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    
    except Exception as e:
        flash(f'Error exporting data: {str(e)}', 'error')
        return redirect(url_for('ytd_reports.reports_dashboard'))

@ytd_reports_bp.route('/ytd-reports/api/shipment/<shipment_number>')
@require_auth
def api_shipment_details(shipment_number, user_data=None):
    """API endpoint to get shipment details"""
    filters = {'cw1_shipment_number': shipment_number}
    invoices = reports_manager.get_detailed_invoice_data(filters)
    return jsonify(invoices)

@ytd_reports_bp.route('/ytd-reports/audit-details/<invoice_no>')
@require_auth
def audit_details(invoice_no, user_data=None):
    """Show detailed audit information for a specific invoice"""
    conn = reports_manager.get_db_connection()
    cursor = conn.cursor()
    
    # Get invoice details
    cursor.execute("""
        SELECT i.*, ar.*
        FROM dhl_ytd_invoices i
        LEFT JOIN ytd_audit_results ar ON i.invoice_no = ar.invoice_no
        WHERE i.invoice_no = ?
    """, (invoice_no,))
    
    row = cursor.fetchone()
    if not row:
        flash(f'Invoice {invoice_no} not found', 'error')
        return redirect(url_for('ytd_reports.invoice_reports'))
    
    # Get column names for both tables
    cursor.execute('PRAGMA table_info(dhl_ytd_invoices)')
    invoice_columns = [col[1] for col in cursor.fetchall()]
    
    cursor.execute('PRAGMA table_info(ytd_audit_results)')
    audit_columns = [col[1] for col in cursor.fetchall()]
    
    # Create combined data dictionary
    all_columns = invoice_columns + audit_columns
    data = dict(zip(all_columns, row))
    
    conn.close()
    
    return render_template('ytd_audit_details.html', 
                         invoice_data=data,
                         invoice_no=invoice_no)
