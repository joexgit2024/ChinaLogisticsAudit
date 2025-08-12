#!/usr/bin/env python3
"""
YTD Batch Audit Routes
Flask routes for the comprehensive YTD batch audit system
"""

from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
from datetime import datetime, timedelta
import json
import sys
import sqlite3
import os

# Add the current directory to sys.path to import our batch audit system
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from ytd_batch_audit_system import YTDBatchAuditSystem

# Create blueprint
ytd_batch_audit_bp = Blueprint('ytd_batch_audit', __name__)

# Initialize the batch audit system
batch_audit_system = YTDBatchAuditSystem()

@ytd_batch_audit_bp.route('/ytd-batch-audit')
def ytd_batch_audit_dashboard():
    """Main dashboard for YTD batch audit system"""
    try:
        # Get current audit summary
        summary = batch_audit_system.get_audit_summary()
        
        # Get recent batch runs
        recent_batches = summary.get('recent_batches', [])
        
        # Get status breakdown
        status_counts = summary.get('status_counts', {})
        
        return render_template('ytd_batch_audit.html',
                             summary=summary,
                             recent_batches=recent_batches,
                             status_counts=status_counts,
                             latest_batch_stats=summary.get('latest_batch_stats', {}),
                             total_results=summary.get('total_results', 0),
                             total_batch_runs=summary.get('total_batch_runs', 0))
    
    except Exception as e:
        print(f"Error in ytd_batch_audit_dashboard: {str(e)}")
        import traceback
        print(traceback.format_exc())
        flash(f'Error loading batch audit dashboard: {str(e)}', 'error')
        return render_template('ytd_batch_audit.html',
                             summary={},
                             recent_batches=[],
                             status_counts={},
                             latest_batch_stats={},
                             total_results=0,
                             total_batch_runs=0)

@ytd_batch_audit_bp.route('/ytd-batch-audit/run', methods=['POST'])
def run_full_ytd_batch_audit():
    """Run a comprehensive batch audit on all YTD invoices"""
    print("ROUTE CALLED - Form submitted successfully!")
    try:
        # Get parameters from form
        batch_name = request.form.get('batch_name', f'YTD Batch Audit - {datetime.now().strftime("%Y-%m-%d %H:%M")}')
        force_reaudit = request.form.get('force_reaudit') == 'on'
        detailed_analysis = request.form.get('detailed_analysis') == 'on'
        
        # Run the comprehensive batch audit
        results = batch_audit_system.run_full_ytd_audit(
            force_reaudit=force_reaudit,
            batch_name=batch_name,
            detailed_analysis=detailed_analysis
        )
        
        flash(f'YTD Batch audit completed successfully! Processed {results.get("total_invoices", 0)} invoices.', 'success')
        return redirect(url_for('ytd_batch_audit.ytd_batch_audit_results', batch_id=results.get('batch_run_id')))
        
    except Exception as e:
        flash(f'Error running YTD batch audit: {str(e)}', 'error')
        return redirect(url_for('ytd_batch_audit.ytd_batch_audit_dashboard'))

@ytd_batch_audit_bp.route('/ytd-batch-audit/results/<int:batch_id>')
def ytd_batch_audit_results(batch_id):
    """View results for a specific batch audit run"""
    try:
        # Get pagination parameters with better error handling
        try:
            page = int(request.args.get('page', '1'))
        except (ValueError, TypeError):
            page = 1
            
        try:
            per_page = int(request.args.get('per_page', '50'))
        except (ValueError, TypeError):
            per_page = 50
            
        status_filter = request.args.get('status', '')
        invoice_filter = request.args.get('invoices', '').strip()
        
        # Get batch run details
        conn = batch_audit_system.get_db_connection()
        cursor = conn.cursor()
        
        # Get batch run info
        cursor.execute("""
            SELECT id, run_name, status, total_invoices, invoices_passed, invoices_warned, 
                   invoices_failed, invoices_error, processing_time_ms, created_at, end_time
            FROM ytd_batch_audit_runs 
            WHERE id = ?
        """, (batch_id,))
        
        batch_info = cursor.fetchone()
        if not batch_info:
            flash('Batch audit run not found', 'error')
            return redirect(url_for('ytd_batch_audit.ytd_batch_audit_dashboard'))
        
        # Convert to dict
        batch_info = {
            'id': batch_info[0],
            'run_name': batch_info[1],
            'status': batch_info[2],
            'total_invoices': batch_info[3],
            'invoices_passed': batch_info[4],
            'invoices_warned': batch_info[5],
            'invoices_failed': batch_info[6],
            'invoices_error': batch_info[7],
            'processing_time_ms': batch_info[8],
            'created_at': batch_info[9],
            'end_time': batch_info[10]
        }
        
        # First count total results (for pagination)
        query_params = [batch_id]
        count_sql = "SELECT COUNT(*) FROM ytd_audit_results WHERE batch_run_id = ?"
        
        # Add status filter if provided
        if status_filter and status_filter != 'all':
            count_sql += " AND audit_status = ?"
            query_params.append(status_filter)
            
        # Add invoice number filter if provided
        if invoice_filter:
            # Split by comma and clean up invoice numbers
            invoice_numbers = [inv.strip().upper() for inv in invoice_filter.split(',') if inv.strip()]
            if invoice_numbers:
                placeholders = ','.join(['?' for _ in invoice_numbers])
                count_sql += f" AND UPPER(invoice_no) IN ({placeholders})"
                query_params.extend(invoice_numbers)
            
        cursor.execute(count_sql, query_params)
        total_results = cursor.fetchone()[0]
        
        # Calculate offset for pagination
        offset = (page - 1) * per_page
        print(f"DEBUG: batch_id={batch_id}, page={page}, per_page={per_page}, offset={offset}")
        print(f"DEBUG: status_filter='{status_filter}', total_results={total_results}")
        
        # Get audit results for this batch with pagination
        sql = """
            SELECT invoice_no, audit_status, transportation_mode, total_invoice_amount,
                   total_expected_amount, total_variance, variance_percent,
                   rate_cards_checked, matching_lanes, processing_time_ms, created_at
            FROM ytd_audit_results 
            WHERE batch_run_id = ?
        """
        
        # Add status filter if provided
        query_params = [batch_id]
        if status_filter and status_filter != 'all':
            sql += " AND audit_status = ?"
            query_params.append(status_filter)
            
        # Add invoice number filter if provided
        if invoice_filter:
            # Split by comma and clean up invoice numbers
            invoice_numbers = [inv.strip().upper() for inv in invoice_filter.split(',') if inv.strip()]
            if invoice_numbers:
                placeholders = ','.join(['?' for _ in invoice_numbers])
                sql += f" AND UPPER(invoice_no) IN ({placeholders})"
                query_params.extend(invoice_numbers)
            
        # Add sorting and pagination
        sql += """
            ORDER BY 
                CASE audit_status 
                    WHEN 'error' THEN 1
                    WHEN 'fail' THEN 2  
                    WHEN 'warning' THEN 3
                    WHEN 'pass' THEN 4
                    ELSE 5
                END,
                ABS(variance_percent) DESC
            LIMIT ? OFFSET ?
        """
        query_params.extend([per_page, offset])
        
        cursor.execute(sql, query_params)
        
        results = []
        raw_results = cursor.fetchall()
        print(f"DEBUG: Raw results count: {len(raw_results)}")
        
        for row in raw_results:
            results.append({
                'invoice_no': row[0],
                'status': row[1],  # Maps 'audit_status' from DB to 'status' for template
                'transportation_mode': row[2],
                'total_invoice_amount': row[3] or 0,
                'total_expected_amount': row[4] or 0,
                'total_variance': row[5] or 0,
                'variance_percent': row[6] or 0,
                'rate_cards_checked': row[7] or 0,
                'matching_lanes': row[8] or 0,
                'processing_time_ms': row[9] or 0,
                'created_at': row[10]
            })
        
        # Calculate pagination information - fix potential division by zero
        if per_page > 0:
            total_pages = (total_results + per_page - 1) // per_page  # Ceiling division
        else:
            total_pages = 1
        has_prev = page > 1
        has_next = page < total_pages
        
        conn.close()
        
        print(f"DEBUG: Final results count: {len(results)}")
        print(f"DEBUG: Pagination info: total_pages={total_pages}, has_prev={has_prev}, has_next={has_next}")
        
        # Calculate status counts for all results in the batch (not just current page)
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Count rejected invoices
        cursor.execute("SELECT COUNT(*) FROM ytd_audit_results WHERE batch_run_id = ? AND audit_status = 'rejected'", [batch_id])
        rejected_count = cursor.fetchone()[0]
        
        # Count fail/no rate card invoices
        cursor.execute("SELECT COUNT(*) FROM ytd_audit_results WHERE batch_run_id = ? AND audit_status = 'fail'", [batch_id])
        no_rate_card_count = cursor.fetchone()[0]
        
        conn.close()
        
        status_counts = {
            'rejected': rejected_count,
            'no_rate_card': no_rate_card_count
        }
        
        return render_template('ytd_batch_audit_results.html',
                             batch_info=batch_info,
                             results=results,
                             status_counts=status_counts,
                             pagination={
                                 'page': page,
                                 'per_page': per_page,
                                 'total_results': total_results,
                                 'total_pages': total_pages,
                                 'has_prev': has_prev,
                                 'has_next': has_next
                             },
                             status_filter=status_filter,
                             invoice_filter=invoice_filter)
        
    except Exception as e:
        flash(f'Error loading batch audit results: {str(e)}', 'error')
        return redirect(url_for('ytd_batch_audit.ytd_batch_audit_dashboard'))

@ytd_batch_audit_bp.route('/ytd-batch-audit/invoice-detail/<invoice_no>')
def ytd_batch_audit_invoice_detail(invoice_no):
    """View detailed audit results for a specific invoice"""
    try:
        conn = batch_audit_system.get_db_connection()
        cursor = conn.cursor()
        
        # Get the most recent audit result for this invoice
        cursor.execute("""
            SELECT invoice_no, audit_status, transportation_mode, total_invoice_amount,
                   total_expected_amount, total_variance, variance_percent,
                   rate_cards_checked, matching_lanes, best_match_rate_card,
                   audit_details, processing_time_ms, created_at
            FROM ytd_audit_results 
            WHERE invoice_no = ?
            ORDER BY created_at DESC
            LIMIT 1
        """, (invoice_no,))
        
        result = cursor.fetchone()
        if not result:
            flash(f'No audit results found for invoice {invoice_no}', 'error')
            return redirect(url_for('ytd_batch_audit.ytd_batch_audit_dashboard'))
        
        # Convert to dict
        audit_result = {
            'invoice_no': result[0],
            'status': result[1],
            'transportation_mode': result[2],
            'total_invoice_amount': result[3] or 0,
            'total_expected_amount': result[4] or 0,
            'total_variance': result[5] or 0,
            'variance_percent': result[6] or 0,
            'rate_cards_checked': result[7] or 0,
            'matching_lanes': result[8] or 0,
            'best_match_rate_card': result[9] or '',
            'audit_details': result[10] or '{}',
            'processing_time_ms': result[11] or 0,
            'created_at': result[12]
        }
        
        # Parse audit details JSON
        try:
            audit_details = json.loads(audit_result['audit_details'])
        except:
            audit_details = {}
        
        # Check if invoice image exists for this invoice number
        cursor.execute('SELECT COUNT(*) FROM invoice_images WHERE invoice_number = ? AND invoice_type = ?', 
                       (invoice_no, 'DGF'))
        has_invoice_image = cursor.fetchone()[0] > 0
        
        conn.close()
        
        return render_template('ytd_batch_audit_invoice_detail.html',
                             audit_result=audit_result,
                             audit_details=audit_details,
                             has_invoice_image=has_invoice_image)
        
    except Exception as e:
        flash(f'Error loading invoice audit details: {str(e)}', 'error')
        return redirect(url_for('ytd_batch_audit.ytd_batch_audit_dashboard'))

@ytd_batch_audit_bp.route('/ytd-batch-audit/api/status')
def api_batch_audit_status():
    """API endpoint to get current batch audit status"""
    try:
        summary = batch_audit_system.get_audit_summary()
        return jsonify(summary)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@ytd_batch_audit_bp.route('/ytd-batch-audit/api/invoices-to-audit')
def api_invoices_to_audit():
    """API endpoint to get count of invoices pending audit"""
    try:
        invoices = batch_audit_system.get_invoices_to_audit()
        return jsonify({
            'pending_audit_count': len(invoices),
            'sample_invoices': invoices[:10]  # First 10 for preview
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@ytd_batch_audit_bp.route('/ytd-batch-audit/export/<int:batch_id>')
def export_batch_results(batch_id):
    """Export batch audit results to Excel or CSV"""
    try:
        export_format = request.args.get('format', 'excel')
        
        conn = batch_audit_system.get_db_connection()
        cursor = conn.cursor()
        
        # Get batch info
        cursor.execute("""
            SELECT run_name, created_at, total_invoices, invoices_passed, 
                   invoices_warned, invoices_failed, invoices_error
            FROM ytd_batch_audit_runs 
            WHERE id = ?
        """, (batch_id,))
        
        batch_info = cursor.fetchone()
        if not batch_info:
            flash('Batch audit run not found', 'error')
            return redirect(url_for('ytd_batch_audit.ytd_batch_audit_dashboard'))
        
        # Get all results for this batch
        cursor.execute("""
            SELECT invoice_no, audit_status, transportation_mode, total_invoice_amount,
                   total_expected_amount, total_variance, variance_percent,
                   rate_cards_checked, matching_lanes, best_match_rate_card,
                   audit_details, created_at
            FROM ytd_audit_results 
            WHERE batch_run_id = ?
            ORDER BY 
                CASE audit_status 
                    WHEN 'error' THEN 1
                    WHEN 'fail' THEN 2  
                    WHEN 'warning' THEN 3
                    WHEN 'pass' THEN 4
                    ELSE 5
                END,
                ABS(variance_percent) DESC
        """, (batch_id,))
        
        results = cursor.fetchall()
        conn.close()
        
        if export_format == 'excel':
            return export_to_excel(batch_info, results)
        else:
            return export_to_csv(batch_info, results)
            
    except Exception as e:
        flash(f'Error exporting batch results: {str(e)}', 'error')
        return redirect(url_for('ytd_batch_audit.ytd_batch_audit_dashboard'))

def export_to_excel(batch_info, results):
    """Export results to Excel format"""
    from io import BytesIO
    import pandas as pd
    
    # Create DataFrame
    df_data = []
    for result in results:
        audit_details = {}
        try:
            audit_details = json.loads(result[10]) if result[10] else {}
        except:
            pass
            
        df_data.append({
            'Invoice Number': result[0],
            'Status': result[1],
            'Transportation Mode': result[2],
            'Invoice Amount': result[3] or 0,
            'Expected Amount': result[4] or 0,
            'Variance': result[5] or 0,
            'Variance %': result[6] or 0,
            'Rate Cards Checked': result[7] or 0,
            'Matching Lanes': result[8] or 0,
            'Best Match Rate Card': result[9] or '',
            'Audit Date': result[11],
            'Audit Details': str(audit_details)
        })
    
    df = pd.DataFrame(df_data)
    
    # Create Excel file in memory
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Audit Results', index=False)
    
    output.seek(0)
    
    from flask import send_file
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'batch_audit_{batch_info[0].replace(" ", "_")}.xlsx'
    )

def export_to_csv(batch_info, results):
    """Export results to CSV format"""
    import csv
    from io import StringIO
    from flask import Response
    
    output = StringIO()
    writer = csv.writer(output)
    
    # Write header
    writer.writerow([
        'Invoice Number', 'Status', 'Transportation Mode', 'Invoice Amount',
        'Expected Amount', 'Variance', 'Variance %', 'Rate Cards Checked',
        'Matching Lanes', 'Best Match Rate Card', 'Audit Date', 'Reason'
    ])
    
    # Write data
    for result in results:
        audit_details = {}
        try:
            audit_details = json.loads(result[10]) if result[10] else {}
        except:
            pass
            
        reason = ''
        if result[1] == 'pass':
            reason = 'Variance within acceptable limits'
        elif result[1] == 'warning':
            reason = 'Variance exceeds warning threshold'
        elif result[1] == 'fail':
            reason = 'Variance exceeds failure threshold'
        elif result[1] == 'error':
            reason = audit_details.get('error', 'Unknown error')
        
        writer.writerow([
            result[0], result[1], result[2], result[3] or 0,
            result[4] or 0, result[5] or 0, result[6] or 0,
            result[7] or 0, result[8] or 0, result[9] or '',
            result[11], reason
        ])
    
    output.seek(0)
    
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={
            'Content-Disposition': f'attachment; filename=batch_audit_{batch_info[0].replace(" ", "_")}.csv'
        }
    )

@ytd_batch_audit_bp.route('/ytd-batch-audit/delete-batch/<int:batch_id>', methods=['POST'])
def delete_batch_audit(batch_id):
    """Delete a batch audit run and its results"""
    try:
        # Use the modular delete_batch method
        success = batch_audit_system.delete_batch(batch_id)
        
        if success:
            flash(f'Batch audit run {batch_id} deleted successfully', 'success')
        else:
            flash(f'Error deleting batch audit {batch_id}', 'error')
            
    except Exception as e:
        flash(f'Error deleting batch audit: {str(e)}', 'error')
    
    return redirect(url_for('ytd_batch_audit.ytd_batch_audit_dashboard'))
