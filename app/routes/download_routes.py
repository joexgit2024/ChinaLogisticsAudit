"""
Download Routes for DHL Invoice Audit Application

Overall Purpose:
----------------
This blueprint handles all file download functionality including JSON, CSV,
EDI, and bulk export capabilities for invoices in the audit system.

Where This File is Used:
------------------------
- Registered as a blueprint in the main Flask application (app.py)
- Provides download endpoints for individual and bulk invoice exports
- Used by the web UI for generating and downloading various file formats
"""

import os
import csv
import json
import time
from io import StringIO, BytesIO
from flask import (
    Blueprint, redirect, url_for, flash, send_file, current_app
)
from app.database import get_db_connection

# Create blueprint
download_bp = Blueprint('download', __name__)


@download_bp.route('/download/invoice/<int:invoice_id>')
def download_invoice_json(invoice_id):
    """Download invoice data as JSON file."""
    try:
        conn = get_db_connection()
        
        # Get invoice details
        invoice = conn.execute(
            'SELECT * FROM invoices WHERE id = ?', (invoice_id,)
        ).fetchone()
        if not invoice:
            flash('Invoice not found', 'error')
            return redirect(url_for('invoice.invoices'))
        
        # Convert invoice to dict
        invoice_dict = dict(invoice)
        
        # Get related data
        charges = conn.execute(
            'SELECT * FROM charges WHERE invoice_id = ?', (invoice_id,)
        ).fetchall()
        line_items = conn.execute(
            'SELECT * FROM line_items WHERE invoice_id = ?', (invoice_id,)
        ).fetchall()
        reference_numbers = conn.execute(
            'SELECT * FROM reference_numbers WHERE invoice_id = ?',
            (invoice_id,)
        ).fetchall()
        shipments = conn.execute(
            'SELECT * FROM shipments WHERE invoice_id = ?', (invoice_id,)
        ).fetchall()
        
        # Build comprehensive invoice data
        invoice_data = {
            'invoice': invoice_dict,
            'charges': [dict(charge) for charge in charges],
            'line_items': [dict(item) for item in line_items],
            'reference_numbers': [dict(ref) for ref in reference_numbers],
            'shipments': [dict(shipment) for shipment in shipments],
            'export_timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'export_source': 'DHL Audit System'
        }
        
        conn.close()
        
        # Create JSON response
        json_data = json.dumps(invoice_data, indent=2, default=str)
        
        # Create file response
        output = BytesIO()
        output.write(json_data.encode('utf-8'))
        output.seek(0)
        
        filename = (
            f"invoice_{invoice['invoice_number']}_"
            f"{time.strftime('%Y%m%d_%H%M%S')}.json"
        )
        
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/json'
        )
        
    except Exception as e:
        flash(f'Error downloading invoice: {str(e)}', 'error')
        return redirect(url_for('invoice.invoices'))


@download_bp.route('/download/invoice/<int:invoice_id>/csv')
def download_invoice_csv(invoice_id):
    """Download invoice data as CSV file."""
    try:
        conn = get_db_connection()
        
        # Get invoice details
        invoice = conn.execute(
            'SELECT * FROM invoices WHERE id = ?', (invoice_id,)
        ).fetchone()
        if not invoice:
            flash('Invoice not found', 'error')
            return redirect(url_for('invoice.invoices'))
        
        # Get charges for CSV
        charges = conn.execute('''
            SELECT charge_type, description, amount, rate, quantity, unit
            FROM charges WHERE invoice_id = ?
        ''', (invoice_id,)).fetchall()
        
        conn.close()
        
        # Create CSV content
        output = StringIO()
        writer = csv.writer(output)
        
        # Write invoice header
        writer.writerow(['INVOICE SUMMARY'])
        writer.writerow(['Invoice Number', invoice['invoice_number']])
        writer.writerow(['Client Code', invoice['client_code'] or 'N/A'])
        writer.writerow(['Account Number', invoice['account_number'] or 'N/A'])
        writer.writerow(['Shipper', invoice['shipper_name'] or 'N/A'])
        writer.writerow(['Consignee', invoice['consignee_name'] or 'N/A'])
        writer.writerow([
            'Total Amount',
            f"${invoice['total_charges'] or 0.0:.2f}"
        ])
        writer.writerow(['Weight', f"{invoice['weight'] or 0.0:.1f} kg"])
        writer.writerow(['Shipping Mode', invoice['shipping_mode'] or 'N/A'])
        writer.writerow(['Ship Date', invoice['ship_date'] or 'N/A'])
        writer.writerow(['Delivery Date', invoice['delivery_date'] or 'N/A'])
        writer.writerow([])
        
        # Write charges header
        writer.writerow(['CHARGES BREAKDOWN'])
        writer.writerow([
            'Charge Type', 'Description', 'Amount', 'Rate', 'Quantity', 'Unit'
        ])
        
        # Write charges data
        for charge in charges:
            writer.writerow([
                charge['charge_type'] or '',
                charge['description'] or '',
                f"${charge['amount'] or 0.0:.2f}",
                f"${charge['rate'] or 0.0:.4f}",
                f"{charge['quantity'] or 0.0:.2f}",
                charge['unit'] or ''
            ])
        
        # Create file response
        csv_content = output.getvalue()
        output_bytes = BytesIO()
        output_bytes.write(csv_content.encode('utf-8'))
        output_bytes.seek(0)
        
        filename = (
            f"invoice_{invoice['invoice_number']}_"
            f"{time.strftime('%Y%m%d_%H%M%S')}.csv"
        )
        
        return send_file(
            output_bytes,
            as_attachment=True,
            download_name=filename,
            mimetype='text/csv'
        )
        
    except Exception as e:
        flash(f'Error downloading invoice CSV: {str(e)}', 'error')
        return redirect(url_for('invoice.invoices'))


@download_bp.route('/download/invoice/<int:invoice_id>/edi')
def download_invoice_edi(invoice_id):
    """Download original EDI file for the invoice."""
    try:
        conn = get_db_connection()
        
        # Get invoice details including raw EDI and file path
        invoice = conn.execute(
            'SELECT invoice_number, raw_edi, uploaded_file_path '
            'FROM invoices WHERE id = ?', 
            (invoice_id,)
        ).fetchone()
        if not invoice:
            flash('Invoice not found', 'error')
            return redirect(url_for('invoice.invoices'))
        
        raw_edi = invoice['raw_edi']
        file_path = invoice['uploaded_file_path']
        
        conn.close()
        
        # First try to use raw_edi from database
        if raw_edi and raw_edi.strip():
            output = BytesIO()
            output.write(raw_edi.encode('utf-8'))
            output.seek(0)
        # If no raw_edi, try to read from original file
        elif file_path:
            try:
                # Look for file in uploads directory
                full_file_path = os.path.join(
                    current_app.config['UPLOAD_FOLDER'], file_path
                )
                if os.path.exists(full_file_path):
                    with open(full_file_path, 'r', encoding='utf-8') as f:
                        file_content = f.read()
                    output = BytesIO()
                    output.write(file_content.encode('utf-8'))
                    output.seek(0)
                else:
                    flash('Original file not found on server', 'error')
                    return redirect(url_for('invoice.invoices'))
            except Exception as e:
                flash(f'Error reading original file: {str(e)}', 'error')
                return redirect(url_for('invoice.invoices'))
        else:
            flash(
                'Original EDI data not available for this invoice', 'error'
            )
            return redirect(url_for('invoice.invoices'))
        
        filename = (
            f"invoice_{invoice['invoice_number']}_original_"
            f"{time.strftime('%Y%m%d_%H%M%S')}.edi"
        )
        
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='text/plain'
        )
        
    except Exception as e:
        flash(f'Error downloading EDI file: {str(e)}', 'error')
        return redirect(url_for('invoice.invoices'))


@download_bp.route('/download/invoices/bulk')
def download_bulk_invoices():
    """Download all invoices as CSV export."""
    try:
        conn = get_db_connection()
        
        # Get all invoices with key information
        invoices = conn.execute('''
            SELECT invoice_number, client_code, account_number, carrier_code,
                   shipper_name, consignee_name, total_charges, weight,
                   shipping_mode, ship_date, delivery_date, audit_status,
                   created_at
            FROM invoices
            ORDER BY created_at DESC
        ''').fetchall()
        
        conn.close()
        
        # Create CSV content
        output = StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow([
            'Invoice Number', 'Client Code', 'Account Number', 'Carrier Code',
            'Shipper', 'Consignee', 'Total Amount', 'Weight (kg)',
            'Shipping Mode', 'Ship Date', 'Delivery Date', 'Audit Status',
            'Created Date'
        ])
        
        # Write invoice data
        for invoice in invoices:
            writer.writerow([
                invoice['invoice_number'] or '',
                invoice['client_code'] or '',
                invoice['account_number'] or '',
                invoice['carrier_code'] or '',
                invoice['shipper_name'] or '',
                invoice['consignee_name'] or '',
                f"${invoice['total_charges'] or 0.0:.2f}",
                f"{invoice['weight'] or 0.0:.1f}",
                invoice['shipping_mode'] or '',
                invoice['ship_date'] or '',
                invoice['delivery_date'] or '',
                invoice['audit_status'] or '',
                invoice['created_at'] or ''
            ])
        
        # Create file response
        csv_content = output.getvalue()
        output_bytes = BytesIO()
        output_bytes.write(csv_content.encode('utf-8'))
        output_bytes.seek(0)
        
        filename = f"all_invoices_{time.strftime('%Y%m%d_%H%M%S')}.csv"
        
        return send_file(
            output_bytes,
            as_attachment=True,
            download_name=filename,
            mimetype='text/csv'
        )
        
    except Exception as e:
        flash(f'Error downloading bulk export: {str(e)}', 'error')
        return redirect(url_for('invoice.invoices'))
