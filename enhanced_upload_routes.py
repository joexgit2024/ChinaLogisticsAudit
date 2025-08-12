"""
Flask route integration for enhanced file upload processing
"""

from flask import request, jsonify, render_template, redirect, url_for, flash
import sqlite3
import time
import traceback
from app.edi_parser import EDIParser
from app.database import get_db_connection
from enhanced_upload_processor import EnhancedEDIProcessor, ProcessingResult, ProcessingStatus


def enhanced_upload_file():
    """
    Enhanced file upload route with comprehensive validation and processing
    """
    if request.method == 'POST':
        # Initialize processor
        processor = EnhancedEDIProcessor('uploads')
        
        # Check if file was uploaded
        if 'file' not in request.files:
            flash('No file selected', 'error')
            return redirect(request.url)
        
        file_obj = request.files['file']
        if file_obj.filename == '':
            flash('No file selected', 'error')
            return redirect(request.url)
        
        start_time = time.time()
        
        try:
            # PHASE 1: File Validation
            is_valid, validation_errors = processor.validate_file(file_obj)
            if not is_valid:
                for error in validation_errors:
                    flash(error, 'error')
                return redirect(request.url)
            
            # Read file content
            content = file_obj.read().decode('utf-8')
            file_hash = processor.calculate_file_hash(content)
            
            # PHASE 2: Content Validation
            is_valid_content, content_errors, content_warnings = processor.validate_edi_content(content)
            if not is_valid_content:
                for error in content_errors:
                    flash(error, 'error')
                return redirect(request.url)
            
            # Show warnings
            for warning in content_warnings:
                flash(warning, 'warning')
            
            # PHASE 3: Save File
            file_path = processor.save_uploaded_file(file_obj, content)
            
            # PHASE 4: Parse EDI Content
            parser = EDIParser()
            parsed_data = parser.parse_edi_content(content)
            
            if not parsed_data:
                flash('No valid invoice data found in file', 'error')
                return redirect(request.url)
            
            # Convert to the expected format
            parsed_data = {'invoices': parsed_data}
            
            # PHASE 5: Database Processing
            processing_result = process_edi_data_enhanced(
                parsed_data, file_path, file_hash, processor
            )
            
            # PHASE 6: Results and Feedback
            processing_time = time.time() - start_time
            
            if processing_result.status == ProcessingStatus.COMPLETED:
                flash(f'Successfully processed {processing_result.invoices_processed} invoices, '
                      f'{processing_result.charges_processed} charges, '
                      f'{processing_result.line_items_processed} line items '
                      f'in {processing_time:.2f} seconds', 'success')
            elif processing_result.status == ProcessingStatus.PARTIAL:
                flash(f'Partially processed file with {len(processing_result.errors)} errors', 'warning')
            else:
                flash(f'Processing failed: {"; ".join(processing_result.errors)}', 'error')
            
            # Show errors and warnings
            for error in processing_result.errors:
                flash(error, 'error')
            for warning in processing_result.warnings:
                flash(warning, 'warning')
            
            return redirect(url_for('core.edi_invoice_dashboard'))
            
        except Exception as e:
            flash(f'Processing failed: {str(e)}', 'error')
            return redirect(request.url)
    
    return render_template('upload.html')


def process_edi_data_enhanced(parsed_data: dict, file_path: str, file_hash: str, processor: EnhancedEDIProcessor) -> ProcessingResult:
    """
    Enhanced EDI data processing with comprehensive error handling and multi-table insertion
    """
    invoices_processed = 0
    charges_processed = 0
    line_items_processed = 0
    references_processed = 0
    all_errors = []
    all_warnings = []
    
    conn = get_db_connection()
    
    try:
        conn.execute('BEGIN TRANSACTION')
        
        for invoice_data in parsed_data.get('invoices', []):
            try:
                # STEP 1: Process main invoice
                invoice_id, invoice_errors = processor.process_invoice_data(
                    invoice_data, conn, file_path, file_hash
                )
                
                if invoice_id is None:
                    all_errors.extend(invoice_errors)
                    continue
                
                invoices_processed += 1
                
                # STEP 2: Process charges
                if 'charges' in invoice_data:
                    charge_count, charge_errors = processor.process_charges(
                        invoice_id, invoice_data['charges'], conn
                    )
                    charges_processed += charge_count
                    all_errors.extend(charge_errors)
                
                # STEP 3: Process line items
                if 'line_items' in invoice_data:
                    item_count, item_errors = processor.process_line_items(
                        invoice_id, invoice_data['line_items'], conn
                    )
                    line_items_processed += item_count
                    all_errors.extend(item_errors)
                
                # STEP 4: Process reference numbers
                if 'references' in invoice_data:
                    ref_count, ref_errors = processor.process_reference_numbers(
                        invoice_id, invoice_data['references'], conn
                    )
                    references_processed += ref_count
                    all_errors.extend(ref_errors)
                
                # STEP 5: Process shipment data (if present)
                if 'shipment' in invoice_data:
                    process_shipment_data(invoice_id, invoice_data['shipment'], conn)
                
                # STEP 6: Update calculated fields
                update_calculated_fields(invoice_id, conn)
                
                # STEP 7: Run validation and update audit status
                audit_status, validation_errors = processor.validate_and_update_audit_status(invoice_id, conn)
                if validation_errors:
                    # Log validation issues but don't fail processing
                    processor.logger.info(f"Validation issues for invoice {invoice_data.get('invoice_number')}: {len(validation_errors)} issues")
                    all_warnings.extend([f"Validation: {err}" for err in validation_errors[:3]])  # Limit to first 3
                
            except Exception as e:
                error_msg = f"Error processing invoice {invoice_data.get('invoice_number', 'Unknown')}: {str(e)}"
                all_errors.append(error_msg)
                processor.logger.error(error_msg)
                continue
        
        # Commit if we processed any data successfully
        if invoices_processed > 0:
            conn.execute('COMMIT')
            status = ProcessingStatus.COMPLETED if len(all_errors) == 0 else ProcessingStatus.PARTIAL
        else:
            conn.execute('ROLLBACK')
            status = ProcessingStatus.FAILED
            
    except Exception as e:
        conn.execute('ROLLBACK')
        all_errors.append(f"Database transaction failed: {str(e)}")
        status = ProcessingStatus.FAILED
        processor.logger.error(f"Transaction failed: {str(e)}")
        processor.logger.error(traceback.format_exc())
        
    finally:
        conn.close()
    
    return ProcessingResult(
        status=status,
        invoices_processed=invoices_processed,
        charges_processed=charges_processed,
        line_items_processed=line_items_processed,
        errors=all_errors,
        warnings=all_warnings,
        file_path=file_path,
        processing_time=0.0,
        file_hash=file_hash
    )


def process_shipment_data(invoice_id: int, shipment_data: dict, conn):
    """Process shipment data into shipments table"""
    try:
        conn.execute('''
            INSERT INTO shipments (
                invoice_id, tracking_number, origin, destination,
                departure_date, arrival_date, carrier, service_type,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            invoice_id,
            shipment_data.get('tracking_number'),
            shipment_data.get('origin'),
            shipment_data.get('destination'),
            shipment_data.get('departure_date'),
            shipment_data.get('arrival_date'),
            shipment_data.get('carrier'),
            shipment_data.get('service_type'),
            time.strftime('%Y-%m-%d %H:%M:%S')
        ))
    except Exception as e:
        print(f"Error processing shipment data: {e}")


def update_calculated_fields(invoice_id: int, conn):
    """Update calculated fields like totals, cost per unit, etc."""
    try:
        # Update total charges from charges table
        cursor = conn.execute('''
            SELECT COALESCE(SUM(amount), 0) as total_charges
            FROM charges WHERE invoice_id = ?
        ''', (invoice_id,))
        
        total_charges = cursor.fetchone()[0]
        
        # Update invoice with calculated total
        conn.execute('''
            UPDATE invoices 
            SET total_charges = ?, 
                net_charge = ?,
                invoice_amount = ?,
                updated_at = ?
            WHERE id = ?
        ''', (total_charges, total_charges, total_charges, 
              time.strftime('%Y-%m-%d %H:%M:%S'), invoice_id))
        
        # Calculate weight-based metrics
        conn.execute('''
            UPDATE invoices 
            SET 
                cost_per_kg = CASE 
                    WHEN weight > 0 THEN total_charges / weight 
                    ELSE 0 
                END,
                cost_per_piece = CASE 
                    WHEN pieces > 0 THEN total_charges / pieces 
                    ELSE 0 
                END
            WHERE id = ?
        ''', (invoice_id,))
        
    except Exception as e:
        print(f"Error updating calculated fields: {e}")


def get_upload_status_api():
    """API endpoint to get upload processing status"""
    try:
        conn = get_db_connection()
        
        # Get recent uploads summary
        cursor = conn.execute('''
            SELECT 
                COUNT(*) as total_invoices,
                COUNT(DISTINCT uploaded_file_path) as total_files,
                MAX(created_at) as last_upload,
                SUM(CASE WHEN audit_status = 'pending' THEN 1 ELSE 0 END) as pending_audits
            FROM invoices
            WHERE created_at >= datetime('now', '-24 hours')
        ''')
        
        summary = cursor.fetchone()
        
        conn.close()
        
        return jsonify({
            'status': 'success',
            'data': dict(summary),
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500
