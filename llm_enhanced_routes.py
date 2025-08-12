#!/usr/bin/env python3
"""
LLM-Enhanced Routes for DHL Invoice Audit App with Automatic Model Loading
"""

from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
import os
import json
import time
import sqlite3
from llm_enhanced_pdf_processor import LLMEnhancedPDFProcessor
from schema_driven_llm_processor import SchemaDrivenLLMProcessor
from model_manager import ModelManager

# Authentication (optional)
try:
    from auth_routes import require_auth, require_auth_api
except ImportError:
    def require_auth(f):
        return f
    def require_auth_api(f):
        return f

# Create blueprint
llm_pdf_bp = Blueprint('llm_pdf', __name__)

@llm_pdf_bp.route('/llm-pdf')
def dashboard():
    """LLM PDF Processing Dashboard"""
    try:
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Get recent extractions
        cursor.execute("""
            SELECT invoice_no, pdf_filename, 
                  (SELECT customer_name FROM llm_invoice_summary WHERE invoice_no = llm_pdf_extractions.invoice_no) as customer_name,
                  (SELECT final_total FROM llm_invoice_summary WHERE invoice_no = llm_pdf_extractions.invoice_no) as final_total,
                  (SELECT currency FROM llm_invoice_summary WHERE invoice_no = llm_pdf_extractions.invoice_no) as currency,
                  extraction_confidence, processing_timestamp
            FROM llm_pdf_extractions 
            ORDER BY processing_timestamp DESC 
            LIMIT 10
        """)
        recent_extractions = cursor.fetchall()
        
        # Get statistics
        cursor.execute("SELECT COUNT(*) FROM llm_pdf_extractions")
        total_processed = cursor.fetchone()[0]
        
        cursor.execute("SELECT AVG(extraction_confidence) FROM llm_pdf_extractions WHERE extraction_confidence IS NOT NULL")
        avg_confidence_result = cursor.fetchone()[0]
        avg_confidence = round(avg_confidence_result * 100, 1) if avg_confidence_result else None
        
        conn.close()
        
        # Format for template
        extractions = []
        if recent_extractions:
            for row in recent_extractions:
                extractions.append({
                    'invoice_no': row[0],
                    'pdf_filename': row[1],
                    'customer_name': row[2],
                    'total_amount': row[3],
                    'currency': row[4],
                    'confidence': row[5],
                    'timestamp': row[6]
                })
        
        return render_template('llm_pdf_dashboard.html', 
                             recent_extractions=extractions,
                             total_processed=total_processed,
                             avg_confidence=avg_confidence)
    except Exception as e:
        print(f"Dashboard error: {e}")
        return render_template('llm_pdf_dashboard.html', 
                             recent_extractions=[],
                             total_processed=0,
                             avg_confidence=None)

@llm_pdf_bp.route('/llm-pdf/upload', methods=['GET', 'POST'])
def upload_page():
    """Upload page and processing"""
    if request.method == 'POST':
        try:
            if 'pdf_file' not in request.files:
                flash('No PDF file provided', 'error')
                return redirect(request.url)
            
            pdf_file = request.files['pdf_file']
            if pdf_file.filename == '':
                flash('No file selected', 'error')
                return redirect(request.url)
            
            # Save temporarily
            upload_dir = 'temp_uploads'
            os.makedirs(upload_dir, exist_ok=True)
            
            file_path = os.path.join(upload_dir, pdf_file.filename)
            pdf_file.save(file_path)
            
            # Process with LLM
            processor = LLMEnhancedPDFProcessor()
            result = processor.process_pdf_with_llm(file_path)
            
            # Clean up
            if os.path.exists(file_path):
                os.remove(file_path)
            
            if result.get('success', False):
                flash(f'PDF processed successfully! Invoice: {result.get("invoice_no", "Unknown")}', 'success')
                return redirect(url_for('llm_pdf.dashboard'))
            else:
                flash(f'Processing failed: {result.get("error", "Unknown error")}', 'error')
                return redirect(request.url)
                
        except Exception as e:
            flash(f'Error: {str(e)}', 'error')
            return redirect(request.url)
    
    return render_template('llm_pdf_upload.html')

@llm_pdf_bp.route('/llm-pdf/process-single', methods=['POST'])
def process_single_pdf():
    """Process single PDF with LLM - AJAX endpoint"""
    try:
        start_time_total = time.time()
        
        # Debug: Log all incoming data
        print(f"Request method: {request.method}")
        print(f"Request content type: {request.content_type}")
        print(f"Request files keys: {list(request.files.keys())}")
        print(f"Request form keys: {list(request.form.keys())}")
        
        # Debug each file in request.files
        for key, file_obj in request.files.items():
            print(f"File key '{key}': filename='{file_obj.filename}', content_length={file_obj.content_length if hasattr(file_obj, 'content_length') else 'unknown'}")
        
        if 'pdf_file' not in request.files:
            print("ERROR: No pdf_file in request.files")
            return jsonify({'success': False, 'error': 'No PDF file provided'})
            
        time_request_received = time.time()
        print(f"[TIMING] Request processing started at {time_request_received:.3f}")
        
        pdf_file = request.files['pdf_file']
        print(f"PDF file object: {pdf_file}")
        print(f"PDF filename: '{pdf_file.filename}'")
        print(f"PDF file size: {pdf_file.content_length if hasattr(pdf_file, 'content_length') else 'unknown'}")
        
        if not pdf_file.filename or pdf_file.filename == '':
            print("ERROR: PDF filename is empty")
            return jsonify({'success': False, 'error': 'No file selected'})
        
        # Check if file has content
        pdf_file.seek(0, 2)  # Seek to end
        file_size = pdf_file.tell()
        pdf_file.seek(0)  # Seek back to beginning
        print(f"PDF file actual size: {file_size} bytes")
        
        if file_size == 0:
            print("ERROR: PDF file is empty")
            return jsonify({'success': False, 'error': 'Empty file provided'})
        
        # Save temporarily - measure file saving time
        time_before_save = time.time()
        
        upload_dir = 'temp_uploads'
        os.makedirs(upload_dir, exist_ok=True)
        
        file_path = os.path.join(upload_dir, pdf_file.filename)
        pdf_file.save(file_path)
        time_after_save = time.time()
        print(f"[TIMING] File saved to: {file_path} - Time taken: {(time_after_save - time_before_save):.3f}s")
        
        try:
            # Record time before processing
            time_before_processing = time.time()
            print(f"[TIMING] Preparation time: {(time_before_processing - time_after_save):.3f}s")
            
            # Always use schema-driven processor for extraction
            print("Using SCHEMA-DRIVEN LLM processor ONLY...")
            processor = SchemaDrivenLLMProcessor()
            # Check LLM connection
            old_processor = LLMEnhancedPDFProcessor()
            connection_result = old_processor.check_ollama_connection()
            time_before_connection_check = time.time()
            time_after_connection_check = time.time()
            print(f"[TIMING] Ollama connection check: {(time_after_connection_check - time_before_connection_check):.3f}s")
            if not connection_result:
                if os.path.exists(file_path):
                    os.remove(file_path)
                return jsonify({
                    'success': False,
                    'error': 'LLM service (Ollama) is not available. Please ensure Ollama is running.'
                })
            print("Ollama connection OK, starting SCHEMA-DRIVEN processing...")
            time_before_pdf_processing = time.time()
            result = processor.process_pdf_with_schema(file_path)
            time_after_pdf_processing = time.time()
            print(f"[TIMING] Schema-driven PDF processing: {(time_after_pdf_processing - time_before_pdf_processing):.3f}s")
            print(f"Schema processing completed with result: {result}")
            
            # Clean up
            time_before_cleanup = time.time()
            if os.path.exists(file_path):
                os.remove(file_path)
            time_after_cleanup = time.time()
            print(f"[TIMING] File cleanup: {(time_after_cleanup - time_before_cleanup):.3f}s")
            
            # Calculate total time
            time_complete = time.time()
            total_time = time_complete - time_request_received
            print(f"[TIMING] Total processing time: {total_time:.3f}s")
            
            # Add timing information to response
            timing_info = {
                'total_processing_time': round(total_time, 3),
                'file_saving_time': round(time_after_save - time_before_save, 3),
                'pdf_processing_time': round(time_after_pdf_processing - time_before_pdf_processing, 3),
                'ollama_connection_check': round(time_after_connection_check - time_before_connection_check, 3),
                'cleanup_time': round(time_after_cleanup - time_before_cleanup, 3)
            }
            
            # Add debug information to response
            if result and 'success' in result:
                print(f"LLM Processing result: {result}")  # Debug logging
                
                # Transform the flat LLM structure into the nested structure expected by the web interface
                extracted_data = result.get('extracted_data', {})
                
                # Debug: Show what we actually have
                print(f"Raw extracted_data keys: {list(extracted_data.keys())}")
                print(f"Raw extracted_data: {extracted_data}")
                
                # Create the nested structure that the JavaScript expects
                transformed_data = {
                    'success': True,
                    'invoice_no': result.get('invoice_no'),
                    'confidence': result.get('confidence'),
                    'manual_review_needed': result.get('manual_review_needed'),
                    'llm_model': result.get('llm_model'),
                    'timing': timing_info,
                    'extracted_data': {
                        'invoice_details': {
                            'invoice_number': extracted_data.get('invoice_no'),
                            'invoice_no': extracted_data.get('invoice_no'),
                            'invoice_date': extracted_data.get('invoice_date'),
                            'due_date': extracted_data.get('due_date'),
                            'customer_name': extracted_data.get('customer_name'),
                            'currency': extracted_data.get('currency'),
                            'service_type': extracted_data.get('service_type'),
                            # Handle shipment_details if it exists, otherwise use None
                            'origin': extracted_data.get('shipment_details', {}).get('origin') if isinstance(extracted_data.get('shipment_details'), dict) else None,
                            'destination': extracted_data.get('shipment_details', {}).get('destination') if isinstance(extracted_data.get('shipment_details'), dict) else None,
                            'weight': extracted_data.get('shipment_details', {}).get('weight') if isinstance(extracted_data.get('shipment_details'), dict) else None,
                            'shipment_ref': extracted_data.get('shipment_details', {}).get('shipment_ref') if isinstance(extracted_data.get('shipment_details'), dict) else None
                        },
                        'billing_breakdown': {
                            'line_items': extracted_data.get('charges', []),
                            'subtotal': extracted_data.get('subtotal'),
                            'gst_amount': extracted_data.get('gst_total'),
                            'gst_total': extracted_data.get('gst_total'),
                            'total_amount': extracted_data.get('final_total'),
                            'final_total': extracted_data.get('final_total')
                        }
                    }
                }
                
                print(f"Transformed data structure: {transformed_data}")  # Debug logging
                
                # Debug: Check the specific fields the frontend is looking for
                invoice_details = transformed_data['extracted_data']['invoice_details']
                billing = transformed_data['extracted_data']['billing_breakdown']
                print(f"Frontend will see:")
                print(f"  Invoice Number: {invoice_details.get('invoice_number')}")
                print(f"  Invoice Date: {invoice_details.get('invoice_date')}")
                print(f"  Customer Name: {invoice_details.get('customer_name')}")
                print(f"  Currency: {invoice_details.get('currency')}")
                print(f"  Total Amount: {billing.get('total_amount')}")
                print(f"  Final Total: {billing.get('final_total')}")
                
                return jsonify(transformed_data)
            else:
                print(f"Invalid result from LLM processor: {result}")  # Debug logging
                return jsonify({
                    'success': False, 
                    'error': 'Invalid response from LLM processor',
                    'timing': timing_info
                })
            
        except Exception as e:
            # Capture timing data for failed process
            time_error = time.time()
            error_timing_info = {
                'total_processing_time': round(time_error - time_request_received, 3),
                'file_saving_time': round(time_after_save - time_before_save, 3),
                'error_time': round(time_error - time_before_processing, 3)
            }
            
            # Add any already captured timing data
            if 'time_after_connection_check' in locals():
                error_timing_info['ollama_connection_check'] = round(time_after_connection_check - time_before_connection_check, 3)
            
            if 'time_before_pdf_processing' in locals() and 'time_after_pdf_processing' in locals():
                error_timing_info['pdf_processing_time'] = round(time_after_pdf_processing - time_before_pdf_processing, 3)
                
            # Clean up on error
            if os.path.exists(file_path):
                os.remove(file_path)
                
            print(f"Exception in process_single_pdf: {str(e)}")  # Debug logging
            print(f"[TIMING] Partial timing data captured: {error_timing_info}")
            
            # Return error with partial timing data instead of re-raising
            return jsonify({
                'success': False,
                'error': f'Processing error: {str(e)}',
                'timing': error_timing_info,
                'partial_processing': True
            })
            
    except Exception as e:
        # Handle outermost exceptions (file upload/save issues)
        return jsonify({
            'success': False, 
            'error': str(e),
            'timing': {'request_handling_time': round(time.time() - time.time(), 3) if 'time_request_received' in locals() else 0}
        })


@llm_pdf_bp.route('/llm-pdf/process-schema', methods=['POST'])
def process_pdf_with_schema():
    """Process PDF using schema-driven extraction - dedicated endpoint"""
    try:
        print("=== SCHEMA-DRIVEN PROCESSING ENDPOINT ===")
        
        if 'pdf_file' not in request.files:
            return jsonify({'success': False, 'error': 'No PDF file provided'})
        
        pdf_file = request.files['pdf_file']
        if not pdf_file.filename or pdf_file.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'})
        
        # Save temporarily
        upload_dir = 'temp_uploads'
        os.makedirs(upload_dir, exist_ok=True)
        
        file_path = os.path.join(upload_dir, pdf_file.filename)
        pdf_file.save(file_path)
        print(f"File saved for schema processing: {file_path}")
        
        try:
            # Use schema-driven processor
            processor = SchemaDrivenLLMProcessor()
            
            # Check LLM connection
            old_processor = LLMEnhancedPDFProcessor()
            if not old_processor.check_ollama_connection():
                if os.path.exists(file_path):
                    os.remove(file_path)
                return jsonify({'success': False, 'error': 'LLM service not available'})
            
            try:
                print("Starting SCHEMA-DRIVEN processing with predefined fields...")
                result = processor.process_pdf_with_schema(file_path)
                print(f"Schema processing result: {result}")
                
                # Ensure result is JSON serializable (handle any None/'null' conversion issues)
                if result and isinstance(result, dict):
                    # Fix any non-serializable values in the result
                    for key in result:
                        if result[key] == 'null':
                            result[key] = None
                
                return jsonify(result)
            finally:
                # Clean up (make sure this happens even if there's an error)
                if os.path.exists(file_path):
                    os.remove(file_path)
            
        except Exception as e:
            if os.path.exists(file_path):
                os.remove(file_path)
            print(f"Schema processing error: {str(e)}")
            return jsonify({'success': False, 'error': str(e)})
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@llm_pdf_bp.route('/llm-pdf/batch')
def batch_page():
    """Batch processing page"""
    return render_template('llm_pdf_batch.html')

@llm_pdf_bp.route('/llm-pdf/test-gpu')
def test_gpu_page():
    """GPU test page"""
    return render_template('llm_pdf_test_gpu.html')

@llm_pdf_bp.route('/llm-pdf/performance')
def performance_analysis_page():
    """Performance analysis page for timing metrics"""
    try:
        # Get performance data from database
        conn = sqlite3.connect('dhl_audit.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get recent processing data with timing information
        cursor.execute("""
            SELECT e.invoice_no, e.pdf_filename, e.extracted_data, e.processing_timestamp,
                   e.extraction_confidence
            FROM llm_pdf_extractions e
            ORDER BY e.processing_timestamp DESC
            LIMIT 10
        """)
        recent_extractions = cursor.fetchall()
        
        print(f"Found {len(recent_extractions)} recent extractions in database")
        
        # Process extraction data to extract timing information
        performance_data = []
        for row in recent_extractions:
            try:
                print(f"Processing row for invoice: {row['invoice_no']}")
                # Extract timing data from extracted_data JSON
                extracted_data = json.loads(row['extracted_data']) if row['extracted_data'] else {}
                print(f"Extracted data keys: {list(extracted_data.keys()) if extracted_data else 'No data'}")
                
                timing_data = extracted_data.get('timing', {})
                print(f"Timing data found: {bool(timing_data)}, keys: {list(timing_data.keys()) if timing_data else 'None'}")
                
                if timing_data:
                    performance_data.append({
                        'invoice_no': row['invoice_no'],
                        'pdf_filename': row['pdf_filename'],
                        'processing_timestamp': row['processing_timestamp'],
                        'confidence': row['extraction_confidence'],
                        'timing': timing_data
                    })
                else:
                    # Even if no timing data, let's see what we have
                    print(f"No timing data for {row['invoice_no']}, extracted_data content: {extracted_data}")
            except Exception as e:
                print(f"Error processing timing data for {row['invoice_no']}: {e}")
        
        print(f"Final performance_data has {len(performance_data)} items")
        conn.close()
        
        return render_template('llm_pdf_performance.html', 
                              performance_data=performance_data)
                              
    except Exception as e:
        print(f"Performance page error: {e}")
        return render_template('llm_pdf_performance.html', 
                              performance_data=[],
                              error=str(e))

@llm_pdf_bp.route('/llm-pdf/view/<invoice_no>')
def view_extraction(invoice_no):
    """View extraction details with comprehensive data from all tables"""
    try:
        print(f"Attempting to view extraction for invoice: {invoice_no}")
        
        # Try schema-driven processor first for comprehensive data
        try:
            from schema_driven_llm_processor import SchemaDrivenLLMProcessor
            schema_processor = SchemaDrivenLLMProcessor()
            extraction = schema_processor.get_llm_extraction(invoice_no)
            if extraction:
                print(f"Found comprehensive extraction using schema processor")
            else:
                print(f"No schema-driven extraction found, trying regular processor")
                # Fallback to regular processor
                processor = LLMEnhancedPDFProcessor()
                extraction = processor.get_llm_extraction(invoice_no)
        except ImportError:
            print("Schema processor not available, using regular processor")
            processor = LLMEnhancedPDFProcessor()
            extraction = processor.get_llm_extraction(invoice_no)
        
        print(f"Extraction result: {extraction}")
        
        if not extraction:
            print(f"No extraction found for invoice: {invoice_no}")
            flash('Extraction not found', 'error')
            return redirect(url_for('llm_pdf.dashboard'))
        
        print(f"Extraction found, rendering template with data: {list(extraction.keys())}")
        return render_template('llm_pdf_extraction_detail.html',
                               extraction_details=extraction,
                               invoice_no=invoice_no)
    except Exception as e:
        flash(f'Error: {str(e)}', 'error')
        return redirect(url_for('llm_pdf.dashboard'))

# API Routes
@llm_pdf_bp.route('/llm-pdf/api/models')
def api_get_models():
    """Get available models"""
    try:
        manager = ModelManager()
        models = manager.list_available_models()
        return jsonify(models)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@llm_pdf_bp.route('/llm-pdf/stats')
def stats():
    """LLM processing statistics page"""
    try:
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Get processing statistics
        cursor.execute("SELECT COUNT(*) FROM llm_pdf_extractions")
        total_processed = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT AVG(extraction_confidence) FROM llm_pdf_extractions WHERE extraction_confidence IS NOT NULL")
        avg_confidence = cursor.fetchone()[0] or 0
        
        cursor.execute("SELECT COUNT(*) FROM llm_invoice_summary")
        total_summaries = cursor.fetchone()[0] or 0
        
        cursor.execute("""
            SELECT DATE(processing_timestamp) as date, COUNT(*) as count 
            FROM llm_pdf_extractions 
            WHERE processing_timestamp IS NOT NULL 
            GROUP BY DATE(processing_timestamp) 
            ORDER BY date DESC 
            LIMIT 30
        """)
        daily_stats = cursor.fetchall()
        
        conn.close()
        
        return render_template('llm_pdf_performance.html',
                             total_processed=total_processed,
                             avg_confidence=avg_confidence,
                             total_summaries=total_summaries,
                             daily_stats=daily_stats)
    except Exception as e:
        print(f"Stats error: {e}")
        return render_template('llm_pdf_performance.html',
                             total_processed=0,
                             avg_confidence=0,
                             total_summaries=0,
                             daily_stats=[])

@llm_pdf_bp.route('/llm-pdf/results')
def results():
    """LLM processing results page"""
    try:
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Get recent processing results
        cursor.execute("""
            SELECT e.invoice_no, e.pdf_filename, e.extraction_confidence, e.processing_timestamp,
                   s.customer_name, s.final_total, s.currency, s.invoice_date
            FROM llm_pdf_extractions e
            LEFT JOIN llm_invoice_summary s ON e.invoice_no = s.invoice_no
            ORDER BY e.processing_timestamp DESC
            LIMIT 100
        """)
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'invoice_no': row[0],
                'pdf_filename': row[1],
                'confidence': row[2],
                'timestamp': row[3],
                'customer_name': row[4],
                'total_amount': row[5],
                'currency': row[6],
                'invoice_date': row[7]
            })
        
        conn.close()
        
        return render_template('llm_pdf_dashboard.html',
                             recent_extractions=results,
                             total_processed=len(results),
                             avg_confidence=sum(r['confidence'] or 0 for r in results) / len(results) if results else 0)
    except Exception as e:
        print(f"Results error: {e}")
        return render_template('llm_pdf_dashboard.html',
                             recent_extractions=[],
                             total_processed=0,
                             avg_confidence=0)

@llm_pdf_bp.route('/llm-pdf/export')
def export_data():
    """Export LLM processing data"""
    try:
        import pandas as pd
        import io
        from flask import send_file
        from datetime import datetime
        
        conn = sqlite3.connect('dhl_audit.db')
        
        # Get extraction data
        extractions_df = pd.read_sql_query("""
            SELECT e.invoice_no, e.pdf_filename, e.extraction_confidence, e.processing_timestamp,
                   s.customer_name, s.final_total, s.currency, s.invoice_date,
                   s.shipper_address, s.consignee_address
            FROM llm_pdf_extractions e
            LEFT JOIN llm_invoice_summary s ON e.invoice_no = s.invoice_no
            ORDER BY e.processing_timestamp DESC
        """, conn)
        
        # Get charge details
        charges_df = pd.read_sql_query("""
            SELECT invoice_no, charge_description, charge_amount, charge_currency, charge_category
            FROM llm_invoice_charges
            ORDER BY invoice_no, charge_amount DESC
        """, conn)
        
        conn.close()
        
        # Create Excel file with multiple sheets
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            extractions_df.to_excel(writer, sheet_name='LLM Extractions', index=False)
            charges_df.to_excel(writer, sheet_name='Charge Details', index=False)
        
        output.seek(0)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'llm_processing_data_{timestamp}.xlsx'
        
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        
    except Exception as e:
        print(f"Export error: {e}")
        flash(f'Error exporting data: {str(e)}', 'error')
        return redirect(url_for('llm_pdf.dashboard'))
@llm_pdf_bp.route('/llm-pdf/api/models')
def api_available_models():
    """Get list of available LLM models"""
    try:
        processor = LLMEnhancedPDFProcessor()
        models = processor.get_available_models()
        return jsonify({'success': True, 'models': models})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@llm_pdf_bp.route('/llm-pdf/api/model-status')
def api_model_status():
    """Check model status"""
    try:
        processor = LLMEnhancedPDFProcessor()
        is_connected = processor.check_ollama_connection()
        return jsonify({'available': is_connected})
    except Exception as e:
        return jsonify({'available': False, 'error': str(e)})

@llm_pdf_bp.route('/llm-pdf/api/gpu-status')
def api_gpu_status():
    """Check GPU status"""
    try:
        processor = LLMEnhancedPDFProcessor()
        is_connected = processor.check_ollama_connection()
        return jsonify({'available': is_connected, 'gpu_enabled': is_connected})
    except Exception as e:
        return jsonify({'available': False, 'error': str(e)})
        
        
@llm_pdf_bp.route('/llm-pdf/api/performance-data')
def api_performance_data():
    """Get performance data from recent processing runs"""
    try:
        # Get performance data from database
        conn = sqlite3.connect('dhl_audit.db')
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get recent processing data with timing information
        cursor.execute("""
            SELECT e.invoice_no, e.pdf_filename, e.extracted_data, e.processing_timestamp,
                   e.extraction_confidence
            FROM llm_pdf_extractions e
            ORDER BY e.processing_timestamp DESC
            LIMIT 20
        """)
        recent_extractions = cursor.fetchall()
        
        # Process extraction data to extract timing information
        performance_data = []
        for row in recent_extractions:
            try:
                # Extract timing data from extracted_data JSON
                extracted_data = json.loads(row['extracted_data']) if row['extracted_data'] else {}
                timing_data = extracted_data.get('timing', {})
                
                if timing_data:
                    performance_data.append({
                        'invoice_no': row['invoice_no'],
                        'pdf_filename': row['pdf_filename'],
                        'processing_timestamp': row['processing_timestamp'],
                        'confidence': row['extraction_confidence'],
                        'timing': timing_data
                    })
            except Exception as e:
                print(f"Error processing timing data: {e}")
                
        conn.close()
        
        # Calculate summary statistics
        if performance_data:
            summary = {
                'avg_total_time': 0,
                'max_total_time': 0,
                'min_total_time': float('inf'),
                'bottlenecks': {}
            }
            
            step_totals = {}
            step_counts = {}
            
            for item in performance_data:
                timing = item['timing']
                total_time = timing.get('total_processing_time', 0)
                
                # Update summary stats
                summary['avg_total_time'] += total_time
                summary['max_total_time'] = max(summary['max_total_time'], total_time)
                if total_time > 0:
                    summary['min_total_time'] = min(summary['min_total_time'], total_time)
                
                # Find bottleneck for this item
                max_step = ''
                max_time = 0
                for step, time in timing.items():
                    if step != 'total_processing_time' and time > max_time:
                        max_step = step
                        max_time = time
                    
                    # Add to step totals
                    if step != 'total_processing_time':
                        step_totals[step] = step_totals.get(step, 0) + time
                        step_counts[step] = step_counts.get(step, 0) + 1
                
                if max_step:
                    summary['bottlenecks'][max_step] = summary['bottlenecks'].get(max_step, 0) + 1
            
            # Calculate averages
            if len(performance_data) > 0:
                summary['avg_total_time'] /= len(performance_data)
            if summary['min_total_time'] == float('inf'):
                summary['min_total_time'] = 0
                
            # Calculate average for each step
            avg_steps = {}
            for step, total in step_totals.items():
                count = step_counts.get(step, 0)
                if count > 0:
                    avg_steps[step] = total / count
            
            summary['avg_steps'] = avg_steps
            
            # Sort bottlenecks
            bottlenecks = sorted(
                [(k, v) for k, v in summary['bottlenecks'].items()],
                key=lambda x: x[1],
                reverse=True
            )
            summary['bottlenecks'] = bottlenecks
        else:
            summary = {}
        
        return jsonify({
            'success': True,
            'performance_data': performance_data,
            'summary': summary
        })
        
    except Exception as e:
        print(f"API performance data error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)})


@llm_pdf_bp.route('/llm-pdf/api/run-benchmark', methods=['POST'])
def api_run_benchmark():
    """Run a benchmark test on PDF processing to generate performance data"""
    try:
        print("\n=== BENCHMARK API CALLED ===")
        print(f"Request method: {request.method}")
        print(f"Request content type: {request.content_type}")
        print(f"Request form keys: {list(request.form.keys())}")
        print(f"Request files keys: {list(request.files.keys())}")
        
        if 'pdf_file' not in request.files:
            print("ERROR: No pdf_file in request.files")
            return jsonify({'success': False, 'error': 'No PDF file provided'})
        
        pdf_file = request.files['pdf_file']
        print(f"PDF filename: '{pdf_file.filename}'")
        print(f"PDF file size: {pdf_file.content_length if hasattr(pdf_file, 'content_length') else 'unknown'}")
        
        if not pdf_file.filename or pdf_file.filename == '':
            print("ERROR: PDF filename is empty")
            return jsonify({'success': False, 'error': 'No file selected'})
        
        # Check if file has content
        pdf_file.seek(0, 2)  # Seek to end
        file_size = pdf_file.tell()
        pdf_file.seek(0)  # Seek back to beginning
        print(f"PDF file actual size: {file_size} bytes")
        
        if file_size == 0:
            print("ERROR: PDF file is empty")
            return jsonify({'success': False, 'error': 'Empty file provided'})
            
        # Save temporarily
        upload_dir = 'temp_uploads'
        os.makedirs(upload_dir, exist_ok=True)
        
        file_path = os.path.join(upload_dir, pdf_file.filename)
        pdf_file.save(file_path)
        print(f"File saved for benchmark: {file_path}, size: {os.path.getsize(file_path)} bytes")
        
        results = []
        iterations = int(request.form.get('iterations', 1))
        use_schema = request.form.get('use_schema_extraction', 'false').lower() == 'true'
        
        # Run multiple iterations for benchmarking
        for i in range(iterations):
            try:
                if use_schema:
                    processor = SchemaDrivenLLMProcessor()
                    result = processor.process_pdf_with_schema(file_path)
                else:
                    processor = LLMEnhancedPDFProcessor()
                    result = processor.process_pdf_with_llm(file_path)
                
                # Append result regardless of success status
                # This ensures we capture timing data even from failed attempts
                if result:
                    if isinstance(result, dict):
                        result['iteration'] = i + 1
                        results.append(result)
            except Exception as e:
                print(f"Benchmark iteration {i+1} failed: {e}")
                # Add error result with any timing data that might be available
                results.append({
                    'success': False,
                    'error': str(e),
                    'iteration': i + 1,
                    'partial_processing': True
                })
        
        # Clean up
        if os.path.exists(file_path):
            os.remove(file_path)
            
        # Calculate average times
        summary = {
            'iterations_completed': len(results),
            'iterations_requested': iterations
        }
        
        if results:
            # Calculate averages for each timing metric
            timing_keys = set()
            for result in results:
                if 'timing' in result:
                    timing_keys.update(result['timing'].keys())
            
            averages = {}
            for key in timing_keys:
                total = sum(result['timing'].get(key, 0) for result in results if 'timing' in result)
                averages[key] = round(total / len(results), 3) if results else 0
            
            summary['average_times'] = averages
        
        return jsonify({
            'success': True,
            'results': results,
            'summary': summary
        })
        
    except Exception as e:
        print(f"Benchmark error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)})


@llm_pdf_bp.route('/llm-pdf/api/process', methods=['POST'])
def api_process_pdf():
    """Process PDF with LLM"""
    try:
        if 'pdf_file' not in request.files:
            return jsonify({'error': 'No PDF file provided'}), 400
        
        pdf_file = request.files['pdf_file']
        if pdf_file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        # Save temporarily
        upload_dir = 'temp_uploads'
        os.makedirs(upload_dir, exist_ok=True)
        
        file_path = os.path.join(upload_dir, pdf_file.filename)
        pdf_file.save(file_path)
        
        # Process with LLM
        processor = LLMEnhancedPDFProcessor()
        result = processor.process_pdf_with_llm(file_path)
        
        # Clean up
        if os.path.exists(file_path):
            os.remove(file_path)
        
        return jsonify(result)
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@llm_pdf_bp.route('/llm-pdf/api/ensure-temp-dir', methods=['GET'])
def api_ensure_temp_dir():
    """Ensure the temp_uploads directory exists"""
    try:
        upload_dir = 'temp_uploads'
        os.makedirs(upload_dir, exist_ok=True)
        print(f"Ensuring temp directory exists: {upload_dir}")
        return jsonify({'success': True, 'message': f'Directory {upload_dir} created or already exists'})
    except Exception as e:
        print(f"Error creating temp directory: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@llm_pdf_bp.route('/llm-pdf/api/test-model', methods=['POST'])
def api_test_model():
    """Test LLM model with custom prompt"""
    try:
        print(f"Request method: {request.method}")
        print(f"Content type: {request.content_type}")
        print(f"Form data keys: {list(request.form.keys())}")
        
        prompt = request.form.get('prompt')
        print(f"Received prompt: {prompt[:30]}..." if prompt else "No prompt received")
        
        if not prompt:
            print("ERROR: No prompt provided in form data")
            return jsonify({'success': False, 'error': 'No prompt provided'}), 400
        
        print(f"Testing model with prompt: {prompt[:50]}...")
        
        # Initialize processor
        processor = LLMEnhancedPDFProcessor()
        
        # Check LLM connection
        connection_status = processor.check_ollama_connection()
        print(f"Ollama connection status: {connection_status}")
        
        if not connection_status:
            print("ERROR: Ollama connection failed. Service not available.")
            return jsonify({'success': False, 'error': 'LLM service not available. Please ensure Ollama is running with DeepSeek-R1 model loaded.'}), 503
        
        # Query LLM directly
        try:
            print("About to call query_ollama method...")
            # Send the prompt to Ollama
            response = processor.query_ollama(prompt)
            print(f"Query successful, received response of {len(response)} characters")
            
            # Return the response
            result = {
                'success': True,
                'response': response,
                'prompt_length': len(prompt),
                'response_length': len(response)
            }
            print("Returning successful response")
            return jsonify(result)
            
        except Exception as e:
            error_message = str(e)
            print(f"Error querying LLM: {error_message}")
            return jsonify({
                'success': False, 
                'error': f'LLM query error: {error_message}',
                'details': 'Please ensure Ollama is running with the DeepSeek-R1 model loaded'
            }), 500
            
    except Exception as e:
        print(f"API test model error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@llm_pdf_bp.route('/llm-pdf/model-comparison')
def model_comparison():
    """Model Performance Comparison Page"""
    return render_template('model_comparison.html')


@llm_pdf_bp.route('/llm-pdf/api/historical-performance')
def api_historical_performance():
    """API endpoint for historical model performance data"""
    try:
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Get historical performance data
        cursor.execute("""
            SELECT id, invoice_no, llm_model_used, extraction_confidence,
                   processing_timestamp, extracted_data
            FROM llm_pdf_extractions
            ORDER BY processing_timestamp DESC
            LIMIT 50
        """)
        
        results = []
        for row in cursor.fetchall():
            extracted_data = {}
            processing_time = None
            
            try:
                if row[5]:  # extracted_data
                    extracted_data = json.loads(row[5])
                    timing_data = extracted_data.get('timing_data', {})
                    if timing_data and 'total_processing_time' in timing_data:
                        time_val = timing_data['total_processing_time']
                        processing_time = f"{time_val:.2f}s"
            except json.JSONDecodeError:
                pass
            
            results.append({
                'id': row[0],
                'invoice_no': row[1],
                'llm_model_used': row[2] or 'Unknown',
                'extraction_confidence': row[3] or 0.0,
                'processing_timestamp': row[4],
                'processing_time': processing_time
            })
        
        conn.close()
        
        return jsonify({
            'success': True,
            'results': results
        })
        
    except Exception as e:
        print(f"Historical performance API error: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@llm_pdf_bp.route('/llm-pdf/results/<invoice_no>')
def view_extraction_details(invoice_no):
    """View detailed extraction results for a specific invoice"""
    try:
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Get the extraction details
        cursor.execute("""
            SELECT id, invoice_no, pdf_filename, extracted_data,
                   extraction_confidence, processing_timestamp,
                   llm_model_used, manual_review_needed
            FROM llm_pdf_extractions
            WHERE invoice_no = ?
            ORDER BY processing_timestamp DESC
            LIMIT 1
        """, (invoice_no,))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            flash(f'No extraction found for invoice {invoice_no}', 'error')
            return redirect(url_for('llm_pdf.model_comparison'))
        
        # Parse extracted data
        extracted_data = {}
        timing_data = {}
        try:
            if result[3]:  # extracted_data
                extracted_data = json.loads(result[3])
                timing_data = extracted_data.get('timing_data', {})
        except json.JSONDecodeError:
            pass
        
        extraction_details = {
            'id': result[0],
            'invoice_no': result[1],
            'pdf_filename': result[2],
            'extracted_data': extracted_data,
            'confidence': result[4] or 0.0,
            'processing_timestamp': result[5],
            'model_used': result[6] or 'Unknown',
            'manual_review_needed': result[7],
            'timing_data': timing_data
        }
        
        return render_template('llm_extraction_details.html',
                               extraction=extraction_details)
        
    except Exception as e:
        print(f"View extraction details error: {str(e)}")
        flash(f'Error loading extraction details: {str(e)}', 'error')
        return redirect(url_for('llm_pdf.model_comparison'))
