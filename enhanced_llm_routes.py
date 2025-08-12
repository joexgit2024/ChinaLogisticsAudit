#!/usr/bin/env python3
"""
Enhanced LLM Routes with Schema-Driven Processing and Automatic Model Loading
"""

from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
import os
import json
import sqlite3
from schema_driven_llm_processor import SchemaDrivenLLMProcessor
from llm_enhanced_pdf_processor import LLMEnhancedPDFProcessor  # Keep for compatibility
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
enhanced_llm_pdf_bp = Blueprint('enhanced_llm_pdf', __name__)

@enhanced_llm_pdf_bp.route('/enhanced-llm-pdf')
def dashboard():
    """Enhanced LLM PDF Processing Dashboard"""
    try:
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Get recent extractions with more details
        cursor.execute("""
            SELECT e.invoice_no, e.pdf_filename, 
                  s.customer_name, s.final_total, s.currency,
                  e.extraction_confidence, e.processing_timestamp,
                  s.service_type, s.origin, s.destination,
                  COUNT(l.id) as line_items_count
            FROM llm_pdf_extractions e
            LEFT JOIN llm_invoice_summary s ON e.invoice_no = s.invoice_no
            LEFT JOIN llm_billing_line_items l ON e.invoice_no = l.invoice_no
            GROUP BY e.invoice_no
            ORDER BY e.processing_timestamp DESC 
            LIMIT 20
        """)
        recent_extractions = cursor.fetchall()
        
        # Get enhanced statistics
        cursor.execute("SELECT COUNT(*) FROM llm_pdf_extractions")
        total_processed = cursor.fetchone()[0]
        
        cursor.execute("SELECT AVG(extraction_confidence) FROM llm_pdf_extractions WHERE extraction_confidence IS NOT NULL")
        avg_confidence_result = cursor.fetchone()[0]
        avg_confidence = round(avg_confidence_result * 100, 1) if avg_confidence_result else None
        
        # Get category breakdown
        cursor.execute("""
            SELECT category, COUNT(*), SUM(total_amount)
            FROM llm_billing_line_items 
            WHERE category IS NOT NULL
            GROUP BY category
            ORDER BY COUNT(*) DESC
        """)
        category_stats = cursor.fetchall()
        
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
                    'timestamp': row[6],
                    'service_type': row[7],
                    'origin': row[8],
                    'destination': row[9],
                    'line_items_count': row[10]
                })
        
        categories = []
        if category_stats:
            for cat_row in category_stats:
                categories.append({
                    'category': cat_row[0],
                    'count': cat_row[1],
                    'total_amount': cat_row[2]
                })
        
        return render_template('enhanced_llm_pdf_dashboard.html', 
                             recent_extractions=extractions,
                             total_processed=total_processed,
                             avg_confidence=avg_confidence,
                             category_stats=categories)
    except Exception as e:
        print(f"Enhanced dashboard error: {e}")
        return render_template('enhanced_llm_pdf_dashboard.html', 
                             recent_extractions=[],
                             total_processed=0,
                             avg_confidence=None,
                             category_stats=[])

@enhanced_llm_pdf_bp.route('/enhanced-llm-pdf/upload')
def upload_page():
    """Enhanced upload page"""
    return render_template('enhanced_llm_pdf_upload.html')

@enhanced_llm_pdf_bp.route('/enhanced-llm-pdf/process-schema', methods=['POST'])
def process_with_schema():
    """Process PDF using schema-driven extraction"""
    try:
        # Debug: Log all incoming data
        print(f"Schema processing - Request method: {request.method}")
        print(f"Request content type: {request.content_type}")
        print(f"Request files keys: {list(request.files.keys())}")
        
        if 'pdf_file' not in request.files:
            print("ERROR: No pdf_file in request.files")
            return jsonify({'success': False, 'error': 'No PDF file provided'})
        
        pdf_file = request.files['pdf_file']
        print(f"PDF file object: {pdf_file}")
        print(f"PDF filename: '{pdf_file.filename}'")
        
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
        print(f"File saved to: {file_path}")
        
        try:
            # Use schema-driven processor
            processor = SchemaDrivenLLMProcessor()
            
            # Check LLM connection first
            print("Checking Ollama connection...")
            old_processor = LLMEnhancedPDFProcessor()  # For connection check
            if not old_processor.check_ollama_connection():
                if os.path.exists(file_path):
                    os.remove(file_path)
                return jsonify({'success': False, 'error': 'LLM service (Ollama) is not available. Please ensure Ollama is running and the DeepSeek-R1 model is loaded.'})
            
            print("Ollama connection OK, starting schema-driven processing...")
            # Process with schema-driven approach
            result = processor.process_pdf_with_schema(file_path)
            print(f"Schema processing completed with result: {result}")
            
            # Clean up
            if os.path.exists(file_path):
                os.remove(file_path)
            
            return jsonify(result)
            
        except Exception as e:
            # Clean up on error
            if os.path.exists(file_path):
                os.remove(file_path)
            print(f"Exception in schema processing: {str(e)}")
            raise e
            
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@enhanced_llm_pdf_bp.route('/enhanced-llm-pdf/view/<invoice_no>')
def view_enhanced_extraction(invoice_no):
    """View enhanced extraction details with schema breakdown"""
    try:
        print(f"Viewing enhanced extraction for invoice: {invoice_no}")
        
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Get main extraction data
        cursor.execute("""
            SELECT e.invoice_no, e.pdf_filename, e.raw_text, e.extracted_data, 
                   e.extraction_confidence, e.processing_timestamp
            FROM llm_pdf_extractions e
            WHERE e.invoice_no = ?
        """, (invoice_no,))
        
        extraction_row = cursor.fetchone()
        if not extraction_row:
            flash('Extraction not found', 'error')
            return redirect(url_for('enhanced_llm_pdf.dashboard'))
        
        # Get invoice summary
        cursor.execute("""
            SELECT invoice_date, due_date, customer_name, currency, subtotal, 
                   gst_total, final_total, service_type, origin, destination, 
                   weight, shipment_ref
            FROM llm_invoice_summary
            WHERE invoice_no = ?
        """, (invoice_no,))
        
        summary_row = cursor.fetchone()
        
        # Get line items with categories
        cursor.execute("""
            SELECT line_item_index, description, amount, gst_amount, 
                   total_amount, currency, category
            FROM llm_billing_line_items
            WHERE invoice_no = ?
            ORDER BY line_item_index
        """, (invoice_no,))
        
        line_items = cursor.fetchall()
        
        conn.close()
        
        # Format data for template
        extraction_data = {
            'invoice_no': extraction_row[0],
            'pdf_filename': extraction_row[1],
            'raw_text': extraction_row[2],
            'extracted_data': json.loads(extraction_row[3]) if extraction_row[3] else {},
            'confidence': extraction_row[4],
            'timestamp': extraction_row[5],
            'invoice_summary': None,
            'billing_line_items': []
        }
        
        if summary_row:
            extraction_data['invoice_summary'] = {
                'invoice_date': summary_row[0],
                'due_date': summary_row[1],
                'customer_name': summary_row[2],
                'currency': summary_row[3],
                'subtotal': summary_row[4],
                'gst_total': summary_row[5],
                'final_total': summary_row[6],
                'service_type': summary_row[7],
                'origin': summary_row[8],
                'destination': summary_row[9],
                'weight': summary_row[10],
                'shipment_ref': summary_row[11]
            }
        
        for item in line_items:
            extraction_data['billing_line_items'].append({
                'line_item_index': item[0],
                'description': item[1],
                'amount': item[2],
                'gst_amount': item[3],
                'total_amount': item[4],
                'currency': item[5],
                'category': item[6]
            })
        
        return render_template('enhanced_llm_pdf_extraction_detail.html',
                               extraction_details=extraction_data,
                               invoice_no=invoice_no)
        
    except Exception as e:
        print(f"Error viewing enhanced extraction: {e}")
        flash(f'Error: {str(e)}', 'error')
        return redirect(url_for('enhanced_llm_pdf.dashboard'))

@enhanced_llm_pdf_bp.route('/enhanced-llm-pdf/api/schema-info')
def api_schema_info():
    """Get schema information"""
    try:
        processor = SchemaDrivenLLMProcessor()
        schema = processor.get_extraction_schema()
        return jsonify({
            'success': True,
            'schema': schema,
            'total_fields': len(schema['invoice_summary']) + len(schema['billing_line_items'])
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# Comparison route
@enhanced_llm_pdf_bp.route('/enhanced-llm-pdf/compare')
def compare_methods():
    """Compare old vs new extraction methods"""
    return render_template('llm_extraction_comparison.html')
