"""
Advanced PDF Processing Routes for DHL Invoice Audit App
"""

from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
import os
import json
import sqlite3
from werkzeug.utils import secure_filename
from advanced_pdf_processor import AdvancedPDFProcessor

# Import authentication
try:
    from auth_routes import require_auth, require_auth_api
except ImportError:
    # Fallback if auth is not available
    def require_auth(f):
        return f
    def require_auth_api(f):
        return f

# Create blueprint
advanced_pdf_bp = Blueprint('advanced_pdf', __name__)

# Initialize processor
pdf_processor = AdvancedPDFProcessor()

@advanced_pdf_bp.route('/advanced-pdf')
@require_auth
def advanced_pdf_dashboard(user_data=None):
    """Advanced PDF processing dashboard"""
    
    # Get all processed PDFs
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT invoice_no, pdf_filename, service_type, 
               classification_confidence, total_amount, 
               currency, extraction_timestamp, extraction_confidence
        FROM invoice_pdf_details_enhanced 
        ORDER BY extraction_timestamp DESC
        LIMIT 50
    ''')
    
    processed_pdfs = []
    for row in cursor.fetchall():
        processed_pdfs.append({
            'invoice_no': row[0],
            'pdf_filename': row[1],
            'service_type': row[2],
            'classification_confidence': row[3],
            'total_amount': row[4],
            'currency': row[5],
            'extraction_timestamp': row[6],
            'extraction_confidence': row[7]
        })
    
    # Get unknown charges needing review
    cursor.execute('''
        SELECT id, invoice_no, charge_description, extracted_amount, 
               suggested_type, confidence, created_timestamp
        FROM unknown_charges 
        WHERE status = 'PENDING'
        ORDER BY confidence ASC, extracted_amount DESC
        LIMIT 20
    ''')
    
    unknown_charges = []
    for row in cursor.fetchall():
        unknown_charges.append({
            'id': row[0],
            'invoice_no': row[1],
            'charge_description': row[2],
            'extracted_amount': row[3],
            'suggested_type': row[4],
            'confidence': row[5],
            'created_timestamp': row[6]
        })
    
    conn.close()
    
    return render_template('advanced_pdf_dashboard.html', 
                         processed_pdfs=processed_pdfs,
                         unknown_charges=unknown_charges)

@advanced_pdf_bp.route('/advanced-pdf/upload', methods=['GET', 'POST'])
@require_auth
def upload_pdf(user_data=None):
    """Upload and process PDF"""
    
    if request.method == 'POST':
        if 'pdf_file' not in request.files:
            flash('No file selected')
            return redirect(request.url)
        
        file = request.files['pdf_file']
        invoice_no = request.form.get('invoice_no', '').strip()
        
        if file.filename == '':
            flash('No file selected')
            return redirect(request.url)
        
        if file and file.filename.lower().endswith('.pdf'):
            filename = secure_filename(file.filename)
            upload_path = os.path.join('uploads', filename)
            os.makedirs('uploads', exist_ok=True)
            file.save(upload_path)
            
            # Process the PDF
            result = pdf_processor.process_pdf_advanced(upload_path, invoice_no)
            
            if result.get('success'):
                # Save to database
                save_pdf_details(result, upload_path, filename)
                flash(f'Successfully processed PDF for invoice {result["invoice_no"]}')
                return redirect(url_for('advanced_pdf.pdf_details', 
                                      invoice_no=result['invoice_no']))
            else:
                flash(f'Error processing PDF: {result.get("error", "Unknown error")}')
        else:
            flash('Please upload a PDF file')
    
    return render_template('upload_pdf.html')

@advanced_pdf_bp.route('/advanced-pdf/details/<invoice_no>')
@require_auth
def pdf_details(invoice_no, user_data=None):
    """Show detailed PDF extraction results"""
    
    details = pdf_processor.get_pdf_details(invoice_no)
    
    if not details:
        flash('PDF details not found')
        return redirect(url_for('advanced_pdf.advanced_pdf_dashboard'))
    
    # Get charge type definitions for display
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    cursor.execute('''
        SELECT charge_type, category, description, is_auditable, is_passthrough
        FROM charge_type_definitions
    ''')
    
    charge_definitions = {}
    for row in cursor.fetchall():
        charge_definitions[row[0]] = {
            'category': row[1],
            'description': row[2],
            'is_auditable': bool(row[3]),
            'is_passthrough': bool(row[4])
        }
    
    conn.close()
    
    return render_template('pdf_details.html', 
                         details=details,
                         charge_definitions=charge_definitions)

@advanced_pdf_bp.route('/advanced-pdf/process-uploads')
@require_auth
def process_uploads(user_data=None):
    """Process all PDFs in uploads folder"""
    
    results = pdf_processor.process_uploads_folder()
    
    processed_count = 0
    error_count = 0
    
    for result in results:
        if result.get('success'):
            # Save to database
            filename = result.get('filename', 'unknown.pdf')
            upload_path = os.path.join('uploads', filename)
            save_pdf_details(result, upload_path, filename)
            processed_count += 1
        else:
            error_count += 1
    
    flash(f'Processed {processed_count} PDFs, {error_count} errors')
    return redirect(url_for('advanced_pdf.advanced_pdf_dashboard'))

@advanced_pdf_bp.route('/advanced-pdf/classify-charge', methods=['POST'])
@require_auth_api
def classify_charge(user_data=None):
    """Classify an unknown charge (AJAX)"""
    
    data = request.get_json()
    charge_id = data.get('charge_id')
    charge_type = data.get('charge_type')
    
    if not charge_id or not charge_type:
        return jsonify({'error': 'Missing parameters'}), 400
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    try:
        # Get charge details
        cursor.execute('''
            SELECT charge_description, extracted_amount, invoice_no
            FROM unknown_charges WHERE id = ?
        ''', (charge_id,))
        
        row = cursor.fetchone()
        if not row:
            return jsonify({'error': 'Charge not found'}), 404
        
        description, amount, invoice_no = row
        
        # Add to training data
        cursor.execute('''
            INSERT INTO charge_training_data 
            (charge_description, charge_type, confidence, source)
            VALUES (?, ?, 1.0, 'MANUAL')
        ''', (description, charge_type))
        
        # Update status
        cursor.execute('''
            UPDATE unknown_charges 
            SET status = 'CLASSIFIED' 
            WHERE id = ?
        ''', (charge_id,))
        
        conn.commit()
        
        # Retrain ML model
        pdf_processor.train_initial_models()
        
        return jsonify({
            'success': True,
            'message': f'Charge classified as {charge_type}'
        })
        
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

@advanced_pdf_bp.route('/advanced-pdf/audit-comparison/<invoice_no>')
@require_auth
def audit_comparison(invoice_no, user_data=None):
    """Compare PDF charges with audit results"""
    
    # Get PDF details
    pdf_details = pdf_processor.get_pdf_details(invoice_no)
    
    # Get audit results
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT audit_results FROM ytd_audit_results_new 
        WHERE invoice_no = ?
    ''', (invoice_no,))
    
    audit_row = cursor.fetchone()
    audit_results = json.loads(audit_row[0]) if audit_row else {}
    
    conn.close()
    
    # Compare charges
    comparison = compare_pdf_vs_audit(pdf_details, audit_results)
    
    return render_template('audit_comparison.html',
                         invoice_no=invoice_no,
                         pdf_details=pdf_details,
                         audit_results=audit_results,
                         comparison=comparison)

def save_pdf_details(result, pdf_path, filename):
    """Save PDF processing results to database"""
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Save unknown charges for review
    for charge in result.get('unknown_charges', []):
        cursor.execute('''
            INSERT OR IGNORE INTO unknown_charges 
            (invoice_no, charge_description, extracted_amount, 
             suggested_type, confidence)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            result['invoice_no'],
            charge['description'],
            charge['amount'],
            charge['suggested_type'],
            charge['confidence']
        ))
    
    # Save main PDF details
    cursor.execute('''
        INSERT OR REPLACE INTO invoice_pdf_details_enhanced
        (invoice_no, pdf_file_path, pdf_filename, extracted_charges,
         charge_descriptions, shipment_references, service_type,
         classification_confidence, total_amount, currency,
         extraction_timestamp, extraction_confidence, ml_predictions)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), ?, ?)
    ''', (
        result['invoice_no'],
        pdf_path,
        filename,
        json.dumps(result['charges']),
        json.dumps(result['charge_descriptions']),
        json.dumps(result['references']),
        result['service_type'],
        result['classification_confidence'],
        result['total_amount'],
        result['currency'],
        result['confidence'],
        json.dumps(result['ml_predictions'])
    ))
    
    conn.commit()
    conn.close()

def compare_pdf_vs_audit(pdf_details, audit_results):
    """Compare PDF extracted charges with audit results"""
    
    if not pdf_details or not audit_results:
        return {'error': 'Missing data for comparison'}
    
    pdf_charges = pdf_details.get('charges', {})
    audit_charges = audit_results.get('charge_breakdown', {})
    
    comparison = {
        'matches': [],
        'pdf_only': [],
        'audit_only': [],
        'variances': []
    }
    
    # Find matches and variances
    for charge_type, pdf_amount in pdf_charges.items():
        if charge_type in audit_charges:
            audit_amount = audit_charges[charge_type]
            variance = abs(pdf_amount - audit_amount)
            
            if variance < 0.01:  # Match within 1 cent
                comparison['matches'].append({
                    'charge_type': charge_type,
                    'amount': pdf_amount
                })
            else:
                comparison['variances'].append({
                    'charge_type': charge_type,
                    'pdf_amount': pdf_amount,
                    'audit_amount': audit_amount,
                    'variance': variance
                })
        else:
            comparison['pdf_only'].append({
                'charge_type': charge_type,
                'amount': pdf_amount
            })
    
    # Find audit-only charges
    for charge_type, audit_amount in audit_charges.items():
        if charge_type not in pdf_charges:
            comparison['audit_only'].append({
                'charge_type': charge_type,
                'amount': audit_amount
            })
    
    return comparison
