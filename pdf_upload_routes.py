from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
import os
from werkzeug.utils import secure_filename
from pdf_invoice_processor import PDFInvoiceProcessor

# Import authentication
try:
    from auth_routes import require_auth, require_auth_api
except ImportError:
    # Fallback if auth is not available
    def require_auth(f):
        return f
    def require_auth_api(f):
        return f

pdf_upload_bp = Blueprint('pdf_upload', __name__, url_prefix='/pdf-upload')

ALLOWED_EXTENSIONS = {'pdf'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@pdf_upload_bp.route('/')
@require_auth
def upload_page(user_data=None):
    """PDF upload page"""
    processor = PDFInvoiceProcessor()
    
    # Get recently processed PDFs
    recent_pdfs = get_recent_pdf_uploads()
    
    return render_template('pdf_upload.html', recent_pdfs=recent_pdfs)

@pdf_upload_bp.route('/upload', methods=['POST'])
@require_auth
def upload_pdf(user_data=None):
    """Handle PDF file upload"""
    if 'pdf_file' not in request.files:
        flash('No file selected', 'error')
        return redirect(request.url)
    
    file = request.files['pdf_file']
    invoice_no = request.form.get('invoice_no', '').strip()
    
    if file.filename == '':
        flash('No file selected', 'error')
        return redirect(request.url)
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        
        # Create uploads directory if it doesn't exist
        upload_folder = 'uploads'
        os.makedirs(upload_folder, exist_ok=True)
        
        # Save file
        file_path = os.path.join(upload_folder, filename)
        file.save(file_path)
        
        # Process PDF
        processor = PDFInvoiceProcessor()
        result = processor.process_pdf(file_path, invoice_no)
        
        if 'error' in result:
            flash(f'Error processing PDF: {result["error"]}', 'error')
        else:
            flash(f'PDF processed successfully! Invoice: {result["invoice_no"]}', 'success')
            return redirect(url_for('pdf_upload.view_pdf_details', invoice_no=result['invoice_no']))
    
    else:
        flash('Invalid file type. Please upload a PDF file.', 'error')
    
    return redirect(url_for('pdf_upload.upload_page'))

@pdf_upload_bp.route('/process-uploads')
@require_auth
def process_uploads(user_data=None):
    """Process all PDFs in uploads folder"""
    processor = PDFInvoiceProcessor()
    results = processor.process_uploads_folder()
    
    success_count = sum(1 for r in results if 'success' in r)
    
    flash(f'Processed {len(results)} PDFs. {success_count} successful.', 'info')
    
    return render_template('pdf_processing_results.html', results=results)

@pdf_upload_bp.route('/details/<invoice_no>')
@require_auth
def view_pdf_details(invoice_no, user_data=None):
    """View PDF extraction details"""
    processor = PDFInvoiceProcessor()
    pdf_details = processor.get_pdf_details(invoice_no)
    
    if not pdf_details:
        flash('No PDF details found for this invoice', 'error')
        return redirect(url_for('pdf_upload.upload_page'))
    
    return render_template('pdf_details.html', pdf_details=pdf_details, invoice_no=invoice_no)

@pdf_upload_bp.route('/api/search-invoice/<invoice_no>')
@require_auth_api
def search_invoice_api(invoice_no, user_data=None):
    """API endpoint to check if invoice exists"""
    # Check if invoice exists in YTD invoices
    import sqlite3
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    cursor.execute('SELECT invoice_no FROM dhl_ytd_invoices WHERE invoice_no = ?', (invoice_no,))
    exists = cursor.fetchone() is not None
    conn.close()
    
    return jsonify({'exists': exists, 'invoice_no': invoice_no})

def get_recent_pdf_uploads(limit=10):
    """Get recently uploaded PDF details"""
    import sqlite3
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT invoice_no, pdf_filename, service_type, total_amount, 
               currency, extraction_timestamp, extraction_confidence
        FROM invoice_pdf_details 
        ORDER BY extraction_timestamp DESC 
        LIMIT ?
    ''', (limit,))
    
    rows = cursor.fetchall()
    conn.close()
    
    return [{
        'invoice_no': row[0],
        'filename': row[1],
        'service_type': row[2],
        'total_amount': row[3],
        'currency': row[4],
        'timestamp': row[5],
        'confidence': row[6]
    } for row in rows]