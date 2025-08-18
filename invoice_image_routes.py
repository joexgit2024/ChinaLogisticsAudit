#!/usr/bin/env python3
"""
Invoice Image Routes for Flask Application
Handles image upload, viewing, and downloading for DHL Express and DGF invoices
"""

from flask import Blueprint, render_template, request, flash, redirect, url_for, send_file, jsonify
from werkzeug.utils import secure_filename
import os
import mimetypes
from invoice_image_manager import save_invoice_image, get_image_for_invoice, list_all_invoice_images, validate_invoice_exists

# Create blueprint
image_routes = Blueprint('images', __name__)

# Allowed file extensions
ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif', 'pdf', 'tiff', 'bmp'}

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# IMPORTANT: More specific routes must come BEFORE general routes
# Define all specific /invoice-images/* routes before the dashboard route

@image_routes.route('/invoice-images/upload', methods=['GET', 'POST'])
def upload_invoice_image():
    """Upload page for invoice images - supports multiple files"""
    if request.method == 'POST':
        try:
            # Get form data
            invoice_type = request.form.get('invoice_type', '')
            description = request.form.get('description', '')
            uploaded_by = request.form.get('uploaded_by', 'web_user')
            
            # Validate invoice type including FEDEX
            if invoice_type not in ['DHL_EXPRESS', 'FEDEX', 'DGF']:
                flash('Please select a valid carrier type (DHL Express, FedEx, or DGF)', 'error')
                return render_template('invoice_images/upload.html')
            
            # Get files from request
            files = request.files.getlist('files')
            invoice_numbers = request.form.getlist('invoice_numbers')
            
            if not files or len(files) == 0:
                flash('No files selected', 'error')
                return render_template('invoice_images/upload.html')
            
            if len(files) != len(invoice_numbers):
                flash('Number of files must match number of invoice numbers', 'error')
                return render_template('invoice_images/upload.html')
            
            # Process each file
            successful_uploads = []
            failed_uploads = []
            
            for i, (file, invoice_number) in enumerate(zip(files, invoice_numbers)):
                # Validate individual file
                if not file or file.filename == '':
                    failed_uploads.append(f'File {i+1}: No file selected')
                    continue
                
                invoice_number = invoice_number.strip().upper()
                if not invoice_number:
                    failed_uploads.append(f'File {i+1} ({file.filename}): Invoice number is required')
                    continue
                
                # Validate file type
                if not allowed_file(file.filename):
                    failed_uploads.append(f'File {i+1} ({file.filename}): File type not allowed. Allowed types: {", ".join(ALLOWED_EXTENSIONS)}')
                    continue
                
                try:
                    # Optional: Validate invoice exists in database
                    validate_invoice = request.form.get('validate_invoice', 'on') == 'on'
                    if validate_invoice:
                        if not validate_invoice_exists(invoice_number, invoice_type):
                            # Just log as warning but continue
                            flash(f'Invoice {invoice_number} not found in {invoice_type} database - uploaded anyway', 'warning')
                    
                    # Save the image
                    result = save_invoice_image(invoice_number, invoice_type, file, uploaded_by, description)
                    
                    if result['success']:
                        successful_uploads.append(f'{invoice_number} ({file.filename})')
                    else:
                        failed_uploads.append(f'{invoice_number} ({file.filename}): {result["error"]}')
                        
                except Exception as e:
                    failed_uploads.append(f'{invoice_number} ({file.filename}): {str(e)}')
            
            # Show results
            if successful_uploads:
                flash(f'Successfully uploaded {len(successful_uploads)} image(s): {", ".join(successful_uploads)}', 'success')
            
            if failed_uploads:
                for error in failed_uploads:
                    flash(f'Upload failed: {error}', 'error')
            
            if successful_uploads and not failed_uploads:
                return redirect(url_for('images.invoice_images_dashboard'))
            else:
                return render_template('invoice_images/upload.html')
                
        except Exception as e:
            flash(f'Upload error: {str(e)}', 'error')
            return render_template('invoice_images/upload.html')
    
    return render_template('invoice_images/upload.html')

@image_routes.route('/invoice-images/view/<invoice_type>/<invoice_number>')
def view_invoice_image(invoice_type, invoice_number):
    """View invoice image details"""
    try:
        image_info = get_image_for_invoice(invoice_number.upper(), invoice_type)
        
        if not image_info['exists']:
            flash(f'No image found for invoice {invoice_number}', 'warning')
            return redirect(url_for('images.invoice_images_dashboard'))
        
        return render_template('invoice_images/view.html', 
                             image=image_info, 
                             invoice_number=invoice_number,
                             invoice_type=invoice_type)
    except Exception as e:
        flash(f'Error viewing image: {str(e)}', 'error')
        return redirect(url_for('images.invoice_images_dashboard'))

@image_routes.route('/invoice-images/download/<invoice_type>/<invoice_number>')
def download_invoice_image(invoice_type, invoice_number):
    """Download invoice image"""
    try:
        image_info = get_image_for_invoice(invoice_number.upper(), invoice_type)
        
        if not image_info['exists']:
            flash(f'No image found for invoice {invoice_number}', 'error')
            return redirect(url_for('images.invoice_images_dashboard'))
        
        file_path = image_info['image_path']
        if not os.path.exists(file_path):
            flash(f'Image file not found on disk: {file_path}', 'error')
            return redirect(url_for('images.invoice_images_dashboard'))
        
        # Use original filename for download
        download_name = image_info['original_filename'] or image_info['image_filename']
        
        return send_file(file_path, as_attachment=True, download_name=download_name)
        
    except Exception as e:
        flash(f'Download error: {str(e)}', 'error')
        return redirect(url_for('images.invoice_images_dashboard'))

@image_routes.route('/invoice-images/display/<invoice_type>/<invoice_number>')
def display_invoice_image(invoice_type, invoice_number):
    """Display invoice image in browser (for supported formats)"""
    try:
        image_info = get_image_for_invoice(invoice_number, invoice_type)
        
        if not image_info['exists']:
            return "Image not found", 404
        
        file_path = image_info['image_path']
        
        if not os.path.exists(file_path):
            return "Image file not found", 404
        
        # Determine MIME type
        mime_type, _ = mimetypes.guess_type(file_path)
        
        return send_file(file_path, mimetype=mime_type)
        
    except Exception as e:
        return f"Error displaying image: {str(e)}", 500

# Dashboard route comes LAST to avoid conflicts
@image_routes.route('/invoice-images')
def invoice_images_dashboard():
    """Dashboard showing all invoice images"""
    try:
        images = list_all_invoice_images()
        return render_template('invoice_images/dashboard.html', images=images)
    except Exception as e:
        flash(f'Error loading images: {str(e)}', 'error')
        return render_template('invoice_images/dashboard.html', images=[])

@image_routes.route('/api/invoice-images/check/<invoice_type>/<invoice_number>')
def api_check_image_exists(invoice_type, invoice_number):
    """API endpoint to check if image exists for an invoice"""
    try:
        image_info = get_image_for_invoice(invoice_number, invoice_type)
        return jsonify({
            'exists': image_info['exists'],
            'image_info': image_info if image_info['exists'] else None
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@image_routes.route('/api/invoice-images/validate/<invoice_type>/<invoice_number>')
def api_validate_invoice(invoice_type, invoice_number):
    """API endpoint to validate if invoice exists in database"""
    try:
        exists = validate_invoice_exists(invoice_number, invoice_type)
        return jsonify({'exists': exists})
    except Exception as e:
        return jsonify({'error': str(e)}), 500
