#!/usr/bin/env python3
"""
FedEx Invoice Upload and Extraction Routes
Integrates with the existing FedEx system
"""

from flask import Blueprint, render_template, request, jsonify, redirect, url_for, flash
import os
import json
from datetime import datetime
import pandas as pd
import sqlite3
from werkzeug.utils import secure_filename
import fitz  # PyMuPDF
import re
from pathlib import Path

# Import existing FedEx components
try:
    from simple_fedex_extractor import SimpleFedExExtractor
except ImportError:
    # Fallback simple extractor
    class SimpleFedExExtractor:
        def __init__(self, db_path='dhl_audit.db'):
            self.db_path = db_path
            self.setup_database()
        
        def setup_database(self):
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fedex_extracted_invoices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    file_path TEXT NOT NULL,
                    file_name TEXT NOT NULL,
                    invoice_number TEXT,
                    extraction_method TEXT,
                    extraction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'extracted',
                    notes TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fedex_invoices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    invoice_number TEXT UNIQUE,
                    file_path TEXT,
                    file_name TEXT,
                    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'uploaded',
                    extraction_method TEXT
                )
            ''')
            conn.commit()
            conn.close()
        
        def extract_invoice_number(self, pdf_path):
            """Extract invoice number from FedEx PDF using PyMuPDF"""
            try:
                doc = fitz.open(pdf_path)
                
                # Check all pages for text
                for page_num in range(doc.page_count):
                    page = doc[page_num]
                    text = page.get_text()
                    
                    if text.strip():  # If there's extractable text
                        # Look for 10-digit invoice numbers
                        pattern = r'\b\d{10}\b'
                        matches = re.findall(pattern, text)
                        if matches:
                            doc.close()
                            return matches[0], f"text_extraction_page_{page_num+1}"
                
                doc.close()
                # For image-based PDFs, return None
                return None, "image_based_pdf_needs_ocr"
                
            except Exception as e:
                return None, f"error: {str(e)}"
        
        def process_single_file(self, pdf_path):
            invoice_number, method = self.extract_invoice_number(pdf_path)
            
            # Store in database
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            try:
                cursor.execute('''
                    INSERT INTO fedex_extracted_invoices 
                    (file_path, file_name, invoice_number, extraction_method, notes)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    pdf_path,
                    os.path.basename(pdf_path),
                    invoice_number,
                    method,
                    f"Extracted on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                ))
                
                if invoice_number:
                    cursor.execute('''
                        INSERT OR REPLACE INTO fedex_invoices 
                        (invoice_number, file_path, file_name, extraction_method)
                        VALUES (?, ?, ?, ?)
                    ''', (
                        invoice_number,
                        pdf_path,
                        os.path.basename(pdf_path),
                        method
                    ))
                
                conn.commit()
            except Exception as e:
                print(f"Database error: {e}")
            finally:
                conn.close()
            
            return {
                'file_name': os.path.basename(pdf_path),
                'invoice_number': invoice_number,
                'method': method,
                'success': invoice_number is not None
            }

fedex_extract_bp = Blueprint('fedex_extract', __name__, url_prefix='/fedex')

UPLOAD_FOLDER = 'uploads/fedex_invoices'
ALLOWED_EXTENSIONS = {'pdf'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def ensure_upload_folder():
    """Ensure upload folder exists"""
    if not os.path.exists(UPLOAD_FOLDER):
        os.makedirs(UPLOAD_FOLDER)

@fedex_extract_bp.route('/invoice-upload', methods=['GET', 'POST'])
def invoice_upload():
    """FedEx invoice upload with automatic number extraction"""
    
    if request.method == 'POST':
        try:
            ensure_upload_folder()
            extractor = SimpleFedExExtractor()
            
            files = request.files.getlist('files')
            if not files or files[0].filename == '':
                flash('No files selected', 'error')
                return redirect(request.url)
            
            results = []
            success_count = 0
            
            for file in files:
                if file and allowed_file(file.filename):
                    # Save file
                    filename = secure_filename(file.filename)
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    filename = f"{timestamp}_{filename}"
                    filepath = os.path.join(UPLOAD_FOLDER, filename)
                    file.save(filepath)
                    
                    # Extract invoice number
                    try:
                        result = extractor.process_single_file(filepath)
                        results.append(result)
                        
                        if result['success']:
                            success_count += 1
                            
                            # Store in FedEx invoices table
                            conn = sqlite3.connect('dhl_audit.db')
                            cursor = conn.cursor()
                            
                            # Check if FedEx invoices table exists, create if not
                            cursor.execute('''
                                CREATE TABLE IF NOT EXISTS fedex_invoices (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    invoice_number TEXT UNIQUE,
                                    file_path TEXT,
                                    file_name TEXT,
                                    upload_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                    status TEXT DEFAULT 'uploaded',
                                    extraction_method TEXT
                                )
                            ''')
                            
                            # Insert invoice record
                            cursor.execute('''
                                INSERT OR REPLACE INTO fedex_invoices 
                                (invoice_number, file_path, file_name, extraction_method)
                                VALUES (?, ?, ?, ?)
                            ''', (
                                result['invoice_number'],
                                filepath,
                                result['file_name'],
                                result['method']
                            ))
                            
                            conn.commit()
                            conn.close()
                            
                    except Exception as e:
                        results.append({
                            'file_name': filename,
                            'invoice_number': None,
                            'method': f'Error: {str(e)}',
                            'success': False
                        })
            
            flash(f'Upload complete: {success_count}/{len(files)} invoices processed successfully', 'success')
            return render_template('fedex/invoice_upload_results.html', results=results)
            
        except Exception as e:
            flash(f'Upload error: {str(e)}', 'error')
            return redirect(request.url)
    
    return render_template('fedex/invoice_upload.html')

@fedex_extract_bp.route('/extracted-invoices')
def extracted_invoices():
    """View extracted invoice numbers"""
    try:
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Get both extracted and uploaded invoices
        cursor.execute('''
            SELECT 
                COALESCE(fi.invoice_number, fei.invoice_number) as invoice_number,
                COALESCE(fi.file_name, fei.file_name) as file_name,
                COALESCE(fi.upload_date, fei.extraction_date) as date,
                COALESCE(fi.status, fei.status) as status,
                COALESCE(fi.extraction_method, fei.extraction_method) as method
            FROM fedex_invoices fi
            FULL OUTER JOIN fedex_extracted_invoices fei 
                ON fi.invoice_number = fei.invoice_number
            ORDER BY date DESC
        ''')
        
        invoices = cursor.fetchall()
        conn.close()
        
        return render_template('fedex/extracted_invoices.html', invoices=invoices)
        
    except Exception as e:
        flash(f'Error loading invoices: {str(e)}', 'error')
        return render_template('fedex/extracted_invoices.html', invoices=[])

@fedex_extract_bp.route('/api/extract-number', methods=['POST'])
def extract_number_api():
    """API endpoint for extracting invoice number from uploaded file"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Invalid file type. Only PDF files allowed'}), 400
        
        # Save temporary file
        ensure_upload_folder()
        filename = secure_filename(file.filename)
        temp_path = os.path.join(UPLOAD_FOLDER, f"temp_{filename}")
        file.save(temp_path)
        
        # Extract invoice number
        extractor = SimpleFedExExtractor()
        result = extractor.process_single_file(temp_path)
        
        # Clean up temp file
        if os.path.exists(temp_path):
            os.remove(temp_path)
        
        if result['success']:
            return jsonify({
                'success': True,
                'invoice_number': result['invoice_number'],
                'method': result['method']
            })
        else:
            return jsonify({
                'success': False,
                'error': 'Could not extract invoice number',
                'method': result['method']
            }), 400
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@fedex_extract_bp.route('/api/manual-invoice', methods=['POST'])
def manual_invoice_api():
    """API endpoint for manually adding invoice numbers"""
    try:
        data = request.get_json()
        if not data or 'invoice_number' not in data:
            return jsonify({'error': 'Invoice number required'}), 400
        
        invoice_number = data['invoice_number'].strip()
        
        # Validate 10-digit number
        if not re.match(r'^\d{10}$', invoice_number):
            return jsonify({'error': 'Invoice number must be exactly 10 digits'}), 400
        
        # Store in database
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        try:
            # Check if already exists
            cursor.execute('SELECT id FROM fedex_invoices WHERE invoice_number = ?', (invoice_number,))
            if cursor.fetchone():
                return jsonify({'error': 'Invoice number already exists'}), 400
            
            # Insert new manual invoice
            cursor.execute('''
                INSERT INTO fedex_invoices 
                (invoice_number, file_path, file_name, extraction_method, status)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                invoice_number,
                'manual_entry',
                f'Manual_{invoice_number}',
                'manual_input',
                'uploaded'
            ))
            
            # Also add to extracted invoices table
            cursor.execute('''
                INSERT INTO fedex_extracted_invoices 
                (file_path, file_name, invoice_number, extraction_method, status, notes)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                'manual_entry',
                f'Manual_{invoice_number}',
                invoice_number,
                'manual_input',
                'extracted',
                f'Manually entered on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'
            ))
            
            conn.commit()
            return jsonify({
                'success': True,
                'message': f'Invoice {invoice_number} added successfully'
            })
            
        finally:
            conn.close()
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@fedex_extract_bp.route('/api/stats', methods=['GET'])
def stats_api():
    """API endpoint for getting invoice statistics"""
    try:
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Get total invoices
        cursor.execute('SELECT COUNT(*) FROM fedex_invoices')
        total = cursor.fetchone()[0] or 0
        
        # Get ready invoices (those with extracted numbers)
        cursor.execute('SELECT COUNT(*) FROM fedex_invoices WHERE invoice_number IS NOT NULL')
        ready = cursor.fetchone()[0] or 0
        
        conn.close()
        
        return jsonify({
            'total': total,
            'ready': ready
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Register the blueprint (this would be done in app.py)
def register_fedex_extraction_routes(app):
    """Register FedEx extraction routes with the Flask app"""
    app.register_blueprint(fedex_extract_bp)
