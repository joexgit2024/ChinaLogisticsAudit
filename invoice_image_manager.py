#!/usr/bin/env python3
"""
Invoice Image Management System
Creates image_table and related functionality for DHL Express and DGF invoices
"""

import sqlite3
import os
from datetime import datetime

def create_image_table():
    """Create the image_table to store invoice images"""
    print("🏗️  CREATING INVOICE IMAGE TABLE")
    print("=" * 40)
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Create image_table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS invoice_images (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_number VARCHAR(50) NOT NULL,
            invoice_type VARCHAR(20) NOT NULL CHECK (invoice_type IN ('DHL_EXPRESS', 'DGF')),
            image_filename VARCHAR(255) NOT NULL,
            image_path VARCHAR(500) NOT NULL,
            original_filename VARCHAR(255),
            file_size INTEGER,
            mime_type VARCHAR(100),
            uploaded_by VARCHAR(100),
            upload_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            description TEXT,
            is_active BOOLEAN DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(invoice_number, invoice_type)
        )
    ''')
    
    # Create index for faster lookups
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_invoice_images_lookup 
        ON invoice_images(invoice_number, invoice_type, is_active)
    ''')
    
    # Create uploads directory if it doesn't exist
    upload_dir = 'uploads/invoice_images'
    os.makedirs(upload_dir, exist_ok=True)
    
    # Create subdirectories for organization
    os.makedirs(f'{upload_dir}/dhl_express', exist_ok=True)
    os.makedirs(f'{upload_dir}/dgf', exist_ok=True)
    
    conn.commit()
    conn.close()
    
    print("✅ Invoice image table created successfully")
    print(f"✅ Upload directories created: {upload_dir}")
    
def get_image_for_invoice(invoice_number: str, invoice_type: str) -> dict:
    """Get image information for a specific invoice"""
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, image_filename, image_path, original_filename, 
               file_size, upload_date, uploaded_by, description
        FROM invoice_images 
        WHERE invoice_number = ? AND invoice_type = ? AND is_active = 1
    ''', (invoice_number.upper(), invoice_type))
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return {
            'id': result[0],
            'image_filename': result[1],
            'image_path': result[2],
            'original_filename': result[3],
            'file_size': result[4],
            'upload_date': result[5],
            'uploaded_by': result[6],
            'description': result[7],
            'exists': True
        }
    else:
        return {'exists': False}

def save_invoice_image(invoice_number: str, invoice_type: str, file, uploaded_by: str = 'system', description: str = None) -> dict:
    """Save an uploaded invoice image"""
    try:
        # Validate invoice type
        if invoice_type not in ['DHL_EXPRESS', 'DGF']:
            return {'success': False, 'error': 'Invalid invoice type'}
        
        # Create filename with timestamp to avoid conflicts
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        file_ext = os.path.splitext(file.filename)[1].lower()
        safe_invoice = invoice_number.upper().replace('/', '_').replace('\\', '_')
        new_filename = f"{invoice_type.lower()}_{safe_invoice}_{timestamp}{file_ext}"
        
        # Determine storage path
        subdir = 'dhl_express' if invoice_type == 'DHL_EXPRESS' else 'dgf'
        upload_dir = f'uploads/invoice_images/{subdir}'
        file_path = os.path.join(upload_dir, new_filename)
        
        # Save file
        file.save(file_path)
        
        # Get file info
        file_size = os.path.getsize(file_path)
        mime_type = file.content_type if hasattr(file, 'content_type') else 'application/octet-stream'
        
        # Save to database
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Check if image already exists (replace if it does)
        cursor.execute('''
            UPDATE invoice_images SET is_active = 0 
            WHERE invoice_number = ? AND invoice_type = ?
        ''', (invoice_number, invoice_type))
        
        # Insert new record
        cursor.execute('''
            INSERT INTO invoice_images 
            (invoice_number, invoice_type, image_filename, image_path, 
             original_filename, file_size, mime_type, uploaded_by, description)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (invoice_number, invoice_type, new_filename, file_path,
              file.filename, file_size, mime_type, uploaded_by, description))
        
        conn.commit()
        conn.close()
        
        return {
            'success': True,
            'filename': new_filename,
            'path': file_path,
            'size': file_size
        }
        
    except Exception as e:
        return {'success': False, 'error': str(e)}

def list_all_invoice_images() -> list:
    """List all invoice images in the system"""
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT invoice_number, invoice_type, image_filename, original_filename,
               file_size, upload_date, uploaded_by, description, is_active
        FROM invoice_images 
        ORDER BY upload_date DESC
    ''')
    
    results = cursor.fetchall()
    conn.close()
    
    images = []
    for row in results:
        images.append({
            'invoice_number': row[0],
            'invoice_type': row[1],
            'image_filename': row[2],
            'original_filename': row[3],
            'file_size': row[4],
            'upload_date': row[5],
            'uploaded_by': row[6],
            'description': row[7],
            'is_active': bool(row[8])
        })
    
    return images

def validate_invoice_exists(invoice_number: str, invoice_type: str) -> bool:
    """Check if the invoice number exists in the respective table"""
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    if invoice_type == 'DHL_EXPRESS':
        cursor.execute('SELECT 1 FROM dhl_express_invoices WHERE invoice_no = ? LIMIT 1', (invoice_number.upper(),))
    elif invoice_type == 'DGF':
        cursor.execute('SELECT 1 FROM dhl_ytd_invoices WHERE invoice_no = ? LIMIT 1', (invoice_number.upper(),))
    else:
        conn.close()
        return False
    
    result = cursor.fetchone()
    conn.close()
    
    return result is not None

if __name__ == "__main__":
    create_image_table()
    print("\n🧪 Testing image functions...")
    
    # Test image retrieval for non-existent invoice
    test_result = get_image_for_invoice('TEST123', 'DHL_EXPRESS')
    print(f"Test result: {test_result}")
    
    # List all images
    all_images = list_all_invoice_images()
    print(f"Total images in system: {len(all_images)}")
