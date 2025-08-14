"""FedEx routes blueprint - phase 1 scaffold."""
from flask import Blueprint, render_template, request, jsonify
import os
import datetime
import uuid
from fedex_audit_engine import FedExAuditEngine

try:
    from auth_routes import require_auth, require_auth_api
except Exception:
    def require_auth(f): return f
    def require_auth_api(f): return f

fedex_bp = Blueprint('fedex', __name__)

@fedex_bp.route('/fedex')
@require_auth
def fedex_dashboard(user_data=None):
    engine = FedExAuditEngine()
    summary = engine.get_invoice_summary()
    return render_template('fedex_dashboard.html', summary=summary)

@fedex_bp.route('/fedex/upload', methods=['GET','POST'])
@require_auth
def fedex_upload(user_data=None):
    if request.method == 'GET':
        return render_template('fedex_upload.html')
    
    engine = FedExAuditEngine()
    res_list = []
    
    # Create uploads directory with date-based subfolder
    today = datetime.datetime.now().strftime('%Y%m%d')
    upload_dir = os.path.join('uploads', 'fedex', today)
    os.makedirs(upload_dir, exist_ok=True)
    
    for key, f in request.files.items():
        if f and f.filename:
            original_filename = f.filename
            
            # Generate unique filename with timestamp and UUID
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            file_ext = os.path.splitext(original_filename)[1].lower()
            unique_id = str(uuid.uuid4())[:8]
            
            # Clean original filename (remove special characters)
            clean_name = "".join(c for c in os.path.splitext(original_filename)[0] if c.isalnum() or c in (' ', '-', '_')).strip()
            clean_name = clean_name.replace(' ', '_')
            
            # Create new filename: timestamp_cleanname_uniqueid.ext
            new_filename = f"{timestamp}_{clean_name}_{unique_id}{file_ext}"
            file_path = os.path.join(upload_dir, new_filename)
            
            try:
                f.save(file_path)
                
                # Process the file
                if file_ext in ['.xls', '.xlsx']:
                    result = engine.load_invoice_xls(file_path)
                    
                    # Add file metadata to result
                    result['original_filename'] = original_filename
                    result['stored_filename'] = new_filename
                    result['file_path'] = file_path
                    result['upload_timestamp'] = timestamp
                    
                else:
                    result = {
                        'success': False, 
                        'error': f'Unsupported file type: {file_ext}',
                        'original_filename': original_filename,
                        'stored_filename': new_filename
                    }
                    
            except Exception as e:
                result = {
                    'success': False,
                    'error': f'File processing error: {str(e)}',
                    'original_filename': original_filename
                }
                
            res_list.append({'file': original_filename, 'result': result})
    
    overall_success = all(r['result'].get('success', False) for r in res_list)
    
    return jsonify({
        'success': overall_success, 
        'details': res_list,
        'upload_directory': upload_dir,
        'processed_count': len(res_list)
    })
