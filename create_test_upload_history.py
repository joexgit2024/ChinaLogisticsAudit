#!/usr/bin/env python3
"""
Quick Upload History Test
========================

This script creates some test upload history to verify the display works.
"""

import sqlite3
import json
import uuid
from datetime import datetime, timedelta
from upload_status_manager import UploadStatusManager

def create_test_upload_history():
    """Create test upload history data"""
    
    print("=== Creating Test Upload History ===")
    
    status_manager = UploadStatusManager()
    
    # Create 3 test upload sessions with different outcomes
    test_sessions = [
        {
            'files': [
                {'filename': 'invoices_jan2025.csv', 'original_filename': 'invoices_jan2025.csv', 'size': 512000},
                {'filename': 'rate_card_jan2025.xlsx', 'original_filename': 'rate_card_jan2025.xlsx', 'size': 1024000}
            ],
            'status': 'success',
            'timestamp_offset': -3  # 3 hours ago
        },
        {
            'files': [
                {'filename': 'test_batch.csv', 'original_filename': 'test_batch.csv', 'size': 256000}
            ],
            'status': 'failed',
            'timestamp_offset': -1  # 1 hour ago
        },
        {
            'files': [
                {'filename': 'partial_upload.csv', 'original_filename': 'partial_upload.csv', 'size': 300000},
                {'filename': 'bad_rate_card.xlsx', 'original_filename': 'bad_rate_card.xlsx', 'size': 800000}
            ],
            'status': 'partial',
            'timestamp_offset': -0.5  # 30 minutes ago
        }
    ]
    
    for i, session_data in enumerate(test_sessions):
        print(f"\nCreating test session {i+1}...")
        
        # Create session
        session_id = status_manager.create_upload_session(session_data['files'])
        
        # Simulate file processing
        for j, file_info in enumerate(session_data['files']):
            if session_data['status'] == 'success':
                result = {
                    'success': True,
                    'message': 'File processed successfully',
                    'records_processed': 100 + (j * 50)
                }
            elif session_data['status'] == 'failed':
                result = {
                    'success': False,
                    'message': 'File processing failed',
                    'error': 'Invalid format'
                }
            else:  # partial
                result = {
                    'success': j == 0,  # First file succeeds, second fails
                    'message': 'File processed' if j == 0 else 'File failed',
                    'records_processed': 75 if j == 0 else 0,
                    'error': 'Sheet not found' if j > 0 else None
                }
            
            status_manager.update_file_result(session_id, file_info['filename'], result)
        
        # Finalize session
        overall_result = {
            'success': session_data['status'] == 'success',
            'files_processed': [
                {'filename': f['filename'], 'type': 'invoice_csv' if f['filename'].endswith('.csv') else 'rate_card_excel'}
                for f in session_data['files']
            ]
        }
        
        if session_data['status'] != 'success':
            overall_result['error'] = f"Session {session_data['status']}"
        
        status_manager.finalize_upload_session(session_id, overall_result)
        
        # Update timestamp to simulate different upload times
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        new_timestamp = datetime.now() + timedelta(hours=session_data['timestamp_offset'])
        cursor.execute('''
            UPDATE upload_sessions 
            SET upload_timestamp = ?, created_at = ?, updated_at = ?
            WHERE session_id = ?
        ''', (new_timestamp.isoformat(), new_timestamp.isoformat(), new_timestamp.isoformat(), session_id))
        
        cursor.execute('''
            UPDATE upload_files 
            SET upload_timestamp = ?
            WHERE session_id = ?
        ''', (new_timestamp.isoformat(), session_id))
        
        conn.commit()
        conn.close()
        
        print(f"   Session {session_id[:8]}... created with status: {session_data['status']}")
    
    # Set the most recent successful session as the current display status
    print("\nSetting current display status...")
    
    # Get the most recent successful session
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT session_id FROM upload_sessions 
        WHERE status = 'success'
        ORDER BY upload_timestamp DESC
        LIMIT 1
    ''')
    
    recent_session = cursor.fetchone()
    if recent_session:
        recent_session_id = recent_session[0]
        
        # Get the session details and set as current status
        session_details = status_manager.get_session_details(recent_session_id)
        if session_details:
            status_data = {
                'session_id': recent_session_id,
                'status': 'success',
                'timestamp': session_details['upload_timestamp'],
                'files_processed': [
                    {
                        'filename': f['filename'],
                        'type': f['file_type'],
                        'status': f['status'],
                        'result': f['processing_result']
                    }
                    for f in session_details['files']
                ],
                'overall_result': session_details['processing_details']
            }
            
            cursor.execute('''
                UPDATE current_upload_status 
                SET session_id = ?, status_data = ?, display_timestamp = CURRENT_TIMESTAMP, is_visible = 1
                WHERE id = 1
            ''', (recent_session_id, json.dumps(status_data)))
            
            conn.commit()
            print(f"   Current status set to session: {recent_session_id[:8]}...")
    
    conn.close()
    
    print("\n✅ Test upload history created!")
    print("\nNow refresh the upload page to see:")
    print("📋 Persistent upload status at the bottom")
    print("📊 Upload history in the Recent Uploads section")
    print("🔍 Click the eye icon to view session details")

if __name__ == "__main__":
    create_test_upload_history()
