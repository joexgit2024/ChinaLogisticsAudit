#!/usr/bin/env python3
"""
Upload Status Manager
====================

Manages persistent upload status and history for DHL Express uploads.
"""

import sqlite3
import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple

class UploadStatusManager:
    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path
    
    def create_upload_session(self, files_info: List[Dict]) -> str:
        """Create a new upload session and return session ID"""
        session_id = str(uuid.uuid4())
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        total_files = len(files_info)
        total_size = sum(f.get('size', 0) for f in files_info)
        
        cursor.execute('''
            INSERT INTO upload_sessions 
            (session_id, status, total_files, total_size)
            VALUES (?, ?, ?, ?)
        ''', (session_id, 'processing', total_files, total_size))
        
        # Add individual files to tracking
        for file_info in files_info:
            file_type = 'invoice_csv' if file_info['filename'].lower().endswith('.csv') else 'rate_card_excel'
            cursor.execute('''
                INSERT INTO upload_files
                (session_id, filename, original_filename, file_type, file_size, status)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (session_id, file_info['filename'], file_info['original_filename'], 
                  file_type, file_info.get('size', 0), 'processing'))
        
        conn.commit()
        conn.close()
        
        return session_id
    
    def update_file_result(self, session_id: str, filename: str, result: Dict):
        """Update result for a specific file in the session"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        status = 'success' if result.get('success', True) else 'failed'
        
        cursor.execute('''
            UPDATE upload_files 
            SET status = ?, processing_result = ?, 
                records_processed = ?, error_details = ?
            WHERE session_id = ? AND filename = ?
        ''', (
            status,
            json.dumps(result),
            result.get('records_processed', 0),
            json.dumps(result.get('error_details', {})) if result.get('error_details') else None,
            session_id,
            filename
        ))
        
        conn.commit()
        conn.close()
    
    def finalize_upload_session(self, session_id: str, overall_result: Dict):
        """Mark upload session as complete and update status"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Count successful and failed files
            cursor.execute('''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
                FROM upload_files 
                WHERE session_id = ?
            ''', (session_id,))
            
            counts = cursor.fetchone()
            
            # Determine overall status
            if counts[2] == 0:  # No failed files
                overall_status = 'success'
            elif counts[1] == 0:  # No successful files
                overall_status = 'failed'
            else:  # Mixed results
                overall_status = 'partial'
            
            # Update session
            cursor.execute('''
                UPDATE upload_sessions 
                SET status = ?, successful_files = ?, failed_files = ?,
                    error_message = ?, processing_details = ?, updated_at = CURRENT_TIMESTAMP
                WHERE session_id = ?
            ''', (
                overall_status,
                counts[1],
                counts[2],
                overall_result.get('error'),
                json.dumps(overall_result),
                session_id
            ))
            
            conn.commit()
            
            # Update current status for display (separate transaction)
            self._update_current_status(session_id, overall_result, overall_status)
            
            return overall_status
            
        except Exception as e:
            conn.rollback()
            print(f"Error finalizing upload session: {e}")
            return 'error'
        finally:
            conn.close()
    
    def _update_current_status(self, session_id: str, result: Dict, status: str):
        """Update the current upload status for persistent display"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Get detailed file information
            cursor.execute('''
                SELECT filename, file_type, status, processing_result, error_details
                FROM upload_files 
                WHERE session_id = ?
                ORDER BY upload_timestamp
            ''', (session_id,))
            
            files_detail = []
            for row in cursor.fetchall():
                file_result = json.loads(row[3]) if row[3] else {}
                error_details = json.loads(row[4]) if row[4] else {}
                
                files_detail.append({
                    'filename': row[0],
                    'type': row[1],
                    'status': row[2],
                    'result': file_result,
                    'error_details': error_details
                })
            
            status_data = {
                'session_id': session_id,
                'status': status,
                'timestamp': datetime.now().isoformat(),
                'files_processed': files_detail,
                'overall_result': result
            }
            
            cursor.execute('''
                UPDATE current_upload_status 
                SET session_id = ?, status_data = ?, display_timestamp = CURRENT_TIMESTAMP, is_visible = 1
                WHERE id = 1
            ''', (session_id, json.dumps(status_data)))
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            print(f"Error updating current status: {e}")
        finally:
            conn.close()
    
    def get_current_status(self) -> Optional[Dict]:
        """Get the current persistent upload status"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT status_data, is_visible 
            FROM current_upload_status 
            WHERE id = 1
        ''')
        
        row = cursor.fetchone()
        conn.close()
        
        if row and row[1] and row[0]:  # is_visible and has status_data
            return json.loads(row[0])
        
        return None
    
    def clear_current_status(self):
        """Clear the current upload status display"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE current_upload_status 
            SET is_visible = 0, session_id = NULL, status_data = NULL
            WHERE id = 1
        ''')
        
        conn.commit()
        conn.close()
    
    def get_upload_history(self, limit: int = 20) -> List[Dict]:
        """Get recent upload history"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                s.session_id, s.upload_timestamp, s.status,
                s.total_files, s.successful_files, s.failed_files,
                s.total_size, s.error_message,
                GROUP_CONCAT(f.filename, '|') as filenames,
                GROUP_CONCAT(f.file_type, '|') as file_types,
                GROUP_CONCAT(f.status, '|') as file_statuses
            FROM upload_sessions s
            LEFT JOIN upload_files f ON s.session_id = f.session_id
            GROUP BY s.session_id, s.upload_timestamp, s.status,
                     s.total_files, s.successful_files, s.failed_files,
                     s.total_size, s.error_message
            ORDER BY s.upload_timestamp DESC
            LIMIT ?
        ''', (limit,))
        
        history = []
        for row in cursor.fetchall():
            files_info = []
            if row[8]:  # filenames exist
                filenames = row[8].split('|')
                file_types = row[9].split('|') if row[9] else []
                file_statuses = row[10].split('|') if row[10] else []
                
                for i, filename in enumerate(filenames):
                    files_info.append({
                        'filename': filename,
                        'type': file_types[i] if i < len(file_types) else 'unknown',
                        'status': file_statuses[i] if i < len(file_statuses) else 'unknown'
                    })
            
            history.append({
                'session_id': row[0],
                'timestamp': row[1],
                'status': row[2],
                'total_files': row[3],
                'successful_files': row[4],
                'failed_files': row[5],
                'total_size': row[6],
                'error_message': row[7],
                'files': files_info
            })
        
        conn.close()
        return history
    
    def get_session_details(self, session_id: str) -> Optional[Dict]:
        """Get detailed information about a specific upload session"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get session info
        cursor.execute('''
            SELECT * FROM upload_sessions WHERE session_id = ?
        ''', (session_id,))
        
        session = cursor.fetchone()
        if not session:
            conn.close()
            return None
        
        # Get file details
        cursor.execute('''
            SELECT * FROM upload_files WHERE session_id = ?
            ORDER BY upload_timestamp
        ''', (session_id,))
        
        files = []
        for file_row in cursor.fetchall():
            processing_result = json.loads(file_row[7]) if file_row[7] else {}
            error_details = json.loads(file_row[10]) if file_row[10] else {}
            
            files.append({
                'filename': file_row[2],
                'original_filename': file_row[3],
                'file_type': file_row[4],
                'file_size': file_row[5],
                'status': file_row[6],
                'processing_result': processing_result,
                'sheets_processed': json.loads(file_row[8]) if file_row[8] else [],
                'records_processed': file_row[9],
                'records_failed': file_row[10],
                'error_details': error_details,
                'upload_timestamp': file_row[11]
            })
        
        conn.close()
        
        return {
            'session_id': session[1],
            'upload_timestamp': session[2],
            'status': session[3],
            'total_files': session[4],
            'successful_files': session[5],
            'failed_files': session[6],
            'total_size': session[7],
            'error_message': session[8],
            'processing_details': json.loads(session[9]) if session[9] else {},
            'files': files
        }
