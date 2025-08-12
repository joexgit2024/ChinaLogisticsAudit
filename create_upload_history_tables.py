#!/usr/bin/env python3
"""
Create Upload History & Status Tables
=====================================

This script creates the necessary database tables for persistent upload 
status and history tracking.
"""

import sqlite3
import json
from datetime import datetime

def create_upload_history_tables():
    """Create upload history and status tables"""
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    print("=== Creating Upload History & Status Tables ===")
    
    # Create upload_sessions table for tracking upload sessions
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS upload_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT UNIQUE NOT NULL,
            upload_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            status TEXT NOT NULL,  -- 'success', 'failed', 'partial'
            total_files INTEGER DEFAULT 0,
            successful_files INTEGER DEFAULT 0,
            failed_files INTEGER DEFAULT 0,
            total_size INTEGER DEFAULT 0,
            error_message TEXT,
            processing_details TEXT,  -- JSON with detailed results
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create upload_files table for tracking individual file uploads
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS upload_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            filename TEXT NOT NULL,
            original_filename TEXT NOT NULL,
            file_type TEXT NOT NULL,  -- 'invoice_csv', 'rate_card_excel'
            file_size INTEGER DEFAULT 0,
            status TEXT NOT NULL,  -- 'success', 'failed', 'warning'
            processing_result TEXT,  -- JSON with processing details
            sheets_processed TEXT,  -- JSON array of sheet names for Excel files
            records_processed INTEGER DEFAULT 0,
            records_failed INTEGER DEFAULT 0,
            error_details TEXT,  -- JSON with error information
            upload_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (session_id) REFERENCES upload_sessions (session_id)
        )
    ''')
    
    # Create current_upload_status table for persistent status display
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS current_upload_status (
            id INTEGER PRIMARY KEY,  -- Only one record allowed
            session_id TEXT,
            status_data TEXT,  -- JSON with complete status information
            display_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            is_visible BOOLEAN DEFAULT 1,
            FOREIGN KEY (session_id) REFERENCES upload_sessions (session_id)
        )
    ''')
    
    # Insert initial empty status if not exists
    cursor.execute('''
        INSERT OR IGNORE INTO current_upload_status (id, is_visible)
        VALUES (1, 0)
    ''')
    
    conn.commit()
    print("✅ Upload history tables created successfully")
    
    # Verify tables
    print("\n📊 Verifying table structures:")
    
    tables = ['upload_sessions', 'upload_files', 'current_upload_status']
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        columns = cursor.fetchall()
        print(f"\n{table} table:")
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
    
    conn.close()
    return True

if __name__ == "__main__":
    success = create_upload_history_tables()
    if success:
        print("\n🎉 Upload history system ready!")
    else:
        print("\n❌ Failed to create upload history tables")
