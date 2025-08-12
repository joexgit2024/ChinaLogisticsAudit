"""
Script to add missing batch_run_id column to ytd_audit_results table
"""

import sqlite3
import os
import sys

def add_batch_run_id_column():
    print("=== Adding batch_run_id column to ytd_audit_results table ===")
    
    try:
        # Connect to database
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Check if column already exists
        cursor.execute("PRAGMA table_info(ytd_audit_results)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'batch_run_id' in columns:
            print("✅ batch_run_id column already exists")
        else:
            # Add the column
            print("Adding batch_run_id column...")
            cursor.execute("""
                ALTER TABLE ytd_audit_results
                ADD COLUMN batch_run_id INTEGER
                REFERENCES ytd_batch_audit_runs(id)
            """)
            conn.commit()
            print("✅ Successfully added batch_run_id column")
            
            # For now, associate all existing results with batch run #1
            print("Associating existing results with batch run #1...")
            
            # First check if batch run #1 exists
            cursor.execute("SELECT id FROM ytd_batch_audit_runs WHERE id = 1")
            batch_exists = cursor.fetchone()
            
            if batch_exists:
                cursor.execute("UPDATE ytd_audit_results SET batch_run_id = 1")
                conn.commit()
                print(f"✅ Associated {cursor.rowcount} audit results with batch run #1")
            else:
                print("⚠️ No batch run with ID 1 found. Results will remain unassociated.")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    finally:
        if conn:
            conn.close()
    
    return True

if __name__ == "__main__":
    success = add_batch_run_id_column()
    if success:
        print("\n=== Column added successfully ===")
        sys.exit(0)
    else:
        print("\n=== Failed to add column ===")
        sys.exit(1)
