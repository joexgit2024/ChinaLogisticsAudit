"""
Add delete_batch method to the DatabaseManager class
"""

from typing import Dict, List, Tuple
import sqlite3

def delete_batch(self, batch_id: int) -> bool:
    """Delete a batch run and its associated audit results.
    
    Args:
        batch_id: The ID of the batch run to delete.
        
    Returns:
        True if the deletion was successful, False otherwise.
    """
    conn = self.get_connection()
    cursor = conn.cursor()
    
    try:
        # Delete audit results first (foreign key constraint)
        cursor.execute("DELETE FROM ytd_audit_results WHERE batch_run_id = ?", (batch_id,))
        
        # Delete the batch run
        cursor.execute("DELETE FROM ytd_batch_audit_runs WHERE id = ?", (batch_id,))
        
        # Commit the changes
        conn.commit()
        return True
    except Exception as e:
        print(f"Error deleting batch {batch_id}: {str(e)}")
        conn.rollback()
        return False
    finally:
        conn.close()
