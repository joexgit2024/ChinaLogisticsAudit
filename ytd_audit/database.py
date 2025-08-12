"""
Database utilities for YTD Audit System.
"""

import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Tuple


class DatabaseManager:
    """Manages database operations for the YTD Audit System."""
    
    def __init__(self, db_path: str = "dhl_audit.db"):
        """Initialize the database manager.
        
        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = db_path
        self.ensure_tables_exist()
    
    def get_connection(self) -> sqlite3.Connection:
        """Get a database connection.
        
        Returns:
            A SQLite connection object.
        """
        return sqlite3.connect(self.db_path)
    
    def ensure_tables_exist(self) -> None:
        """Create required tables if they don't exist."""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Create batch audit runs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ytd_batch_audit_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_name TEXT NOT NULL,
                status TEXT DEFAULT 'running',
                total_invoices INTEGER DEFAULT 0,
                invoices_passed INTEGER DEFAULT 0,
                invoices_warned INTEGER DEFAULT 0,
                invoices_failed INTEGER DEFAULT 0,
                invoices_error INTEGER DEFAULT 0,
                processing_time_ms INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP
            )
        ''')
        
        # Create audit results table with batch_run_id
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ytd_audit_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_run_id INTEGER,
                invoice_no TEXT NOT NULL,
                audit_status TEXT NOT NULL,
                transportation_mode TEXT,
                total_invoice_amount REAL DEFAULT 0,
                total_expected_amount REAL DEFAULT 0,
                total_variance REAL DEFAULT 0,
                variance_percent REAL DEFAULT 0,
                rate_cards_checked INTEGER DEFAULT 0,
                matching_lanes INTEGER DEFAULT 0,
                best_match_rate_card TEXT,
                audit_details TEXT,
                processing_time_ms INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (batch_run_id) REFERENCES ytd_batch_audit_runs(id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def get_all_ytd_invoices(self) -> List[Tuple]:
        """Get all invoices from YTD tables for auditing.
        
        Returns:
            A list of invoice tuples (invoice_no, transportation_mode, total_amount, 
            origin, destination, weight, service_type).
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Check which YTD tables exist and get invoices
            table_names = ['dhl_ytd_invoices', 'ytd_invoices', 'invoices']
            all_invoices = []
            
            for table_name in table_names:
                try:
                    cursor.execute(f"""
                        SELECT name FROM sqlite_master 
                        WHERE type='table' AND name=?
                    """, (table_name,))
                    
                    if cursor.fetchone():
                        # Table exists, get invoices with available columns
                        if table_name == 'dhl_ytd_invoices':
                            # Check what columns are available
                            cursor.execute(f"PRAGMA table_info({table_name})")
                            columns = [row[1] for row in cursor.fetchall()]
                            
                            # Build query based on available columns
                            select_cols = ['invoice_no']
                            if 'transportation_mode' in columns:
                                select_cols.append('transportation_mode')
                            else:
                                select_cols.append("'unknown' as transportation_mode")
                                
                            # Map to the correct total amount column - USE USD COLUMNS
                            if 'total_charges_with_duty_tax_usd' in columns:
                                select_cols.append('COALESCE(total_charges_with_duty_tax_usd, 0) as total_amount')
                            elif 'total_charges_without_duty_tax_usd' in columns:
                                select_cols.append('COALESCE(total_charges_without_duty_tax_usd, 0) as total_amount')
                            elif 'total_charges_with_duty_tax' in columns:
                                select_cols.append('COALESCE(total_charges_with_duty_tax, 0) as total_amount')
                            elif 'total_charges_without_duty_tax' in columns:
                                select_cols.append('COALESCE(total_charges_without_duty_tax, 0) as total_amount')
                            elif 'total_amount' in columns:
                                select_cols.append('COALESCE(total_amount, 0) as total_amount')
                            elif 'invoice_amount' in columns:
                                select_cols.append('COALESCE(invoice_amount, 0) as total_amount')
                            else:
                                select_cols.append('0 as total_amount')
                            
                            if 'origin' in columns:
                                select_cols.append('origin')
                            else:
                                select_cols.append("'Unknown' as origin")
                                
                            if 'destination' in columns:
                                select_cols.append('destination')
                            else:
                                select_cols.append("'Unknown' as destination")
                                
                            # Map to the correct weight column
                            if 'shipment_weight_kg' in columns:
                                select_cols.append('COALESCE(shipment_weight_kg, 0) as weight')
                            elif 'total_shipment_chargeable_weight_kg' in columns:
                                select_cols.append('COALESCE(total_shipment_chargeable_weight_kg, 0) as weight')
                            elif 'weight' in columns:
                                select_cols.append('COALESCE(weight, 0) as weight')
                            else:
                                select_cols.append('0 as weight')
                                
                            if 'service_type' in columns:
                                select_cols.append('service_type')
                            else:
                                select_cols.append("'Standard' as service_type")
                            
                            query = f"SELECT {', '.join(select_cols)} FROM {table_name}"
                            cursor.execute(query)
                            invoices = cursor.fetchall()
                            all_invoices.extend(invoices)
                            print(f"Found {len(invoices)} invoices in {table_name}")
                            break  # Use first available table
                            
                except Exception as e:
                    print(f"Error querying table {table_name}: {e}")
                    continue
            
            return all_invoices
            
        except Exception as e:
            print(f"Error getting YTD invoices: {e}")
            return []
        finally:
            conn.close()
    
    def create_batch_run(self, batch_name: str) -> int:
        """Create a new batch run record.
        
        Args:
            batch_name: Name of the batch run.
            
        Returns:
            The ID of the created batch run.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO ytd_batch_audit_runs (run_name, status, created_at)
                VALUES (?, 'running', ?)
            """, (batch_name, datetime.now()))
            
            batch_run_id = cursor.lastrowid
            conn.commit()
            return batch_run_id
        except Exception as e:
            print(f"Error creating batch run: {e}")
            raise
        finally:
            conn.close()
    
    def update_batch_run(self, batch_run_id: int, status: str, total_invoices: int,
                        passed: int, warned: int, failed: int, error: int,
                        processing_time: int = 0) -> bool:
        """Update a batch run with final results.
        
        Args:
            batch_run_id: ID of the batch run to update.
            status: New status of the batch run.
            total_invoices: Total number of invoices processed.
            passed: Number of invoices that passed audit.
            warned: Number of invoices with warnings.
            failed: Number of invoices that failed audit.
            error: Number of invoices with errors.
            processing_time: Processing time in milliseconds.
            
        Returns:
            True if update was successful, False otherwise.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                UPDATE ytd_batch_audit_runs 
                SET status = ?,
                    total_invoices = ?,
                    invoices_passed = ?,
                    invoices_warned = ?,
                    invoices_failed = ?,
                    invoices_error = ?,
                    processing_time_ms = ?,
                    end_time = ?
                WHERE id = ?
            """, (status, total_invoices, passed, warned, failed, error, 
                  processing_time, datetime.now(), batch_run_id))
            
            conn.commit()
            return True
        except Exception as e:
            print(f"Error updating batch run: {e}")
            return False
        finally:
            conn.close()
    
    def save_audit_result(self, batch_run_id: int, invoice_no: str, status: str,
                         transportation_mode: str, invoice_amount: float,
                         expected_amount: float, variance: float, variance_percent: float,
                         rate_cards_checked: int, matching_lanes: int,
                         best_match_rate_card: str, audit_details: Dict) -> bool:
        """Save an audit result to the database.
        
        Args:
            batch_run_id: ID of the batch run.
            invoice_no: Invoice number.
            status: Audit status.
            transportation_mode: Transportation mode.
            invoice_amount: Total invoice amount.
            expected_amount: Expected amount from rate card.
            variance: Difference between invoice and expected amount.
            variance_percent: Variance as a percentage.
            rate_cards_checked: Number of rate cards checked.
            matching_lanes: Number of matching lanes found.
            best_match_rate_card: Description of best matching rate card.
            audit_details: Detailed audit information.
            
        Returns:
            True if save was successful, False otherwise.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO ytd_audit_results (
                    batch_run_id, invoice_no, audit_status, transportation_mode,
                    total_invoice_amount, total_expected_amount, total_variance,
                    variance_percent, rate_cards_checked, matching_lanes,
                    best_match_rate_card, audit_details, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                batch_run_id, invoice_no, status, transportation_mode,
                invoice_amount, expected_amount, variance, variance_percent,
                rate_cards_checked, matching_lanes, best_match_rate_card,
                json.dumps(audit_details), datetime.now()
            ))
            
            conn.commit()
            return True
        except Exception as e:
            print(f"Error saving audit result: {e}")
            return False
        finally:
            conn.close()
    
    def save_error_result(self, batch_run_id: int, invoice_no: str, error_message: str) -> bool:
        """Save an error result to the database.
        
        Args:
            batch_run_id: ID of the batch run.
            invoice_no: Invoice number.
            error_message: Error message.
            
        Returns:
            True if save was successful, False otherwise.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO ytd_audit_results (
                    batch_run_id, invoice_no, audit_status, transportation_mode,
                    total_invoice_amount, total_expected_amount, total_variance,
                    variance_percent, rate_cards_checked, matching_lanes,
                    audit_details, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                batch_run_id, invoice_no, 'error', 'unknown',
                0.0, 0.0, 0.0, 0.0, 0, 0,
                json.dumps({'error': error_message}), datetime.now()
            ))
            
            conn.commit()
            return True
        except Exception as e:
            print(f"Error saving error result: {e}")
            return False
        finally:
            conn.close()
    
    def get_audit_summary(self) -> Dict:
        """Get audit summary statistics.
        
        Returns:
            Dictionary with summary statistics.
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Get total results with better error handling
            cursor.execute("SELECT COUNT(*) FROM ytd_audit_results")
            result = cursor.fetchone()
            total_results = int(result[0]) if result and result[0] is not None else 0
            
            # Get total batch runs with better error handling
            cursor.execute("SELECT COUNT(*) FROM ytd_batch_audit_runs")
            result = cursor.fetchone()
            total_batch_runs = int(result[0]) if result and result[0] is not None else 0
            
            # Get recent batches and convert to dictionaries
            cursor.execute("""
                SELECT id, run_name, status, total_invoices, invoices_passed, 
                       invoices_warned, invoices_failed, invoices_error, created_at
                FROM ytd_batch_audit_runs 
                ORDER BY created_at DESC 
                LIMIT 10
            """)
            recent_batches_raw = cursor.fetchall()
            
            # Convert tuples to dictionaries with better null handling
            recent_batches = []
            for batch in recent_batches_raw:
                recent_batches.append({
                    'id': int(batch[0]) if batch[0] is not None else 0,
                    'run_name': str(batch[1]) if batch[1] is not None else 'Unknown',
                    'status': str(batch[2]) if batch[2] is not None else 'unknown',
                    'total_invoices': int(batch[3]) if batch[3] is not None else 0,
                    'invoices_passed': int(batch[4]) if batch[4] is not None else 0,
                    'invoices_warned': int(batch[5]) if batch[5] is not None else 0,
                    'invoices_failed': int(batch[6]) if batch[6] is not None else 0,
                    'invoices_error': int(batch[7]) if batch[7] is not None else 0,
                    'created_at': str(batch[8]) if batch[8] is not None else ''
                })
            
            # Get status counts with better error handling and status mapping
            cursor.execute("""
                SELECT audit_status, COUNT(*) 
                FROM ytd_audit_results 
                GROUP BY audit_status
            """)
            status_counts_raw = cursor.fetchall()
            status_counts = {}
            
            # Map database statuses to display statuses
            status_mapping = {
                'approved': 'APPROVED',
                'review_required': 'REVIEW_REQUIRED', 
                'rejected': 'REJECTED',
                'error': 'ERROR',
                'pass': 'APPROVED',  # Legacy status mapping
                'warning': 'REVIEW_REQUIRED',  # Legacy status mapping
                'fail': 'REJECTED'  # Legacy status mapping
            }
            
            for status, count in status_counts_raw:
                if status and count is not None:
                    # Map to display status, defaulting to original if not found
                    display_status = status_mapping.get(str(status).lower(), str(status).upper())
                    
                    # Aggregate counts for mapped statuses
                    if display_status in status_counts:
                        status_counts[display_status] += int(count)
                    else:
                        status_counts[display_status] = int(count)
            
            # Get latest batch statistics for dashboard
            latest_batch_stats = {}
            if recent_batches:
                latest_batch = recent_batches[0]  # First batch is the most recent
                latest_batch_stats = {
                    'latest_total': latest_batch.get('total_invoices', 0),
                    'latest_passed': latest_batch.get('invoices_passed', 0),
                    'latest_warned': latest_batch.get('invoices_warned', 0),
                    'latest_failed': latest_batch.get('invoices_failed', 0),
                    'latest_error': latest_batch.get('invoices_error', 0),
                    'latest_batch_name': latest_batch.get('run_name', 'No recent batch'),
                    'latest_created_at': latest_batch.get('created_at', '')
                }
            
            return {
                'total_results': total_results,
                'total_batch_runs': total_batch_runs,
                'recent_batches': recent_batches,
                'status_counts': status_counts,
                'latest_batch_stats': latest_batch_stats
            }
            
        except Exception as e:
            print(f"Error getting audit summary: {e}")
            import traceback
            print(traceback.format_exc())
            return {
                'total_results': 0,
                'total_batch_runs': 0,
                'recent_batches': [],
                'status_counts': {}
            }
        finally:
            conn.close()
            
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
            cursor.execute(
                "DELETE FROM ytd_audit_results WHERE batch_run_id = ?",
                (batch_id,)
            )
            
            # Delete the batch run
            cursor.execute(
                "DELETE FROM ytd_batch_audit_runs WHERE id = ?",
                (batch_id,)
            )
            
            # Commit the changes
            conn.commit()
            return True
        except Exception as e:
            print(f"Error deleting batch {batch_id}: {str(e)}")
            conn.rollback()
            return False
        finally:
            conn.close()
