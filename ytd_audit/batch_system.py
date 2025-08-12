"""
YTD Batch Audit System - Core batch processing module.
"""

import time
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from ytd_audit.database import DatabaseManager
from ytd_audit.results import AuditResult, BatchAuditResults
from .audit import AuditEngineFactory, AuditResult
from updated_ytd_audit_engine import UpdatedYTDAuditEngine


class YTDBatchAuditSystem:
    """Core YTD Batch Audit System for processing invoices in batch."""
    
    def __init__(self, db_path: str = "dhl_audit.db"):
        """Initialize the YTD Batch Audit System.
        
        Args:
            db_path: Path to the SQLite database file.
        """
        self.db = DatabaseManager(db_path)
        self.batch_results = None
        
    def run_full_ytd_audit(self, batch_name: Optional[str] = None, 
                          force_reaudit: bool = False,
                          detailed_analysis: bool = False) -> Dict:
        """Run a full audit on all YTD invoices.
        
        Args:
            batch_name: Optional name for this batch run. Defaults to timestamp.
            force_reaudit: Whether to delete existing results and re-audit all invoices.
            detailed_analysis: Whether to include detailed variance analysis.
            
        Returns:
            Dictionary with audit results and statistics.
        """
        # Create batch name if not provided
        if not batch_name:
            batch_name = f"YTD_Audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
        # Create batch run in database
        batch_run_id = self.db.create_batch_run(batch_name)
        
        # Handle force re-audit if requested
        if force_reaudit:
            try:
                print("Force re-audit requested - deleting existing audit results...")
                # Get all YTD invoice numbers first
                invoices = self.db.get_all_ytd_invoices()
                invoice_numbers = [inv[0] for inv in invoices]
                
                if invoice_numbers:
                    # Delete existing audit results for these invoices
                    placeholders = ','.join(['?' for _ in invoice_numbers])
                    conn = self.db.get_connection()
                    cursor = conn.cursor()
                    cursor.execute(f"DELETE FROM ytd_audit_results WHERE invoice_no IN ({placeholders})", invoice_numbers)
                    deleted_count = cursor.rowcount
                    conn.commit()
                    conn.close()
                    print(f"Deleted {deleted_count} existing audit results")
            except Exception as e:
                print(f"Warning: Could not delete existing results: {e}")
        
        # Initialize batch results
        self.batch_results = BatchAuditResults(batch_run_id, batch_name)
        
        try:
            # Get all YTD invoices from database
            invoices = self.db.get_all_ytd_invoices()
            total_invoices = len(invoices)
            
            print(f"Starting batch audit of {total_invoices} invoices")
            
            # Process each invoice
            for idx, invoice in enumerate(invoices):
                try:
                    # Extract invoice data
                    invoice_no = invoice[0]
                    transportation_mode = invoice[1] if len(invoice) > 1 else "unknown"
                    invoice_amount = float(invoice[2]) if len(invoice) > 2 else 0.0
                    
                    # Show progress
                    print(f"Processing invoice {idx+1}/{total_invoices}: {invoice_no}")
                    
                    # Create invoice data dictionary
                    invoice_data = {
                        "invoice_no": invoice_no,
                        "transportation_mode": transportation_mode,
                        "total_amount": invoice_amount
                    }
                    
                    # Add origin/destination if available
                    if len(invoice) > 3:
                        invoice_data["origin"] = invoice[3]
                    if len(invoice) > 4:
                        invoice_data["destination"] = invoice[4]
                    
                    # Add weight if available
                    if len(invoice) > 5:
                        invoice_data["weight"] = float(invoice[5])
                        
                    # Add service type if available
                    if len(invoice) > 6:
                        invoice_data["service_type"] = invoice[6]
                    
                    # Audit the invoice
                    result = self.audit_single_invoice_comprehensive(invoice_data)
                    
                    # Add result to batch
                    self.batch_results.add_result(result)
                    
                    # Save result to database
                    self.db.save_audit_result(
                        batch_run_id,
                        result.invoice_no,
                        result.audit_status,
                        result.transportation_mode,
                        result.total_invoice_amount,
                        result.total_expected_amount,
                        result.total_variance,
                        result.variance_percent,
                        result.rate_cards_checked,
                        result.matching_lanes,
                        result.best_match_rate_card,
                        result.audit_details
                    )
                    
                except Exception as e:
                    # Handle individual invoice errors
                    print(f"Error auditing invoice {invoice[0] if invoice else 'Unknown'}: {e}")
                    error_result = AuditResult(invoice[0] if invoice else "Unknown")
                    error_result.set_status("error")
                    error_result.set_details({"error": str(e)})
                    
                    # Add error result to batch
                    self.batch_results.add_result(error_result)
                    
                    # Save error result to database
                    self.db.save_error_result(batch_run_id, invoice[0] if invoice else "Unknown", str(e))
                    
                    # Continue with next invoice
                    continue
                    
            # Complete batch and save statistics
            self.batch_results.complete()
            self.db.update_batch_run(
                batch_run_id,
                "completed",
                self.batch_results.total_invoices,
                self.batch_results.invoices_passed,
                self.batch_results.invoices_warned,
                self.batch_results.invoices_failed,
                self.batch_results.invoices_error,
                self.batch_results.processing_time_ms
            )
            
            return self.batch_results.get_statistics()
            
        except Exception as e:
            # Handle batch-level errors
            print(f"Error running batch audit: {e}")
            print(traceback.format_exc())
            
            # Update batch status to error
            if self.batch_results:
                self.batch_results.status = "error"
                self.batch_results.complete()
                
            self.db.update_batch_run(
                batch_run_id,
                "error",
                self.batch_results.total_invoices if self.batch_results else 0,
                self.batch_results.invoices_passed if self.batch_results else 0,
                self.batch_results.invoices_warned if self.batch_results else 0,
                self.batch_results.invoices_failed if self.batch_results else 0,
                self.batch_results.invoices_error if self.batch_results else 0,
                self.batch_results.processing_time_ms if self.batch_results else 0
            )
            
            return {
                "status": "error",
                "error": str(e),
                "batch_run_id": batch_run_id
            }
    
    def run_batch_audit(self, invoice_numbers: List[str], 
                      batch_name: Optional[str] = None) -> Dict:
        """Run an audit on a specific batch of invoices.
        
        Args:
            invoice_numbers: List of invoice numbers to audit.
            batch_name: Optional name for this batch run. Defaults to timestamp.
            
        Returns:
            Dictionary with audit results and statistics.
        """
        # Create batch name if not provided
        if not batch_name:
            batch_name = f"Batch_Audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
        # Create batch run in database
        batch_run_id = self.db.create_batch_run(batch_name)
        
        # Initialize batch results
        self.batch_results = BatchAuditResults(batch_run_id, batch_name)
        
        try:
            total_invoices = len(invoice_numbers)
            print(f"Starting batch audit of {total_invoices} invoices")
            
            # Process each invoice
            for idx, invoice_no in enumerate(invoice_numbers):
                try:
                    # Show progress
                    print(f"Processing invoice {idx+1}/{total_invoices}: {invoice_no}")
                    
                    # Create basic invoice data
                    invoice_data = {"invoice_no": invoice_no}
                    
                    # Audit the invoice
                    result = self.audit_single_invoice_comprehensive(invoice_data)
                    
                    # Add result to batch
                    self.batch_results.add_result(result)
                    
                    # Save result to database
                    self.db.save_audit_result(
                        batch_run_id,
                        result.invoice_no,
                        result.audit_status,
                        result.transportation_mode,
                        result.total_invoice_amount,
                        result.total_expected_amount,
                        result.total_variance,
                        result.variance_percent,
                        result.rate_cards_checked,
                        result.matching_lanes,
                        result.best_match_rate_card,
                        result.audit_details
                    )
                    
                except Exception as e:
                    # Handle individual invoice errors
                    print(f"Error auditing invoice {invoice_no}: {e}")
                    error_result = AuditResult(invoice_no)
                    error_result.set_status("error")
                    error_result.set_details({"error": str(e)})
                    
                    # Add error result to batch
                    self.batch_results.add_result(error_result)
                    
                    # Save error result to database
                    self.db.save_error_result(batch_run_id, invoice_no, str(e))
                    
                    # Continue with next invoice
                    continue
                    
            # Complete batch and save statistics
            self.batch_results.complete()
            self.db.update_batch_run(
                batch_run_id,
                "completed",
                self.batch_results.total_invoices,
                self.batch_results.invoices_passed,
                self.batch_results.invoices_warned,
                self.batch_results.invoices_failed,
                self.batch_results.invoices_error,
                self.batch_results.processing_time_ms
            )
            
            return self.batch_results.get_statistics()
            
        except Exception as e:
            # Handle batch-level errors
            print(f"Error running batch audit: {e}")
            print(traceback.format_exc())
            
            # Update batch status to error
            if self.batch_results:
                self.batch_results.status = "error"
                self.batch_results.complete()
                
            self.db.update_batch_run(
                batch_run_id,
                "error",
                self.batch_results.total_invoices if self.batch_results else 0,
                self.batch_results.invoices_passed if self.batch_results else 0,
                self.batch_results.invoices_warned if self.batch_results else 0,
                self.batch_results.invoices_failed if self.batch_results else 0,
                self.batch_results.invoices_error if self.batch_results else 0,
                self.batch_results.processing_time_ms if self.batch_results else 0
            )
            
            return {
                "status": "error",
                "error": str(e),
                "batch_run_id": batch_run_id
            }
            
    def audit_single_invoice_comprehensive(self, invoice_data: Dict) -> AuditResult:
        """Audit a single invoice comprehensively using the existing proven audit engine.
        
        Args:
            invoice_data: Dictionary containing invoice data.
            
        Returns:
            An AuditResult with the audit results.
        """
        start_time = time.time()
        invoice_no = invoice_data.get("invoice_no", "Unknown")
        transportation_mode = invoice_data.get("transportation_mode", "unknown")
        
        try:
            # Use the existing proven UpdatedYTDAuditEngine
            audit_engine = UpdatedYTDAuditEngine()
            
            # Run the audit using the existing engine's audit_invoice method (takes invoice_no)
            result = audit_engine.audit_invoice(invoice_no)
            
            # Convert to AuditResult format
            audit_result = AuditResult(invoice_no, transportation_mode)
            
            # Map the existing engine's status to the expected batch system status
            if result and not result.get('error'):
                summary = result.get('summary', {})
                overall_status = summary.get('overall_status', 'NO_RATES_FOUND')
                rate_cards_checked = summary.get('total_rate_cards_checked', 0)
                best_match = summary.get('best_match')
                
                # Extract financial amounts from the audit result
                if best_match:
                    total_actual = best_match.get('total_actual', 0)
                    total_expected = best_match.get('total_expected', 0)
                    audit_result.set_amounts(total_actual, total_expected)
                    
                    # Set rate card matching info
                    audit_result.set_matching_info(
                        rate_cards_checked=rate_cards_checked,
                        matching_lanes=1 if best_match else 0,
                        best_match_rate_card=best_match.get('rate_card_info', {}).get('card_name', '')
                    )
                
                if overall_status == 'PASS' and rate_cards_checked > 0:
                    audit_result.set_status("approved")  # Maps to invoices_passed
                elif overall_status in ['VARIANCE_FOUND', 'REVIEW'] and rate_cards_checked > 0:
                    audit_result.set_status("review_required")  # Maps to invoices_warned  
                elif overall_status == 'FAIL' and rate_cards_checked > 0:
                    audit_result.set_status("rejected")  # Maps to invoices_failed
                else:
                    # No rate cards found - use the exact status name the UI expects
                    audit_result.set_status("No Rate Card")  # For UI filtering
                    
                audit_result.set_details(result)
            elif result and result.get('error'):
                audit_result.set_status("error")
                audit_result.set_details(result)
            else:
                audit_result.set_status("No Rate Card")  # For UI filtering
                audit_result.set_details({"error": "No audit result returned"})
            
            # Set processing time
            end_time = time.time()
            audit_result.set_processing_time(int((end_time - start_time) * 1000))
            
            return audit_result
            
        except Exception as e:
            # Handle exceptions
            error_result = AuditResult(invoice_no, transportation_mode)
            error_result.set_status("error")
            error_result.set_details({"error": str(e)})
            
            # Set processing time
            end_time = time.time()
            error_result.set_processing_time(int((end_time - start_time) * 1000))
            
            return error_result
            
    def audit_single_invoice(self, invoice_no: str) -> Dict:
        """Audit a single invoice and return results as a dictionary.
        
        Args:
            invoice_no: The invoice number to audit.
            
        Returns:
            Dictionary with audit results.
        """
        # Create basic invoice data
        invoice_data = {"invoice_no": invoice_no}
        
        # Run comprehensive audit
        result = self.audit_single_invoice_comprehensive(invoice_data)
        
        # Return as dictionary
        return result.to_dict()
        
    def get_audit_summary(self) -> Dict:
        """Get summary statistics for all audits.
        
        Returns:
            Dictionary with summary statistics.
        """
        return self.db.get_audit_summary()
        
    def get_existing_audit_results(self, invoice_no: str) -> List[Dict]:
        """Get existing audit results for an invoice.
        
        Args:
            invoice_no: The invoice number to get results for.
            
        Returns:
            List of dictionaries with audit results.
        """
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT id, batch_run_id, audit_status, transportation_mode,
                       total_invoice_amount, total_expected_amount, total_variance,
                       variance_percent, audit_details, created_at
                FROM ytd_audit_results 
                WHERE invoice_no = ?
                ORDER BY created_at DESC
            """, (invoice_no,))
            
            results = cursor.fetchall()
            
            audit_results = []
            for row in results:
                audit_results.append({
                    "id": row[0],
                    "batch_run_id": row[1],
                    "audit_status": row[2],
                    "transportation_mode": row[3],
                    "total_invoice_amount": row[4],
                    "total_expected_amount": row[5],
                    "total_variance": row[6],
                    "variance_percent": row[7],
                    "audit_details": row[8],
                    "created_at": row[9]
                })
                
            return audit_results
            
        except Exception as e:
            print(f"Error getting existing audit results: {e}")
            return []
        finally:
            conn.close()
            
    def delete_batch(self, batch_id: int) -> bool:
        """Delete a batch run and all associated audit results.
        
        Args:
            batch_id: The ID of the batch run to delete.
            
        Returns:
            True if deletion was successful, False otherwise.
        """
        return self.db.delete_batch(batch_id)
