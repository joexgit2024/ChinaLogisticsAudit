"""
Results processing for YTD Audit System.
"""

import json
from typing import Dict, List, Optional
from datetime import datetime


class AuditResult:
    """Represents the result of a single invoice audit."""
    
    def __init__(self, invoice_no: str, transportation_mode: str = "unknown"):
        """Initialize an audit result.
        
        Args:
            invoice_no: The invoice number being audited.
            transportation_mode: The transportation mode of the invoice.
        """
        self.invoice_no = invoice_no
        self.transportation_mode = transportation_mode
        self.audit_status = "pending"
        self.total_invoice_amount = 0.0
        self.total_expected_amount = 0.0
        self.total_variance = 0.0
        self.variance_percent = 0.0
        self.rate_cards_checked = 0
        self.matching_lanes = 0
        self.best_match_rate_card = ""
        self.audit_details = {}
        self.processing_time_ms = 0
        self.created_at = datetime.now()
        self.line_items = []
        
    def set_status(self, status: str) -> None:
        """Set the audit status.
        
        Args:
            status: The audit status (approved, review_required, rejected, error).
        """
        self.audit_status = status
        
    def add_line_item(self, line_item: Dict) -> None:
        """Add a line item audit result.
        
        Args:
            line_item: Dictionary containing line item audit details.
        """
        self.line_items.append(line_item)
        
    def set_amounts(self, invoice_amount: float, expected_amount: float) -> None:
        """Set the invoice and expected amounts and calculate variance.
        
        Args:
            invoice_amount: The total invoice amount.
            expected_amount: The total expected amount from rate cards.
        """
        self.total_invoice_amount = invoice_amount
        self.total_expected_amount = expected_amount
        self.total_variance = invoice_amount - expected_amount
        
        # Calculate variance percentage with safety check for division by zero
        if expected_amount != 0:
            self.variance_percent = (self.total_variance / expected_amount) * 100
        else:
            self.variance_percent = 0 if self.total_variance == 0 else 100
            
    def set_matching_info(self, rate_cards_checked: int, matching_lanes: int, 
                          best_match_rate_card: str) -> None:
        """Set information about rate card matching.
        
        Args:
            rate_cards_checked: Number of rate cards checked.
            matching_lanes: Number of matching lanes found.
            best_match_rate_card: Description of best matching rate card.
        """
        self.rate_cards_checked = rate_cards_checked
        self.matching_lanes = matching_lanes
        self.best_match_rate_card = best_match_rate_card
        
    def set_processing_time(self, time_ms: int) -> None:
        """Set the processing time for this audit.
        
        Args:
            time_ms: Processing time in milliseconds.
        """
        self.processing_time_ms = time_ms
        
    def set_details(self, audit_details: Dict) -> None:
        """Set detailed audit information.
        
        Args:
            audit_details: Dictionary with detailed audit results.
        """
        self.audit_details = audit_details
        
    def determine_status(self, threshold: float = 5.0) -> str:
        """Determine audit status based on variance.
        
        Args:
            threshold: Percentage threshold for warnings.
            
        Returns:
            The determined audit status.
        """
        if abs(self.variance_percent) <= 0.1:
            self.audit_status = "approved"
        elif abs(self.variance_percent) <= threshold:
            self.audit_status = "review_required"
        else:
            self.audit_status = "rejected"
            
        return self.audit_status
        
    def to_dict(self) -> Dict:
        """Convert result to a dictionary.
        
        Returns:
            Dictionary representation of this audit result.
        """
        return {
            "invoice_no": self.invoice_no,
            "transportation_mode": self.transportation_mode,
            "audit_status": self.audit_status,
            "total_invoice_amount": self.total_invoice_amount,
            "total_expected_amount": self.total_expected_amount,
            "total_variance": self.total_variance,
            "variance_percent": self.variance_percent,
            "rate_cards_checked": self.rate_cards_checked,
            "matching_lanes": self.matching_lanes,
            "best_match_rate_card": self.best_match_rate_card,
            "audit_details": self.audit_details,
            "processing_time_ms": self.processing_time_ms,
            "line_items": self.line_items,
            "created_at": self.created_at.isoformat()
        }
        
    def to_json(self) -> str:
        """Convert result to JSON string.
        
        Returns:
            JSON string representation of this audit result.
        """
        return json.dumps(self.to_dict())


class BatchAuditResults:
    """Manages results for a batch audit run."""
    
    def __init__(self, batch_run_id: int, batch_name: str):
        """Initialize batch audit results.
        
        Args:
            batch_run_id: The database ID of this batch run.
            batch_name: The name of this batch run.
        """
        self.batch_run_id = batch_run_id
        self.batch_name = batch_name
        self.status = "running"
        self.total_invoices = 0
        self.invoices_passed = 0
        self.invoices_warned = 0
        self.invoices_failed = 0
        self.invoices_error = 0
        self.results = []
        self.start_time = datetime.now()
        self.end_time = None
        self.processing_time_ms = 0
        
    def add_result(self, result: AuditResult) -> None:
        """Add an audit result to this batch.
        
        Args:
            result: The audit result to add.
        """
        self.results.append(result)
        self.total_invoices += 1
        
        # Update counters based on status
        if result.audit_status == "approved":
            self.invoices_passed += 1
        elif result.audit_status == "review_required":
            self.invoices_warned += 1
        elif result.audit_status == "rejected":
            self.invoices_failed += 1
        elif result.audit_status in ["error", "No Rate Card"]:
            self.invoices_error += 1
            
    def complete(self) -> None:
        """Mark the batch as complete and calculate processing time."""
        self.end_time = datetime.now()
        self.status = "completed"
        
        # Calculate total processing time
        delta = self.end_time - self.start_time
        self.processing_time_ms = int(delta.total_seconds() * 1000)
        
    def get_statistics(self) -> Dict:
        """Get batch statistics.
        
        Returns:
            Dictionary with batch statistics.
        """
        return {
            "batch_run_id": self.batch_run_id,
            "batch_name": self.batch_name,
            "status": self.status,
            "total_invoices": self.total_invoices,
            "invoices_passed": self.invoices_passed,
            "invoices_warned": self.invoices_warned,
            "invoices_failed": self.invoices_failed,
            "invoices_error": self.invoices_error,
            "processing_time_ms": self.processing_time_ms,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None
        }
        
    def get_results_json(self) -> str:
        """Get all results as a JSON string.
        
        Returns:
            JSON string with all results.
        """
        return json.dumps([r.to_dict() for r in self.results])
