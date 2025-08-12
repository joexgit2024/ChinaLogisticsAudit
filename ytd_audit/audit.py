"""
Audit engines interface for YTD Audit System.
"""

import time
import importlib
from typing import Dict, List, Tuple, Any, Optional, Union

# Import the AuditResult class from results module
from ytd_audit.results import AuditResult


class BaseAuditEngine:
    """Base class for all audit engines."""
    
    def __init__(self):
        """Initialize the base audit engine."""
        self.name = "base"
        self.supported_modes = []
        
    def can_audit(self, transportation_mode: str) -> bool:
        """Check if this engine can audit the given transportation mode.
        
        Args:
            transportation_mode: The transportation mode to check.
            
        Returns:
            True if this engine supports the mode, False otherwise.
        """
        return transportation_mode.lower() in [mode.lower() for mode in self.supported_modes]
        
    def audit_invoice(self, invoice_data: Dict) -> AuditResult:
        """Audit an invoice using this engine.
        
        Args:
            invoice_data: Dictionary containing invoice data.
            
        Returns:
            An AuditResult with the audit results.
        """
        # This is a base method that should be overridden by subclasses
        result = AuditResult(
            invoice_data.get("invoice_no", "Unknown"),
            invoice_data.get("transportation_mode", "unknown")
        )
        result.set_status("error")
        result.set_details({
            "error": "Base audit method called, not implemented for this engine"
        })
        return result


class ExpressAuditEngine(BaseAuditEngine):
    """Audit engine for DHL Express shipments."""
    
    def __init__(self):
        """Initialize the express audit engine."""
        super().__init__()
        self.name = "express"
        self.supported_modes = ["express", "dhl_express"]
        
        # Dynamically import DHLExpressAuditEngine to avoid circular imports
        try:
            express_module = importlib.import_module("dhl_express_audit_engine")
            self.engine = express_module.DHLExpressAuditEngine()
        except ImportError:
            print("Warning: DHLExpressAuditEngine not found")
            self.engine = None
        
    def audit_invoice(self, invoice_data: Dict) -> AuditResult:
        """Audit a DHL Express invoice.
        
        Args:
            invoice_data: Dictionary containing invoice data.
            
        Returns:
            An AuditResult with the audit results.
        """
        start_time = time.time()
        invoice_no = invoice_data.get("invoice_no", "Unknown")
        
        result = AuditResult(invoice_no, "express")
        
        if not self.engine:
            result.set_status("error")
            result.set_details({
                "error": "Express audit engine not available"
            })
            return result
            
        try:
            # Get invoice details from database if needed
            # [Implementation specific to how invoice data is stored]
            
            # Run the audit using the DHL Express engine
            audit_response = self.engine.audit_invoice(invoice_no)
            
            # Process the response
            if audit_response.get("status") == "success":
                # Extract amounts
                invoice_amount = audit_response.get("invoice_amount", 0)
                expected_amount = audit_response.get("expected_amount", 0)
                result.set_amounts(invoice_amount, expected_amount)
                
                # Set matching info
                result.set_matching_info(
                    audit_response.get("rate_cards_checked", 0),
                    audit_response.get("matching_lanes", 0),
                    audit_response.get("best_match_rate_card", "")
                )
                
                # Set details and determine status
                result.set_details(audit_response.get("details", {}))
                result.determine_status()
            else:
                # Handle error
                result.set_status("error")
                result.set_details({
                    "error": audit_response.get("error", "Unknown error"),
                    "details": audit_response.get("details", {})
                })
                
        except Exception as e:
            # Handle exceptions
            result.set_status("error")
            result.set_details({
                "error": str(e)
            })
            
        # Set processing time
        end_time = time.time()
        result.set_processing_time(int((end_time - start_time) * 1000))
        
        return result


class OceanAuditEngine(BaseAuditEngine):
    """Audit engine for Ocean freight shipments."""
    
    def __init__(self):
        """Initialize the ocean audit engine."""
        super().__init__()
        self.name = "ocean"
        self.supported_modes = ["ocean", "ocean_freight", "sea"]
        
        # Dynamically import the ocean audit engine if available
        try:
            ocean_module = importlib.import_module("ocean_freight_audit_engine")
            self.engine = ocean_module.OceanFreightAuditEngine()
        except ImportError:
            print("Warning: OceanFreightAuditEngine not found")
            self.engine = None
        
    def audit_invoice(self, invoice_data: Dict) -> AuditResult:
        """Audit an Ocean freight invoice.
        
        Args:
            invoice_data: Dictionary containing invoice data.
            
        Returns:
            An AuditResult with the audit results.
        """
        start_time = time.time()
        invoice_no = invoice_data.get("invoice_no", "Unknown")
        
        result = AuditResult(invoice_no, "ocean")
        
        if not self.engine:
            result.set_status("error")
            result.set_details({
                "error": "Ocean audit engine not available"
            })
            return result
            
        try:
            # Run the audit using the Ocean engine
            audit_response = self.engine.audit_invoice(invoice_no)
            
            # Process the response
            if audit_response.get("status") == "success":
                # Extract amounts
                invoice_amount = audit_response.get("invoice_amount", 0)
                expected_amount = audit_response.get("expected_amount", 0)
                result.set_amounts(invoice_amount, expected_amount)
                
                # Set matching info
                result.set_matching_info(
                    audit_response.get("rate_cards_checked", 0),
                    audit_response.get("matching_lanes", 0),
                    audit_response.get("best_match_rate_card", "")
                )
                
                # Set details and determine status
                result.set_details(audit_response.get("details", {}))
                result.determine_status()
            else:
                # Handle error
                result.set_status("error")
                result.set_details({
                    "error": audit_response.get("error", "Unknown error"),
                    "details": audit_response.get("details", {})
                })
                
        except Exception as e:
            # Handle exceptions
            result.set_status("error")
            result.set_details({
                "error": str(e)
            })
            
        # Set processing time
        end_time = time.time()
        result.set_processing_time(int((end_time - start_time) * 1000))
        
        return result


class AirAuditEngine(BaseAuditEngine):
    """Audit engine for Air freight shipments."""
    
    def __init__(self):
        """Initialize the air audit engine."""
        super().__init__()
        self.name = "air"
        self.supported_modes = ["air", "air_freight"]
        
        # Dynamically import the air audit engine if available
        try:
            air_module = importlib.import_module("air_freight_audit_engine")
            self.engine = air_module.AirFreightAuditEngine()
        except ImportError:
            print("Warning: AirFreightAuditEngine not found")
            self.engine = None
        
    def audit_invoice(self, invoice_data: Dict) -> AuditResult:
        """Audit an Air freight invoice.
        
        Args:
            invoice_data: Dictionary containing invoice data.
            
        Returns:
            An AuditResult with the audit results.
        """
        start_time = time.time()
        invoice_no = invoice_data.get("invoice_no", "Unknown")
        
        result = AuditResult(invoice_no, "air")
        
        if not self.engine:
            result.set_status("error")
            result.set_details({
                "error": "Air audit engine not available"
            })
            return result
            
        try:
            # Run the audit using the Air engine
            audit_response = self.engine.audit_invoice(invoice_no)
            
            # Process the response
            if audit_response.get("status") == "success":
                # Extract amounts
                invoice_amount = audit_response.get("invoice_amount", 0)
                expected_amount = audit_response.get("expected_amount", 0)
                result.set_amounts(invoice_amount, expected_amount)
                
                # Set matching info
                result.set_matching_info(
                    audit_response.get("rate_cards_checked", 0),
                    audit_response.get("matching_lanes", 0),
                    audit_response.get("best_match_rate_card", "")
                )
                
                # Set details and determine status
                result.set_details(audit_response.get("details", {}))
                result.determine_status()
            else:
                # Handle error
                result.set_status("error")
                result.set_details({
                    "error": audit_response.get("error", "Unknown error"),
                    "details": audit_response.get("details", {})
                })
                
        except Exception as e:
            # Handle exceptions
            result.set_status("error")
            result.set_details({
                "error": str(e)
            })
            
        # Set processing time
        end_time = time.time()
        result.set_processing_time(int((end_time - start_time) * 1000))
        
        return result


class AuDomesticAuditEngine(BaseAuditEngine):
    """Audit engine for Australia Domestic shipments."""
    
    def __init__(self):
        """Initialize the AU Domestic audit engine."""
        super().__init__()
        self.name = "au_domestic"
        self.supported_modes = ["au_domestic", "australia_domestic"]
        
        # Dynamically import the AU Domestic audit engine if available
        try:
            au_module = importlib.import_module("au_domestic_audit_engine")
            self.engine = au_module.AUDomesticAuditEngine()
        except ImportError:
            print("Warning: AUDomesticAuditEngine not found")
            self.engine = None
        
    def audit_invoice(self, invoice_data: Dict) -> AuditResult:
        """Audit an AU Domestic invoice.
        
        Args:
            invoice_data: Dictionary containing invoice data.
            
        Returns:
            An AuditResult with the audit results.
        """
        start_time = time.time()
        invoice_no = invoice_data.get("invoice_no", "Unknown")
        
        result = AuditResult(invoice_no, "au_domestic")
        
        if not self.engine:
            result.set_status("error")
            result.set_details({
                "error": "AU Domestic audit engine not available"
            })
            return result
            
        try:
            # Run the audit using the AU Domestic engine
            audit_response = self.engine.audit_invoice(invoice_no)
            
            # Process the response
            if audit_response.get("status") == "success":
                # Extract amounts
                invoice_amount = audit_response.get("invoice_amount", 0)
                expected_amount = audit_response.get("expected_amount", 0)
                result.set_amounts(invoice_amount, expected_amount)
                
                # Set matching info
                result.set_matching_info(
                    audit_response.get("rate_cards_checked", 0),
                    audit_response.get("matching_lanes", 0),
                    audit_response.get("best_match_rate_card", "")
                )
                
                # Set details and determine status
                result.set_details(audit_response.get("details", {}))
                result.determine_status()
            else:
                # Handle error
                result.set_status("error")
                result.set_details({
                    "error": audit_response.get("error", "Unknown error"),
                    "details": audit_response.get("details", {})
                })
                
        except Exception as e:
            # Handle exceptions
            result.set_status("error")
            result.set_details({
                "error": str(e)
            })
            
        # Set processing time
        end_time = time.time()
        result.set_processing_time(int((end_time - start_time) * 1000))
        
        return result


class AuditEngineFactory:
    """Factory for creating appropriate audit engines."""
    
    @staticmethod
    def get_engine(transportation_mode: str) -> BaseAuditEngine:
        """Get the appropriate audit engine for a transportation mode.
        
        Args:
            transportation_mode: The transportation mode for the invoice.
            
        Returns:
            An appropriate audit engine for the mode.
        """
        # Normalize the mode
        mode = transportation_mode.lower() if transportation_mode else "unknown"
        
        # Check modes in priority order and create only the needed engine
        
        # Check DHL Express first
        if mode in ['express', 'dhl_express']:
            return ExpressAuditEngine()
            
        # Check Ocean freight
        if mode in ['ocean', 'ocean_freight', 'sea']:
            return OceanAuditEngine()
            
        # Check Air freight 
        if mode in ['air', 'air_freight']:
            return AirAuditEngine()
            
        # Check AU Domestic
        if mode in ['au_domestic', 'australia_domestic']:
            return AuDomesticAuditEngine()
        
        # Default fallback to Express
        return ExpressAuditEngine()
