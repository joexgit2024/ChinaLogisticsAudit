#!/usr/bin/env python3
"""
DHL Express Audit Engine - Refactored Version

This file now serves as a backward compatibility layer, importing the main 
DHLExpressAuditEngine class from the refactored core module.

The original large file has been split into multiple modules for better maintainability:
- dhl_express_audit_core.py: Main audit engine class
- dhl_express_audit_constants.py: Constants and mappings  
- dhl_express_audit_utils.py: Utility functions
- dhl_express_audit_service_charges.py: Service charge calculations

This ensures all existing imports and references continue to work unchanged.
"""

# Import the main class from the core module
from dhl_express_audit_core import DHLExpressAuditEngine

# Re-export for backward compatibility
__all__ = ['DHLExpressAuditEngine']
