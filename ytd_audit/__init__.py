"""
YTD Audit System Package.

This package provides functionality for auditing DHL invoices.
"""

# Import core classes for easy access
from ytd_audit.database import DatabaseManager
from ytd_audit.batch_system import YTDBatchAuditSystem
from ytd_audit.results import AuditResult, BatchAuditResults
from ytd_audit.audit import (
    BaseAuditEngine, 
    ExpressAuditEngine,
    OceanAuditEngine,
    AirAuditEngine,
    AuDomesticAuditEngine,
    AuditEngineFactory
)

# Define package exports
__all__ = [
    'DatabaseManager',
    'YTDBatchAuditSystem',
    'AuditResult',
    'BatchAuditResults',
    'BaseAuditEngine',
    'ExpressAuditEngine',
    'OceanAuditEngine',
    'AirAuditEngine', 
    'AuDomesticAuditEngine',
    'AuditEngineFactory'
]
