"""
Core Data Models for DHL Invoice Audit Application

Overall Purpose:
----------------
Defines the main data models (Invoice, Charge, Shipment, AuditRule, etc.) and
validation logic used throughout the DHL Invoice Audit Application. These
models provide structured representations for invoice data, charges, shipments, and audit
rules, and are used for data validation, business logic, and database operations.

Where This File is Used:
------------------------
- Imported by Flask route modules (e.g., invoice, audit, and upload routes)
- Used by audit engines and validation utilities for processing and validating
    invoice and shipment data
- Referenced by other modules for business logic and data manipulation
"""
from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime
from decimal import Decimal

@dataclass
class Invoice:
    """Invoice data model."""
    id: Optional[int] = None
    invoice_number: str = ""
    shipper_name: str = ""
    shipper_address: str = ""
    consignee_name: str = ""
    consignee_address: str = ""
    origin_city: str = ""
    destination_city: str = ""
    service_date: Optional[str] = None
    delivery_date: Optional[str] = None
    reference_number: str = ""
    pro_number: str = ""
    total_charges: float = 0.0
    weight: float = 0.0
    pieces: int = 0
    audit_status: str = "pending"
    audit_notes: str = ""
    raw_edi: str = ""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@dataclass
class Charge:
    """Charge data model."""
    id: Optional[int] = None
    invoice_id: int = 0
    charge_type: str = ""
    amount: Decimal = Decimal("0.00")
    description: str = ""
    rate: Optional[Decimal] = None
    quantity: Optional[Decimal] = None
    unit: str = ""
    created_at: Optional[datetime] = None

@dataclass
class Shipment:
    """Shipment data model."""
    id: Optional[int] = None
    invoice_id: int = 0
    tracking_number: str = ""
    origin_location: str = ""
    destination_location: str = ""
    pickup_date: Optional[str] = None
    delivery_date: Optional[str] = None
    service_type: str = ""
    weight: float = 0.0
    dimensions: str = ""
    package_count: int = 0
    status: str = ""
    created_at: Optional[datetime] = None

@dataclass
class AuditRule:
    """Audit rule data model."""
    id: Optional[int] = None
    rule_name: str = ""
    rule_type: str = ""
    condition_field: str = ""
    operator: str = ""
    threshold_value: Optional[float] = None
    action: str = ""
    is_active: bool = True
    created_at: Optional[datetime] = None

class InvoiceValidator:
    """Validator class for invoice data."""
    
    @staticmethod
    def validate_invoice(invoice: Invoice) -> List[str]:
        """Validate invoice data and return list of errors."""
        errors = []
        
        if not invoice.invoice_number:
            errors.append("Invoice number is required")
        
        if not invoice.shipper_name:
            errors.append("Shipper name is required")
        
        if not invoice.consignee_name:
            errors.append("Consignee name is required")
        
        if invoice.total_charges < 0:
            errors.append("Total charges cannot be negative")
        
        if invoice.weight < 0:
            errors.append("Weight cannot be negative")
        
        if invoice.pieces < 0:
            errors.append("Pieces cannot be negative")
        
        return errors
    
    @staticmethod
    def validate_charge(charge: Charge) -> List[str]:
        """Validate charge data and return list of errors."""
        errors = []
        
        if not charge.charge_type:
            errors.append("Charge type is required")
        
        if charge.amount < 0:
            errors.append("Charge amount cannot be negative")
        
        if charge.rate is not None and charge.rate < 0:
            errors.append("Rate cannot be negative")
        
        if charge.quantity is not None and charge.quantity < 0:
            errors.append("Quantity cannot be negative")
        
        return errors

class AuditEngine:
    """Engine for performing automated audit checks."""
    
    def __init__(self):
        self.rules = []
    
    def load_rules(self, rules: List[AuditRule]):
        """Load audit rules."""
        self.rules = [rule for rule in rules if rule.is_active]
    
    def audit_invoice(self, invoice: Invoice, charges: List[Charge]) -> dict:
        """Perform audit on invoice and return results."""
        audit_result = {
            'status': 'approved',
            'flags': [],
            'score': 100,
            'recommendations': []
        }
        
        # Basic validation
        validation_errors = InvoiceValidator.validate_invoice(invoice)
        if validation_errors:
            audit_result['flags'].extend(validation_errors)
            audit_result['status'] = 'review'
            audit_result['score'] -= len(validation_errors) * 10
        
        # High value check
        if invoice.total_charges > 1000:
            audit_result['flags'].append('High value invoice requires review')
            audit_result['status'] = 'review'
            audit_result['score'] -= 15
        
        # Weight consistency check
        if invoice.weight == 0 and invoice.total_charges > 100:
            audit_result['flags'].append('Zero weight with significant charges')
            audit_result['status'] = 'review'
            audit_result['score'] -= 20
        
        # Charge breakdown analysis
        total_charge_amount = sum(charge.amount for charge in charges)
        if abs(total_charge_amount - invoice.total_charges) > 0.01:
            audit_result['flags'].append('Charge breakdown does not match total')
            audit_result['status'] = 'review'
            audit_result['score'] -= 25
        
        # Missing critical information
        if not invoice.origin_city or not invoice.destination_city:
            audit_result['flags'].append('Missing origin or destination information')
            audit_result['status'] = 'review'
            audit_result['score'] -= 10
        
        # Service date validation
        if invoice.service_date:
            try:
                service_date = datetime.strptime(invoice.service_date, '%Y-%m-%d')
                if service_date > datetime.now():
                    audit_result['flags'].append('Service date is in the future')
                    audit_result['status'] = 'review'
                    audit_result['score'] -= 15
            except:
                audit_result['flags'].append('Invalid service date format')
                audit_result['status'] = 'review'
                audit_result['score'] -= 10
        
        # Ensure score doesn't go below 0
        audit_result['score'] = max(0, audit_result['score'])
        
        # Add recommendations based on flags
        if audit_result['flags']:
            audit_result['recommendations'] = [
                'Review flagged items carefully',
                'Verify data with original documentation',
                'Contact shipper for clarification if needed'
            ]
        
        return audit_result
