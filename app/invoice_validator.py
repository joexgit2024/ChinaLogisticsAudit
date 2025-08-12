"""
Invoice Validation Engine
========================

This module provides comprehensive invoice validation logic for DHL audit system.
It performs basic field validation and business rule checks.
"""

from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
from enum import Enum
import re
from datetime import datetime

class ValidationSeverity(Enum):
    """Validation issue severity levels"""
    ERROR = "error"      # Critical - blocks processing
    WARNING = "warning"  # Important - needs attention
    INFO = "info"       # Informational - good to know

@dataclass
class ValidationIssue:
    """Single validation issue"""
    field: str
    severity: ValidationSeverity
    message: str
    current_value: Any = None
    expected_format: str = None

@dataclass
class ValidationResult:
    """Complete validation result for an invoice"""
    invoice_number: str
    is_valid: bool
    issues: List[ValidationIssue]
    score: float  # 0-100 validation score
    
    @property
    def error_count(self) -> int:
        return len([i for i in self.issues if i.severity == ValidationSeverity.ERROR])
    
    @property
    def warning_count(self) -> int:
        return len([i for i in self.issues if i.severity == ValidationSeverity.WARNING])
    
    @property
    def status_class(self) -> str:
        if self.error_count > 0:
            return "danger"
        elif self.warning_count > 0:
            return "warning"
        else:
            return "success"

class InvoiceValidator:
    """Main invoice validation engine"""
    
    # Required fields for basic invoice processing
    REQUIRED_FIELDS = {
        'invoice_number': 'Invoice Number',
        'shipper_name': 'Shipper Name',
        'consignee_name': 'Consignee Name',
        'total_charges': 'Total Charges',
        'currency': 'Currency'
    }
    
    # Location fields - at least one pair should be present
    LOCATION_FIELD_PAIRS = [
        ('origin_port', 'destination_port'),
        ('shipper_country', 'consignee_country'),
        ('shipper_city', 'consignee_city')
    ]
    
    # Important fields that should be present
    IMPORTANT_FIELDS = {
        'weight': 'Weight',
        'pieces': 'Pieces',
        'tracking_number': 'Tracking Number',
        'reference_number': 'Reference Number',
        'exchange_rate': 'Exchange Rate',
        'pickup_date': 'Pickup Date',
        'delivery_date': 'Delivery Date',
        'invoice_date': 'Invoice Date'
    }
    
    # Valid currency codes
    VALID_CURRENCIES = {'USD', 'AUD', 'EUR', 'GBP', 'CAD', 'NZD', 'SGD', 'HKD', 'JPY'}
    
    # Valid country codes (ISO 3166-1 alpha-2)
    VALID_COUNTRIES = {'AU', 'US', 'CN', 'NZ', 'GB', 'DE', 'FR', 'SG', 'HK', 'JP', 'CA', 'IN', 'TH', 'VN', 'MY'}

    def validate_invoice(self, invoice_data: Dict[str, Any]) -> ValidationResult:
        """
        Validate a single invoice and return validation result
        
        Args:
            invoice_data: Dictionary containing invoice data
            
        Returns:
            ValidationResult with all validation issues
        """
        issues = []
        invoice_number = invoice_data.get('invoice_number', 'Unknown')
        
        # 1. Check required fields
        issues.extend(self._validate_required_fields(invoice_data))
        
        # 2. Check location fields (at least one pair must be present)
        issues.extend(self._validate_location_fields(invoice_data))
        
        # 3. Check important fields (warnings)
        issues.extend(self._validate_important_fields(invoice_data))
        
        # 4. Check field formats and business rules
        issues.extend(self._validate_field_formats(invoice_data))
        
        # 5. Check business logic rules
        issues.extend(self._validate_business_rules(invoice_data))
        
        # Calculate validation score (0-100)
        score = self._calculate_validation_score(issues)
        
        # Determine if invoice is valid (no errors)
        is_valid = not any(issue.severity == ValidationSeverity.ERROR for issue in issues)
        
        return ValidationResult(
            invoice_number=invoice_number,
            is_valid=is_valid,
            issues=issues,
            score=score
        )
    
    def _validate_required_fields(self, invoice_data: Dict[str, Any]) -> List[ValidationIssue]:
        """Check that all required fields are present and not empty"""
        issues = []
        
        for field_key, field_name in self.REQUIRED_FIELDS.items():
            value = invoice_data.get(field_key)
            
            if value is None or value == '' or (isinstance(value, str) and not value.strip()):
                issues.append(ValidationIssue(
                    field=field_key,
                    severity=ValidationSeverity.ERROR,
                    message=f"{field_name} is required but missing or empty",
                    current_value=value
                ))
        
        return issues
    
    def _validate_location_fields(self, invoice_data: Dict[str, Any]) -> List[ValidationIssue]:
        """Check that at least one pair of location fields is present"""
        issues = []
        
        # Check if any location pair has both values
        valid_location_pair = False
        location_info = []
        
        for origin_field, dest_field in self.LOCATION_FIELD_PAIRS:
            origin_val = invoice_data.get(origin_field)
            dest_val = invoice_data.get(dest_field)
            
            # Check if both values are present and not empty
            origin_valid = origin_val and str(origin_val).strip()
            dest_valid = dest_val and str(dest_val).strip()
            
            if origin_valid and dest_valid:
                valid_location_pair = True
                location_info.append(f"{origin_field}: {origin_val}, {dest_field}: {dest_val}")
            elif origin_valid or dest_valid:
                # Partial location info - note it but don't fail
                partial_info = f"{origin_field}: {origin_val or 'Missing'}, {dest_field}: {dest_val or 'Missing'}"
                location_info.append(f"Partial - {partial_info}")
        
        if not valid_location_pair:
            issues.append(ValidationIssue(
                field='location_fields',
                severity=ValidationSeverity.ERROR,
                message="At least one complete location pair (origin-destination) is required",
                current_value="; ".join(location_info) if location_info else "No location data found",
                expected_format="origin_port+destination_port OR shipper_country+consignee_country OR shipper_city+consignee_city"
            ))
        
        return issues
    
    def _validate_important_fields(self, invoice_data: Dict[str, Any]) -> List[ValidationIssue]:
        """Check important fields that should be present (warnings if missing)"""
        issues = []
        
        for field_key, field_name in self.IMPORTANT_FIELDS.items():
            value = invoice_data.get(field_key)
            
            if value is None or value == '' or (isinstance(value, str) and not value.strip()):
                issues.append(ValidationIssue(
                    field=field_key,
                    severity=ValidationSeverity.WARNING,
                    message=f"{field_name} is missing - recommended for complete processing",
                    current_value=value
                ))
        
        return issues
    
    def _validate_field_formats(self, invoice_data: Dict[str, Any]) -> List[ValidationIssue]:
        """Validate field formats and data types"""
        issues = []
        
        # Validate invoice number format
        invoice_number = invoice_data.get('invoice_number')
        if invoice_number and not re.match(r'^[A-Z0-9]{5,20}$', str(invoice_number)):
            issues.append(ValidationIssue(
                field='invoice_number',
                severity=ValidationSeverity.WARNING,
                message="Invoice number format may be invalid",
                current_value=invoice_number,
                expected_format="Alphanumeric, 5-20 characters"
            ))
        
        # Validate currency
        currency = invoice_data.get('currency')
        if currency and currency not in self.VALID_CURRENCIES:
            issues.append(ValidationIssue(
                field='currency',
                severity=ValidationSeverity.ERROR,
                message=f"Invalid currency code: {currency}",
                current_value=currency,
                expected_format=f"One of: {', '.join(sorted(self.VALID_CURRENCIES))}"
            ))
        
        # Validate countries
        for field in ['shipper_country', 'consignee_country']:
            country = invoice_data.get(field)
            if country and country not in self.VALID_COUNTRIES:
                issues.append(ValidationIssue(
                    field=field,
                    severity=ValidationSeverity.WARNING,
                    message=f"Unknown country code: {country}",
                    current_value=country,
                    expected_format="ISO 3166-1 alpha-2 code"
                ))
        
        # Validate numeric fields
        numeric_fields = {
            'total_charges': 'Total Charges',
            'weight': 'Weight', 
            'pieces': 'Pieces',
            'exchange_rate': 'Exchange Rate'
        }
        
        for field_key, field_name in numeric_fields.items():
            value = invoice_data.get(field_key)
            if value is not None:
                try:
                    num_value = float(value)
                    if num_value < 0:
                        issues.append(ValidationIssue(
                            field=field_key,
                            severity=ValidationSeverity.ERROR,
                            message=f"{field_name} cannot be negative",
                            current_value=value
                        ))
                except (ValueError, TypeError):
                    issues.append(ValidationIssue(
                        field=field_key,
                        severity=ValidationSeverity.ERROR,
                        message=f"{field_name} must be a valid number",
                        current_value=value
                    ))
        
        # Validate date fields - only flag if completely missing
        # Date format validation removed as per user request
        # If dates are present, accept them regardless of format
        
        return issues
    
    def _validate_business_rules(self, invoice_data: Dict[str, Any]) -> List[ValidationIssue]:
        """Validate business logic rules"""
        issues = []
        
        # Check if total charges is reasonable
        total_charges = invoice_data.get('total_charges')
        if total_charges:
            try:
                charges = float(total_charges)
                if charges > 100000:  # $100k threshold
                    issues.append(ValidationIssue(
                        field='total_charges',
                        severity=ValidationSeverity.WARNING,
                        message=f"Unusually high charges: ${charges:,.2f}",
                        current_value=charges
                    ))
                elif charges < 1:
                    issues.append(ValidationIssue(
                        field='total_charges',
                        severity=ValidationSeverity.WARNING,
                        message=f"Unusually low charges: ${charges:.2f}",
                        current_value=charges
                    ))
            except (ValueError, TypeError):
                pass
        
        # Check exchange rate reasonableness
        exchange_rate = invoice_data.get('exchange_rate')
        if exchange_rate:
            try:
                rate = float(exchange_rate)
                if rate > 10 or rate < 0.1:
                    issues.append(ValidationIssue(
                        field='exchange_rate',
                        severity=ValidationSeverity.WARNING,
                        message=f"Exchange rate seems unusual: {rate}",
                        current_value=rate
                    ))
            except (ValueError, TypeError):
                pass
        
        # Check weight vs charges correlation
        weight = invoice_data.get('weight')
        if weight and total_charges:
            try:
                w = float(weight)
                c = float(total_charges)
                if w > 0 and c > 0:
                    rate_per_kg = c / w
                    if rate_per_kg > 1000:  # $1000/kg seems excessive
                        issues.append(ValidationIssue(
                            field='total_charges',
                            severity=ValidationSeverity.INFO,
                            message=f"High cost per kg: ${rate_per_kg:.2f}/kg",
                            current_value=f"${c:.2f} for {w}kg"
                        ))
            except (ValueError, TypeError):
                pass
        
        # Check origin != destination for any valid location pair
        for origin_field, dest_field in self.LOCATION_FIELD_PAIRS:
            origin = invoice_data.get(origin_field)
            destination = invoice_data.get(dest_field)
            
            if (origin and destination and 
                str(origin).strip() and str(destination).strip() and
                str(origin).strip() == str(destination).strip()):
                issues.append(ValidationIssue(
                    field=origin_field,
                    severity=ValidationSeverity.WARNING,
                    message=f"Origin and destination are the same ({origin_field}: {origin})",
                    current_value=f"{origin} -> {destination}"
                ))
                break  # Only report once if multiple pairs have same issue
        
        return issues
    
    def _is_valid_date(self, date_str: str) -> bool:
        """Check if date string is in valid format"""
        if not date_str:
            return False
        
        # Try common date formats
        formats = ['%Y-%m-%d', '%Y%m%d', '%Y-%m-%d %H:%M:%S']
        for fmt in formats:
            try:
                datetime.strptime(str(date_str)[:len(fmt.replace('%', ''))], fmt)
                return True
            except ValueError:
                continue
        return False
    
    def _calculate_validation_score(self, issues: List[ValidationIssue]) -> float:
        """
        Calculate validation score (0-100) based on issues
        - Start with 100
        - Subtract 20 for each error
        - Subtract 5 for each warning
        - Subtract 1 for each info
        """
        score = 100.0
        
        for issue in issues:
            if issue.severity == ValidationSeverity.ERROR:
                score -= 20
            elif issue.severity == ValidationSeverity.WARNING:
                score -= 5
            elif issue.severity == ValidationSeverity.INFO:
                score -= 1
        
        return max(0.0, score)

# Convenience function for validating invoices from database
def validate_invoice_from_db(invoice_row) -> ValidationResult:
    """
    Validate an invoice from database row data
    
    Args:
        invoice_row: Database row (dict-like) containing invoice data
        
    Returns:
        ValidationResult
    """
    # Convert database row to dictionary
    if hasattr(invoice_row, 'keys'):
        invoice_data = dict(invoice_row)
    else:
        # Handle tuple/list case
        # This would need to be mapped based on your query structure
        invoice_data = invoice_row
    
    validator = InvoiceValidator()
    return validator.validate_invoice(invoice_data)
