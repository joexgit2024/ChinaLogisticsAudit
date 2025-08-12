"""
Enhanced EDI File Upload and Processing System
==============================================

This module provides comprehensive file upload, validation, parsing, and database storage
for DHL EDI invoice files with full audit trail and error handling.
"""

import os
import hashlib
import logging
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
import time

# Import the validator
try:
    from app.invoice_validator import InvoiceValidator
except ImportError:
    # For when running from different contexts
    InvoiceValidator = None

class ProcessingStatus(Enum):
    """Processing status enumeration"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"

class ValidationError(Exception):
    """Custom validation error"""
    pass

@dataclass
class ProcessingResult:
    """Result of processing operation"""
    status: ProcessingStatus
    invoices_processed: int
    charges_processed: int
    line_items_processed: int
    errors: List[str]
    warnings: List[str]
    file_path: str
    processing_time: float
    file_hash: str

class EnhancedEDIProcessor:
    """Enhanced EDI file processor with comprehensive validation and error handling"""
    
    def __init__(self, upload_folder: str, max_file_size: int = 52428800):  # 50MB
        self.upload_folder = upload_folder
        self.max_file_size = max_file_size
        self.allowed_extensions = {'.edi', '.txt', '.x12'}
        self.supported_transaction_types = {'210', '310', '110', '214'}
        
        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)
    
    def validate_file(self, file_obj) -> Tuple[bool, List[str]]:
        """
        Comprehensive file validation
        Returns: (is_valid, error_list)
        """
        errors = []
        
        # Check filename
        if not file_obj.filename:
            errors.append("No filename provided")
            return False, errors
        
        # Check file extension
        file_ext = os.path.splitext(file_obj.filename)[1].lower()
        if file_ext not in self.allowed_extensions:
            errors.append(f"Invalid file extension. Allowed: {', '.join(self.allowed_extensions)}")
        
        # Check file size (if possible)
        try:
            file_obj.seek(0, 2)  # Seek to end
            file_size = file_obj.tell()
            file_obj.seek(0)  # Reset to beginning
            
            if file_size > self.max_file_size:
                errors.append(f"File too large. Maximum size: {self.max_file_size / 1024 / 1024:.1f}MB")
            elif file_size == 0:
                errors.append("File is empty")
                
        except Exception as e:
            errors.append(f"Could not determine file size: {str(e)}")
        
        return len(errors) == 0, errors
    
    def validate_edi_content(self, content: str) -> Tuple[bool, List[str], List[str]]:
        """
        Validate EDI content structure
        Returns: (is_valid, errors, warnings)
        """
        errors = []
        warnings = []
        
        if not content.strip():
            errors.append("File content is empty")
            return False, errors, warnings
        
        # Check for basic EDI structure
        if 'ISA' not in content and 'ST' not in content:
            errors.append("No valid EDI segments found (missing ISA or ST)")
        
        # Check for transaction types
        found_transactions = []
        for tx_type in self.supported_transaction_types:
            if f'ST*{tx_type}*' in content or f'ST,{tx_type},' in content:
                found_transactions.append(tx_type)
        
        if not found_transactions:
            errors.append(f"No supported transaction types found. Expected: {', '.join(self.supported_transaction_types)}")
        else:
            warnings.append(f"Found transaction types: {', '.join(found_transactions)}")
        
        # Check segment count
        segment_count_tilde = content.count('~')
        segment_count_comma = content.count(',')
        
        if segment_count_tilde < 5 and segment_count_comma < 5:
            warnings.append("Low segment count detected - file may be incomplete")
        
        return len(errors) == 0, errors, warnings
    
    def calculate_file_hash(self, content: str) -> str:
        """Calculate SHA-256 hash of file content"""
        return hashlib.sha256(content.encode('utf-8')).hexdigest()
    
    def save_uploaded_file(self, file_obj, content: str) -> str:
        """Save uploaded file with timestamp and return filepath"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_filename = "".join(c for c in file_obj.filename if c.isalnum() or c in '._-')
        saved_filename = f"{timestamp}_{safe_filename}"
        file_path = os.path.join(self.upload_folder, saved_filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
            
        return file_path
    
    def process_invoice_data(self, invoice_data: Dict[str, Any], conn, file_path: str, file_hash: str) -> Tuple[int, List[str]]:
        """
        Process single invoice with comprehensive error handling
        Returns: (invoice_id, errors)
        """
        errors = []
        
        try:
            # Map all available invoice fields from the database schema
            invoice_fields = {
                'invoice_number': invoice_data.get('invoice_number'),
                'client_code': invoice_data.get('client_code'),
                'carrier_code': invoice_data.get('carrier_code'),
                'account_number': invoice_data.get('account_number'),
                'account_period': invoice_data.get('account_period'),
                'billed_to_type': invoice_data.get('billed_to_type'),
                'tracking_number': invoice_data.get('tracking_number'),
                'invoice_date': invoice_data.get('invoice_date'),
                'invoice_status': 'pending',
                'audit_exception_status': None,
                
                # Address information
                'shipper_name': invoice_data.get('shipper_name'),
                'shipper_address': invoice_data.get('shipper_address'),
                'shipper_city': invoice_data.get('shipper_city'),
                'shipper_state': invoice_data.get('shipper_state'),
                'shipper_postal_code': invoice_data.get('shipper_postal_code'),
                'shipper_country': invoice_data.get('shipper_country'),
                
                'consignee_name': invoice_data.get('consignee_name'),
                'consignee_address': invoice_data.get('consignee_address'),
                'consignee_city': invoice_data.get('consignee_city'),
                'consignee_state': invoice_data.get('consignee_state'),
                'consignee_postal_code': invoice_data.get('consignee_postal_code'),
                'consignee_country': invoice_data.get('consignee_country'),
                
                'bill_to_name': invoice_data.get('bill_to_name'),
                'bill_to_address': invoice_data.get('bill_to_address'),
                'bill_to_city': invoice_data.get('bill_to_city'),
                'bill_to_state': invoice_data.get('bill_to_state'),
                'bill_to_postal_code': invoice_data.get('bill_to_postal_code'),
                'bill_to_country': invoice_data.get('bill_to_country'),
                
                # Shipment information
                'vessel_name': invoice_data.get('vessel_name'),
                'container_number': invoice_data.get('container_number'),
                'bill_of_lading': invoice_data.get('bill_of_lading'),
                'booking_number': invoice_data.get('booking_number'),
                'origin_port': invoice_data.get('origin_port'),
                'destination_port': invoice_data.get('destination_port'),
                'pickup_date': invoice_data.get('pickup_date'),
                'delivery_date': invoice_data.get('delivery_date'),
                'service_date': invoice_data.get('service_date'),
                'ship_date': invoice_data.get('ship_date'),
                'shipment_entered_date': invoice_data.get('shipment_entered_date'),
                'invoice_created_date': invoice_data.get('invoice_created_date'),
                
                # Reference information
                'reference_number': invoice_data.get('reference_number'),
                'pro_number': invoice_data.get('pro_number'),
                
                # Financial information
                'total_charges': invoice_data.get('total_charges', 0.0),
                'net_charge': invoice_data.get('net_charge', 0.0),
                'invoice_amount': invoice_data.get('invoice_amount', 0.0),
                'check_number': invoice_data.get('check_number'),
                'check_date': invoice_data.get('check_date'),
                
                # Weight and measurement
                'weight': invoice_data.get('weight', 0.0),
                'bill_weight': invoice_data.get('bill_weight', 0.0),
                'ship_weight': invoice_data.get('ship_weight', 0.0),
                'pieces': invoice_data.get('pieces', 0),
                'volume': invoice_data.get('volume', 0.0),
                'declared_value': invoice_data.get('declared_value', 0.0),
                
                # Currency and rates
                'currency': invoice_data.get('currency'),
                'exchange_rate': invoice_data.get('exchange_rate', 1.0),
                'from_currency': invoice_data.get('from_currency'),
                'to_currency': invoice_data.get('to_currency'),
                
                # Service information
                'shipping_mode': invoice_data.get('shipping_mode'),
                'service_type': invoice_data.get('service_type'),
                'delivery_commitment': invoice_data.get('delivery_commitment'),
                'commodity_type': invoice_data.get('commodity_type'),
                'incoterm': invoice_data.get('incoterm'),
                
                # Business information
                'vendor_number': invoice_data.get('vendor_number'),
                'customer_vat_registration': invoice_data.get('customer_vat_registration'),
                'sap_plant': invoice_data.get('sap_plant'),
                'shipper_company_code': invoice_data.get('shipper_company_code'),
                'mode': invoice_data.get('mode'),
                'allocation_percentage': invoice_data.get('allocation_percentage'),
                'master_shipper_address': invoice_data.get('master_shipper_address'),
                'company_code': invoice_data.get('company_code'),
                'shipper_description': invoice_data.get('shipper_description'),
                'gl_account': invoice_data.get('gl_account'),
                
                # Carrier information
                'carrier_name': invoice_data.get('carrier_name'),
                'carrier_address': invoice_data.get('carrier_address'),
                'carrier_city': invoice_data.get('carrier_city'),
                'carrier_state': invoice_data.get('carrier_state'),
                'carrier_postal_code': invoice_data.get('carrier_postal_code'),
                'carrier_country': invoice_data.get('carrier_country'),
                'carrier_vat_registration': invoice_data.get('carrier_vat_registration'),
                
                # Additional business fields
                'direction': invoice_data.get('direction'),
                'charge_group': invoice_data.get('charge_group'),
                'recipient_description': invoice_data.get('recipient_description'),
                'partner_bank_type': invoice_data.get('partner_bank_type'),
                'profit_center': invoice_data.get('profit_center'),
                'recipient_type': invoice_data.get('recipient_type'),
                'shipper_plant': invoice_data.get('shipper_plant'),
                'tax_code': invoice_data.get('tax_code'),
                
                # System fields
                'audit_status': 'pending',
                'audit_notes': None,
                'raw_edi': invoice_data.get('raw_edi', ''),
                'uploaded_file_path': os.path.basename(file_path),
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            
            # Build dynamic INSERT statement
            columns = list(invoice_fields.keys())
            placeholders = ', '.join(['?' for _ in columns])
            values = [invoice_fields[col] for col in columns]
            
            insert_sql = f'''
                INSERT INTO invoices ({', '.join(columns)})
                VALUES ({placeholders})
            '''
            
            cursor = conn.execute(insert_sql, values)
            invoice_id = cursor.lastrowid
            
            self.logger.info(f"Inserted invoice {invoice_data.get('invoice_number')} with ID: {invoice_id}")
            
            return invoice_id, errors
            
        except Exception as e:
            error_msg = f"Error inserting invoice {invoice_data.get('invoice_number', 'Unknown')}: {str(e)}"
            errors.append(error_msg)
            self.logger.error(error_msg)
            return None, errors
    
    def process_charges(self, invoice_id: int, charges: List[Dict[str, Any]], conn) -> Tuple[int, List[str]]:
        """Process charges for an invoice"""
        errors = []
        processed_count = 0
        
        for charge in charges:
            try:
                conn.execute('''
                    INSERT INTO charges (
                        invoice_id, charge_type, amount, description, 
                        rate, quantity, unit, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    invoice_id,
                    charge.get('charge_type') or charge.get('type'),
                    charge.get('amount', 0.0),
                    charge.get('description'),
                    charge.get('rate', 0.0),
                    charge.get('quantity', 0.0),
                    charge.get('unit'),
                    datetime.now().isoformat()
                ))
                processed_count += 1
            except Exception as e:
                error_msg = f"Error inserting charge: {str(e)}"
                errors.append(error_msg)
                self.logger.error(error_msg)
        
        return processed_count, errors
    
    def process_line_items(self, invoice_id: int, line_items: List[Dict[str, Any]], conn) -> Tuple[int, List[str]]:
        """Process line items for an invoice"""
        errors = []
        processed_count = 0
        
        for item in line_items:
            try:
                conn.execute('''
                    INSERT INTO line_items (
                        invoice_id, line_number, item_description, quantity, 
                        weight, volume, dimensions, unit_type, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    invoice_id,
                    item.get('line_number'),
                    item.get('description') or item.get('item_description'),
                    item.get('quantity', 0.0),
                    item.get('weight', 0.0),
                    item.get('volume', 0.0),
                    item.get('dimensions'),
                    item.get('unit_type'),
                    datetime.now().isoformat()
                ))
                processed_count += 1
            except Exception as e:
                error_msg = f"Error inserting line item: {str(e)}"
                errors.append(error_msg)
                self.logger.error(error_msg)
        
        return processed_count, errors
    
    def process_reference_numbers(self, invoice_id: int, references: List[Dict[str, Any]], conn) -> Tuple[int, List[str]]:
        """Process reference numbers for an invoice"""
        errors = []
        processed_count = 0
        
        for ref in references:
            try:
                conn.execute('''
                    INSERT INTO reference_numbers (
                        invoice_id, reference_type, reference_value, created_at
                    ) VALUES (?, ?, ?, ?)
                ''', (
                    invoice_id,
                    ref.get('reference_type') or ref.get('type'),
                    ref.get('reference_value') or ref.get('value'),
                    datetime.now().isoformat()
                ))
                processed_count += 1
            except Exception as e:
                error_msg = f"Error inserting reference: {str(e)}"
                errors.append(error_msg)
                self.logger.error(error_msg)
        
        return processed_count, errors
    
    def validate_and_update_audit_status(self, invoice_id: int, conn) -> Tuple[str, List[str]]:
        """
        Run validation on processed invoice and update audit status
        Returns: (audit_status, validation_errors)
        """
        if not InvoiceValidator:
            return 'pending', ['Validation module not available']
        
        try:
            # Get invoice data from database
            invoice = conn.execute('''
                SELECT 
                    invoice_number, shipper_name, consignee_name,
                    origin_port, destination_port, total_charges, weight, pieces,
                    currency, exchange_rate, service_date, delivery_date,
                    shipper_country, consignee_country, tracking_number, reference_number,
                    pickup_date, invoice_date
                FROM invoices 
                WHERE id = ?
            ''', (invoice_id,)).fetchone()
            
            if not invoice:
                return 'pending', ['Invoice not found for validation']
            
            # Convert to dict and validate
            invoice_dict = dict(invoice)
            validator = InvoiceValidator()
            validation_result = validator.validate_invoice(invoice_dict)
            
            # Determine audit status based on validation
            if validation_result.is_valid:
                if validation_result.score >= 95:
                    audit_status = 'approved'
                elif validation_result.score >= 80:
                    audit_status = 'pending'  # Good but needs review
                else:
                    audit_status = 'review'   # Passed validation but has warnings
            else:
                audit_status = 'flagged'  # Has errors
            
            # Update audit status in database
            conn.execute('''
                UPDATE invoices 
                SET audit_status = ?, updated_at = ?
                WHERE id = ?
            ''', (audit_status, datetime.now().isoformat(), invoice_id))
            
            # Return validation issues as errors for logging
            validation_errors = [issue.message for issue in validation_result.issues]
            
            return audit_status, validation_errors
            
        except Exception as e:
            error_msg = f"Error during validation: {str(e)}"
            self.logger.error(error_msg)
            return 'pending', [error_msg]
