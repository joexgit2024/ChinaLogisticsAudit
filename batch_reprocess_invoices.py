#!/usr/bin/env python3
"""
Batch Reprocessing Script for Incomplete Invoice Extractions
Uses Llama 3.2 with enhanced schema-guided extraction
"""

import os
import json
import sqlite3
from datetime import datetime
import time
import argparse
from typing import Dict, List, Optional, Any
from schema_driven_llm_processor import SchemaDrivenLLMProcessor

class InvoiceReprocessor:
    """
    Class to identify and reprocess incomplete invoice extractions
    using Llama 3.2 and enhanced schema-guided extraction
    """
    
    def __init__(self, db_path: str = "dhl_audit.db", model_name: str = "llama3.2:latest"):
        self.db_path = db_path
        self.model_name = model_name
        self.processor = SchemaDrivenLLMProcessor(model_name=model_name)
        self.batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.reprocessed_count = 0
        self.success_count = 0
        self.stats = {
            'total_identified': 0,
            'successfully_reprocessed': 0,
            'failures': 0,
            'processing_time': 0
        }
        
    def identify_incomplete_extractions(self, limit: int = 100) -> List[Dict]:
        """
        Identify invoices with incomplete extractions based on missing key fields
        """
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Find extractions that have missing critical data in the invoice_summary table
        cursor.execute("""
            SELECT e.invoice_no, e.pdf_filename, e.raw_pdf_text, e.extracted_data,
                   s.invoice_date, s.customer_name, s.final_total, s.currency,
                   e.extraction_confidence
            FROM llm_pdf_extractions e
            LEFT JOIN llm_invoice_summary s ON e.invoice_no = s.invoice_no
            WHERE (s.invoice_date IS NULL OR s.customer_name IS NULL OR 
                  s.final_total IS NULL OR s.final_total = 0 OR
                  s.currency IS NULL OR e.extraction_confidence < 0.5)
                  AND e.raw_pdf_text IS NOT NULL
            LIMIT ?
        """, (limit,))
        
        incomplete_invoices = []
        for row in cursor.fetchall():
            invoice_data = {
                'invoice_no': row['invoice_no'],
                'pdf_filename': row['pdf_filename'],
                'raw_pdf_text': row['raw_pdf_text'],
                'extracted_data': row['extracted_data'] if row['extracted_data'] else None,
                'missing_fields': []
            }
            
            # Identify which fields are missing
            if not row['invoice_date']:
                invoice_data['missing_fields'].append('invoice_date')
            if not row['customer_name']:
                invoice_data['missing_fields'].append('customer_name')
            if not row['final_total'] or float(row['final_total'] or 0) == 0:
                invoice_data['missing_fields'].append('final_total')
            if not row['currency']:
                invoice_data['missing_fields'].append('currency')
            if row['extraction_confidence'] is None or float(row['extraction_confidence'] or 0) < 0.5:
                invoice_data['missing_fields'].append('low_confidence')
                
            incomplete_invoices.append(invoice_data)
        
        conn.close()
        self.stats['total_identified'] = len(incomplete_invoices)
        print(f"Identified {len(incomplete_invoices)} invoices with incomplete extractions")
        return incomplete_invoices

    def reprocess_invoice(self, invoice_data: Dict) -> Dict:
        """
        Reprocess a single invoice using enhanced schema-guided extraction
        """
        invoice_no = invoice_data['invoice_no']
        raw_pdf_text = invoice_data['raw_pdf_text']
        
        if not raw_pdf_text:
            print(f"⚠️ No raw PDF text available for invoice {invoice_no}")
            return {
                'invoice_no': invoice_no,
                'success': False,
                'error': 'No raw PDF text available'
            }
        
        print(f"🔄 Reprocessing invoice {invoice_no}...")
        start_time = time.time()
        
        try:
            # Create a custom structured prompt with the exact schema and format examples
            custom_prompt = self.create_custom_prompt(raw_pdf_text)
            
            # Query the Llama 3.2 model with the enhanced prompt
            llm_response = self.processor.query_llm(custom_prompt)
            
            if not llm_response:
                return {
                    'invoice_no': invoice_no,
                    'success': False,
                    'error': 'No response from LLM'
                }
                
            # Clean and parse the response
            cleaned_response = self.processor.clean_llm_response(llm_response)
            
            try:
                # Parse the JSON response
                parsed_response = json.loads(cleaned_response)
                
                # Validate the extraction
                validation_result = self.validate_extraction(parsed_response)
                
                # Calculate confidence based on validation
                confidence = validation_result['confidence']
                manual_review = validation_result['manual_review_needed']
                
                # Update the database with the new extraction
                self.update_extraction_in_db(
                    invoice_no=invoice_no,
                    extracted_json=parsed_response,
                    confidence=confidence,
                    manual_review=manual_review
                )
                
                processing_time = time.time() - start_time
                
                return {
                    'invoice_no': invoice_no,
                    'success': True,
                    'confidence': confidence,
                    'manual_review_needed': manual_review,
                    'processing_time': processing_time,
                    'validation_issues': validation_result.get('issues', [])
                }
                
            except json.JSONDecodeError as e:
                print(f"❌ Error parsing JSON response for invoice {invoice_no}: {e}")
                return {
                    'invoice_no': invoice_no,
                    'success': False,
                    'error': f'Invalid JSON response: {str(e)}'
                }
                
        except Exception as e:
            print(f"❌ Error reprocessing invoice {invoice_no}: {e}")
            return {
                'invoice_no': invoice_no,
                'success': False,
                'error': str(e)
            }
    
    def validate_extraction(self, extraction: Dict) -> Dict:
        """
        Validate the extracted data against business rules
        Returns confidence score and flags for manual review if needed
        """
        issues = []
        confidence = 0.95  # Start with high confidence
        manual_review_needed = False
        
        # Extract the summary and line items
        summary = extraction.get('invoice_summary', {})
        line_items = extraction.get('billing_line_items', [])
        
        # Check required fields
        required_fields = [
            'invoice_no', 'invoice_date', 'customer_name', 
            'currency', 'final_total'
        ]
        
        for field in required_fields:
            if not summary.get(field):
                issues.append(f"Missing required field: {field}")
                confidence -= 0.15
                manual_review_needed = True
        
        # Validate invoice number format (usually starts with letter followed by numbers)
        invoice_no = summary.get('invoice_no', '')
        if not invoice_no or not (invoice_no[0].isalpha() and any(c.isdigit() for c in invoice_no)):
            issues.append("Invoice number format appears invalid")
            confidence -= 0.1
            manual_review_needed = True
        
        # Validate date format
        invoice_date = summary.get('invoice_date', '')
        if invoice_date:
            try:
                # Try to parse the date (should be in YYYY-MM-DD format)
                datetime.strptime(invoice_date, '%Y-%m-%d')
            except ValueError:
                issues.append("Invoice date format is not YYYY-MM-DD")
                confidence -= 0.05
        
        # Validate currency
        valid_currencies = ['AUD', 'USD', 'EUR', 'SGD', 'HKD', 'CNY']
        if summary.get('currency') and summary.get('currency') not in valid_currencies:
            issues.append(f"Unknown currency: {summary.get('currency')}")
            confidence -= 0.05
            
        # Validate amounts
        if summary.get('final_total'):
            try:
                final_total = float(summary.get('final_total'))
                
                # Check for unreasonable values
                if final_total <= 0:
                    issues.append("Final total should be greater than zero")
                    confidence -= 0.1
                elif final_total > 1000000:  # Unusually large amount
                    issues.append("Final total seems unusually large")
                    confidence -= 0.05
                    manual_review_needed = True
                    
                # Consistency check - does sum of line items match final total?
                if line_items:
                    line_items_total = sum(float(item.get('amount') or 0) for item in line_items)
                    if abs(line_items_total - final_total) > 1.0:  # Allow small rounding differences
                        issues.append(f"Sum of line items ({line_items_total}) doesn't match final total ({final_total})")
                        confidence -= 0.15
                        manual_review_needed = True
                        
            except (ValueError, TypeError):
                issues.append("Final total is not a valid number")
                confidence -= 0.1
                manual_review_needed = True
        
        # Check line item categorization
        valid_categories = [
            'FREIGHT', 'SERVICE_CHARGE', 'SURCHARGE', 
            'DUTY_TAX', 'FUEL_SURCHARGE', 'SECURITY_CHARGE', 'OTHER'
        ]
        
        for i, item in enumerate(line_items):
            if not item.get('description'):
                issues.append(f"Line item {i+1} missing description")
                confidence -= 0.05
                
            if not item.get('amount'):
                issues.append(f"Line item {i+1} missing amount")
                confidence -= 0.05
                
            if item.get('category') and item.get('category') not in valid_categories:
                issues.append(f"Line item {i+1} has invalid category: {item.get('category')}")
                confidence -= 0.05
        
        # Cap confidence at reasonable bounds
        confidence = max(0.1, min(confidence, 1.0))
        
        return {
            'confidence': confidence,
            'manual_review_needed': manual_review_needed,
            'issues': issues
        }
    
    def update_extraction_in_db(self, invoice_no: str, extracted_json: Dict, 
                               confidence: float, manual_review: bool) -> None:
        """
        Update the database with the new extraction results
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # First, update the main extraction record
            cursor.execute("""
                UPDATE llm_pdf_extractions
                SET extracted_data = ?,
                    extraction_confidence = ?,
                    processing_timestamp = ?,
                    llm_model_used = ?,
                    manual_review_needed = ?,
                    reprocessed = 1,
                    reprocessed_batch = ?
                WHERE invoice_no = ?
            """, (
                json.dumps(extracted_json),
                confidence,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                self.model_name,
                1 if manual_review else 0,
                self.batch_id,
                invoice_no
            ))
            
            # Next, update the invoice summary table
            invoice_summary = extracted_json.get("invoice_summary", {})
            
            # Check if record exists in llm_invoice_summary
            cursor.execute("SELECT invoice_no FROM llm_invoice_summary WHERE invoice_no = ?", (invoice_no,))
            if cursor.fetchone():
                # Update existing record
                cursor.execute("""
                    UPDATE llm_invoice_summary
                    SET invoice_date = ?,
                        due_date = ?,
                        customer_name = ?,
                        currency = ?,
                        subtotal = ?,
                        gst_total = ?,
                        final_total = ?,
                        service_type = ?,
                        origin = ?,
                        destination = ?,
                        weight = ?,
                        shipment_ref = ?
                    WHERE invoice_no = ?
                """, (
                    invoice_summary.get("invoice_date"),
                    invoice_summary.get("due_date"),
                    invoice_summary.get("customer_name"),
                    invoice_summary.get("currency"),
                    invoice_summary.get("subtotal"),
                    invoice_summary.get("gst_total"),
                    invoice_summary.get("final_total"),
                    invoice_summary.get("service_type"),
                    invoice_summary.get("origin"),
                    invoice_summary.get("destination"),
                    invoice_summary.get("weight"),
                    invoice_summary.get("shipment_ref"),
                    invoice_no
                ))
            else:
                # Insert new record
                cursor.execute("""
                    INSERT INTO llm_invoice_summary
                    (invoice_no, invoice_date, due_date, customer_name, currency, 
                    subtotal, gst_total, final_total, service_type, origin, 
                    destination, weight, shipment_ref)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    invoice_no,
                    invoice_summary.get("invoice_date"),
                    invoice_summary.get("due_date"),
                    invoice_summary.get("customer_name"),
                    invoice_summary.get("currency"),
                    invoice_summary.get("subtotal"),
                    invoice_summary.get("gst_total"),
                    invoice_summary.get("final_total"),
                    invoice_summary.get("service_type"),
                    invoice_summary.get("origin"),
                    invoice_summary.get("destination"),
                    invoice_summary.get("weight"),
                    invoice_summary.get("shipment_ref")
                ))
            
            # Update the billing line items
            # First delete any existing line items for this invoice
            cursor.execute("DELETE FROM llm_billing_line_items WHERE invoice_no = ?", (invoice_no,))
            
            # Then insert the new line items
            for item in extracted_json.get("billing_line_items", []):
                cursor.execute("""
                    INSERT INTO llm_billing_line_items
                    (invoice_no, line_item_index, description, amount, gst_amount, 
                    total_amount, currency, category)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    invoice_no,
                    item.get("line_item_index"),
                    item.get("description"),
                    item.get("amount"),
                    item.get("gst_amount"),
                    item.get("total_amount"),
                    item.get("currency"),
                    item.get("category")
                ))
            
            # Commit the changes
            conn.commit()
            self.success_count += 1
            print(f"✅ Successfully updated extraction for invoice {invoice_no}")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error updating database for invoice {invoice_no}: {e}")
            raise
            
        finally:
            conn.close()
    
    def create_custom_prompt(self, pdf_text: str) -> str:
        """
        Create a schema-driven prompt with few-shot examples for maximum accuracy
        """
        return f"""You are an expert DHL invoice data extraction specialist. Extract information from the invoice text below, and format it according to this exact JSON schema with few-shot examples:

{{{{
  "invoice_summary": {{{{
    "invoice_no": "<INVOICE_NUMBER>",
    "invoice_date": "<YYYY-MM-DD>",
    "due_date": "<YYYY-MM-DD>",
    "customer_name": "<CUSTOMER_NAME>",
    "currency": "<CURRENCY_CODE>",
    "subtotal": <NUMERIC_AMOUNT>,
    "gst_total": <NUMERIC_AMOUNT>,
    "final_total": <NUMERIC_AMOUNT>,
    "service_type": "<SERVICE_TYPE>",
    "origin": "<ORIGIN_LOCATION>",
    "destination": "<DESTINATION_LOCATION>",
    "weight": "<WEIGHT_WITH_UNIT>",
    "shipment_ref": "<SHIPMENT_REFERENCE>",
    "account_number": "<ACCOUNT_NUMBER>",
    "payment_terms": "<PAYMENT_TERMS>",
    "incoterms": "<INCOTERMS>",
    "transportation_mode": "<TRANSPORTATION_MODE>",
    "masterbill": "<MASTER_BILL>",
    "housebill": "<HOUSE_BILL>",
    "awb_number": "<AWB_NUMBER>",
    "shipment_date": "<YYYY-MM-DD>",
    "total_pieces": <NUMBER_OF_PIECES>,
    "chargeable_weight": <WEIGHT_VALUE>,
    "volume_weight": <WEIGHT_VALUE>,
    "exchange_rate_eur": <EXCHANGE_RATE>,
    "exchange_rate_usd": <EXCHANGE_RATE>,
    "shipper_name": "<SHIPPER_NAME>",
    "shipper_address": "<SHIPPER_ADDRESS>",
    "consignee_name": "<CONSIGNEE_NAME>",
    "consignee_address": "<CONSIGNEE_ADDRESS>",
    "commodity_description": "<COMMODITY_DESCRIPTION>"
  }}}},
  "billing_line_items": [
    {{{{
      "line_item_index": <INDEX_NUMBER>,
      "description": "<LINE_ITEM_DESCRIPTION>",
      "amount": <NUMERIC_AMOUNT>,
      "gst_amount": <NUMERIC_AMOUNT>,
      "total_amount": <NUMERIC_AMOUNT>,
      "currency": "<CURRENCY_CODE>",
      "category": "<CATEGORY>"
    }}}}
  ]
}}}}

EXAMPLE 1 - Multi-line items with freight and service charges:
INPUT: Invoice D2159508, OUTDOOR WIRELESS NETWORKS AUSTRALIA PTY LTD, 2025-07-14, Due: 2025-10-12, AUD 3328.3, Sydney to Auckland, 693.000 KG, 5 pieces, exchange rate USD 0.624795

OUTPUT:
{{{{
    "invoice_summary": {{{{
        "invoice_no": "D2159508",
        "invoice_date": "2025-07-14",
        "due_date": "2025-10-12",
        "customer_name": "OUTDOOR WIRELESS NETWORKS AUSTRALIA PTY LTD",
        "currency": "AUD",
        "subtotal": 3328.3,
        "gst_total": 0.0,
        "final_total": 3328.3,
        "service_type": null,
        "origin": "AUSYD = Sydney, Australia",
        "destination": "NZAKL Auckland, New Zealand",
        "weight": "693.000 KG",
        "shipment_ref": "S2503229764",
        "account_number": "AUOU0003/AUOU0003",
        "payment_terms": "90 days from Inv. Date",
        "incoterms": null,
        "transportation_mode": null,
        "masterbill": "255655431",
        "housebill": "DZ1395970",
        "awb_number": null,
        "shipment_date": "2025-07-15",
        "total_pieces": 5,
        "chargeable_weight": 7.791,
        "volume_weight": 7.791,
        "exchange_rate_eur": null,
        "exchange_rate_usd": 0.624795,
        "shipper_name": "OUTDOOR WIRELESS NETWORKS AUSTRALIA PTY LTD",
        "shipper_address": null,
        "consignee_name": "AVW LIMITED",
        "consignee_address": null,
        "commodity_description": "telecommunication cable"
    }}}},
    "billing_line_items": [
        {{{{
            "line_item_index": 1,
            "description": "PICK UP OF THIS SHIPMENT Greater of (Min Rate USD 125.22, 7.791 Cubic Metre(s) @ USD 44.70/M3) USD 348.26 @ 0.624795",
            "amount": 557.40,
            "gst_amount": null,
            "total_amount": null,
            "currency": "AUD",
            "category": "FREIGHT"
        }}}},
        {{{{
            "line_item_index": 2,
            "description": "ORIGIN CHARGES ALL IN Greater of (Min Rate USD 199.66, 7.791 Cubic Metre(s) @ USD $55.78/M3)$ USD 434.58 @ 0.624795",
            "amount": 695.56,
            "gst_amount": null,
            "total_amount": null,
            "currency": "AUD",
            "category": "SERVICE_CHARGE"
        }}}},
        {{{{
            "line_item_index": 3,
            "description": "Freight Greater of (Min Rate USD 55.00, 7.791 Cubic Metre(s) @ USD 55.00/M3) USD 428.51 @ 0.624795",
            "amount": 685.84,
            "gst_amount": null,
            "total_amount": null,
            "currency": "AUD",
            "category": "FREIGHT"
        }}}},
        {{{{
            "line_item_index": 4,
            "description": "DELIVERY ALL IN Greater of (Min Rate USD 31.10, 7.791 Cubic Metre(s) @ USD 31.10/M3) USD 242.30 @ 0.624795",
            "amount": 387.81,
            "gst_amount": null,
            "total_amount": null,
            "currency": "AUD",
            "category": "SERVICE_CHARGE"
        }}}},
        {{{{
            "line_item_index": 5,
            "description": "DESTINATION CHARGES ALL IN Greater of (Min Rate USD 192.78, 7.791 Cubic Metre(s) @ USD 80.33/M3) USD 625.85 @ 0.624795",
            "amount": 1001.69,
            "gst_amount": null,
            "total_amount": null,
            "currency": "AUD",
            "category": "SERVICE_CHARGE"
        }}}}
    ]
}}}}

EXAMPLE 2 - Single line item with EUR conversion:
INPUT: Invoice D2133359, OUTDOOR WIRELESS NETWORKS AUSTRALIA PTY LTD, 2025-06-25, Due: 2025-09-23, AUD 326.79, Rotterdam to Sydney, 72.000 KG, exchange rate EUR 0.55081

OUTPUT:
{{{{
    "invoice_summary": {{{{
        "invoice_no": "D2133359",
        "invoice_date": "2025-06-25",
        "due_date": "2025-09-23",
        "customer_name": "OUTDOOR WIRELESS NETWORKS AUSTRALIA PTY LTD",
        "currency": "AUD",
        "subtotal": 326.79,
        "gst_total": 0.0,
        "final_total": 326.79,
        "service_type": "ROAD REFERENCE",
        "origin": "NLRTM = Rotterdam, Netherlands",
        "destination": "AUSYD = Sydney, Australia",
        "weight": "72.000 KG",
        "shipment_ref": "S2501307888",
        "account_number": "AUOU0003/AUOU0003",
        "payment_terms": "90 days from Inv. Date",
        "incoterms": null,
        "transportation_mode": null,
        "masterbill": null,
        "housebill": "RTMB13675",
        "awb_number": null,
        "shipment_date": "2025-04-11",
        "total_pieces": 1,
        "chargeable_weight": 0.335,
        "volume_weight": 0.335,
        "exchange_rate_eur": 0.55081,
        "exchange_rate_usd": null,
        "shipper_name": "COMMSCOPE TECHNOLOGIES UK",
        "shipper_address": null,
        "consignee_name": "OUTDOOR WIRELESS NETWORKS AUSTRALIA PTY LTD",
        "consignee_address": null,
        "commodity_description": "TELECOMMUNICATION EQUIPMENT"
    }}}},
    "billing_line_items": [
        {{{{
            "line_item_index": 1,
            "description": "Org. Fumigation Fee EUR 180.00 @ 0.550810",
            "amount": 326.79,
            "gst_amount": null,
            "total_amount": null,
            "currency": "AUD",
            "category": "SERVICE_CHARGE"
        }}}}
    ]
}}}}

EXAMPLE 3 - Invoice with GST:
INPUT: Invoice D2133868, OUTDOOR WIRELESS NETWORKS AUSTRALIA PTY LTD, 2025-06-25, Due: 2025-09-23, AUD 103.13, Shanghai to Sydney, 7779.400 KG, subtotal 93.75, GST 9.38, Dest. Waiting Time Fee

OUTPUT:
{{{{
    "invoice_summary": {{{{
        "invoice_no": "D2133868",
        "invoice_date": "2025-06-25",
        "due_date": "2025-09-23",
        "customer_name": "OUTDOOR WIRELESS NETWORKS AUSTRALIA PTY LTD",
        "currency": "AUD",
        "subtotal": 93.75,
        "gst_total": 9.38,
        "final_total": 103.13,
        "service_type": null,
        "origin": "CNSHA = Shanghai, China",
        "destination": "AUSYD = Sydney, Australia",
        "weight": "7779.400 KG",
        "shipment_ref": "S2502322386",
        "account_number": "AUOU0003/AUOU0003",
        "payment_terms": "90 days from Inv. Date",
        "incoterms": null,
        "transportation_mode": null,
        "masterbill": "757510358700",
        "housebill": "SZVC11136",
        "awb_number": null,
        "shipment_date": "2025-06-01",
        "total_pieces": 40,
        "chargeable_weight": 52.075,
        "volume_weight": 52.075,
        "exchange_rate_eur": null,
        "exchange_rate_usd": null,
        "shipper_name": "COMMSCOPE TELECOMMUNICATIONS CHINA CO LTD",
        "shipper_address": null,
        "consignee_name": "OUTDOOR WIRELESS NETWORKS AUSTRALIA PTY LTD",
        "consignee_address": null,
        "commodity_description": "ANTENNA"
    }}}},
    "billing_line_items": [
        {{{{
            "line_item_index": 1,
            "description": "Dest. Waiting Time Fee 45.00 Mins",
            "amount": 93.75,
            "gst_amount": 9.38,
            "total_amount": 103.13,
            "currency": "AUD",
            "category": "SERVICE_CHARGE"
        }}}}
    ]
}}}}

CLASSIFICATION RULES:
- "PICK UP", "Freight" = FREIGHT category
- "ORIGIN CHARGES", "DELIVERY", "DESTINATION CHARGES", "Fumigation Fee", "Waiting Time" = SERVICE_CHARGE category
- "FUEL", "FUEL ADJUSTMENT" = FUEL_SURCHARGE category
- "SECURITY", "SCREENING" = SECURITY_CHARGE category
- "DUTY", "TAX", "GST" = DUTY_TAX category
- "SURCHARGE" = SURCHARGE category
- Other charges = OTHER category
- Extract exact description text including calculations and exchange rates
- Convert all dates to YYYY-MM-DD format
- Preserve numeric precision for amounts and exchange rates
- Use null for missing values, not empty strings

NOW EXTRACT FROM THIS INVOICE USING THE EXACT SAME FORMAT AND CLASSIFICATION PATTERNS:

{pdf_text}

RESPONSE (JSON only, no explanations):"""
    
    def batch_reprocess(self, limit: int = 100, dry_run: bool = False) -> Dict:
        """
        Batch reprocess incomplete extractions
        """
        print(f"🚀 Starting batch reprocessing of incomplete invoice extractions...")
        print(f"📊 Using model: {self.model_name}")
        print(f"🆔 Batch ID: {self.batch_id}")
        
        # Test LLM connection first
        connection_test = self.processor.test_llm_connection()
        if not connection_test.get('success', False):
            print(f"❌ LLM connection test failed: {connection_test.get('error', 'Unknown error')}")
            return {
                'success': False,
                'error': f"LLM connection test failed: {connection_test.get('error', 'Unknown error')}"
            }
            
        print(f"✅ LLM connection test successful")
        
        # Identify incomplete extractions
        incomplete_invoices = self.identify_incomplete_extractions(limit=limit)
        
        if not incomplete_invoices:
            print("✅ No incomplete extractions found")
            return {
                'success': True,
                'message': 'No incomplete extractions found',
                'stats': self.stats
            }
            
        if dry_run:
            print(f"🔍 DRY RUN: Would reprocess {len(incomplete_invoices)} invoices")
            return {
                'success': True,
                'message': f'DRY RUN: Would reprocess {len(incomplete_invoices)} invoices',
                'invoices': [inv['invoice_no'] for inv in incomplete_invoices],
                'stats': self.stats
            }
        
        # Reprocess each invoice
        start_time = time.time()
        results = []
        
        for invoice in incomplete_invoices:
            try:
                invoice_no = invoice['invoice_no']
                print(f"\n🔄 Processing invoice {invoice_no} ({self.reprocessed_count + 1}/{len(incomplete_invoices)})")
                print(f"   Missing fields: {', '.join(invoice['missing_fields'])}")
                
                result = self.reprocess_invoice(invoice)
                results.append(result)
                
                self.reprocessed_count += 1
                if result.get('success', False):
                    self.stats['successfully_reprocessed'] += 1
                else:
                    self.stats['failures'] += 1
                    
                # Add a small delay to avoid overloading the LLM service
                time.sleep(1)
                
            except Exception as e:
                print(f"❌ Unexpected error processing invoice: {e}")
                self.stats['failures'] += 1
                results.append({
                    'invoice_no': invoice.get('invoice_no', 'Unknown'),
                    'success': False,
                    'error': str(e)
                })
        
        processing_time = time.time() - start_time
        self.stats['processing_time'] = processing_time
        
        print(f"\n✅ Batch reprocessing complete!")
        print(f"📊 Stats:")
        print(f"   Total identified: {self.stats['total_identified']}")
        print(f"   Successfully reprocessed: {self.stats['successfully_reprocessed']}")
        print(f"   Failures: {self.stats['failures']}")
        print(f"   Processing time: {processing_time:.2f} seconds")
        
        return {
            'success': True,
            'batch_id': self.batch_id,
            'results': results,
            'stats': self.stats
        }

def main():
    """Main function to run batch reprocessing"""
    parser = argparse.ArgumentParser(description='Batch reprocess incomplete DHL invoice extractions')
    parser.add_argument('--limit', type=int, default=10, help='Maximum number of invoices to reprocess')
    parser.add_argument('--dry-run', action='store_true', help='Perform a dry run without updating the database')
    parser.add_argument('--model', type=str, default='llama3.2:latest', help='LLM model to use')
    parser.add_argument('--db-path', type=str, default='dhl_audit.db', help='Path to SQLite database')
    
    args = parser.parse_args()
    
    reprocessor = InvoiceReprocessor(db_path=args.db_path, model_name=args.model)
    result = reprocessor.batch_reprocess(limit=args.limit, dry_run=args.dry_run)
    
    if result.get('success', False):
        print("✅ Batch reprocessing completed successfully")
    else:
        print(f"❌ Batch reprocessing failed: {result.get('error', 'Unknown error')}")
    
if __name__ == "__main__":
    main()
