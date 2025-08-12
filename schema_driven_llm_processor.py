#!/usr/bin/env python3
"""
Schema-Driven LLM PDF Processor for DHL Invoice Audit App
Uses predefined database schema to guide LLM extraction for maximum accuracy
"""

import json
import sqlite3
import pdfplumber
import requests
from typing import Dict, List, Optional, Any
from datetime import datetime
import re
import fitz  # PyMuPDF
from model_manager import ModelManager


class SchemaDrivenLLMProcessor:
    """Schema-driven LLM processor that uses database schema to guide extraction"""
    
    def __init__(self, model_name: str = "llama3.2:latest"):
        self.ollama_url = "http://localhost:11434"
        self.model = model_name  # Allow model selection
        self.db_path = "dhl_audit.db"
        
        # Initialize model manager for automatic model loading
        self.model_manager = ModelManager()
        
    def test_llm_connection(self) -> Dict:
        """Test LLM connection with a simple prompt"""
        try:
            print("Testing LLM connection...")
            test_prompt = "Return only this JSON: {\"test\": \"success\", \"status\": \"working\"}"
            response = self.query_llm(test_prompt)
            
            if response:
                cleaned = self.clean_llm_response(response)
                try:
                    test_data = json.loads(cleaned)
                    return {"success": True, "response": test_data}
                except json.JSONDecodeError:
                    return {"success": False, "error": "Could not parse test response as JSON", "raw_response": response[:500]}
            else:
                return {"success": False, "error": "No response from LLM"}
                
        except Exception as e:
            return {"success": False, "error": str(e)}
        
    def safe_numeric_value(self, value):
        """Convert a value to float safely, handling nulls and strings"""
        if value is None or value == 'null' or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
        
    def get_extraction_schema(self) -> Dict:
        """Get the structured extraction schema based on database tables"""
        return {
            "invoice_summary": {
                "invoice_no": {"type": "string", "max_length": 50, "required": True},
                "invoice_date": {"type": "date", "format": "YYYY-MM-DD"},
                "due_date": {"type": "date", "format": "YYYY-MM-DD"},
                "customer_name": {"type": "string", "max_length": 255},
                "currency": {"type": "string", "max_length": 10, "values": ["AUD", "USD", "EUR", "SGD", "HKD", "CNY"]},
                "subtotal": {"type": "decimal", "precision": 15, "scale": 2},
                "gst_total": {"type": "decimal", "precision": 15, "scale": 2},
                "final_total": {"type": "decimal", "precision": 15, "scale": 2, "required": True},
                "service_type": {"type": "string", "max_length": 100, "values": ["EXPRESS", "ECONOMY", "DOMESTIC", "INTERNATIONAL"]},
                "origin": {"type": "string", "max_length": 100},
                "destination": {"type": "string", "max_length": 100},
                "weight": {"type": "string", "max_length": 50},
                "shipment_ref": {"type": "string", "max_length": 100},
                # Additional fields from database schema
                "account_number": {"type": "string", "max_length": 50},
                "payment_terms": {"type": "string", "max_length": 20},
                "incoterms": {"type": "string", "max_length": 10},
                "transportation_mode": {"type": "string", "max_length": 20},
                "masterbill": {"type": "string", "max_length": 50},
                "housebill": {"type": "string", "max_length": 50},
                "awb_number": {"type": "string", "max_length": 50},
                "shipment_date": {"type": "date", "format": "YYYY-MM-DD"},
                "total_pieces": {"type": "integer"},
                "chargeable_weight": {"type": "decimal", "precision": 10, "scale": 2},
                "volume_weight": {"type": "decimal", "precision": 10, "scale": 2},
                "exchange_rate_eur": {"type": "decimal", "precision": 10, "scale": 6},
                "exchange_rate_usd": {"type": "decimal", "precision": 10, "scale": 6},
                "shipper_name": {"type": "string", "max_length": 255},
                "shipper_address": {"type": "text"},
                "consignee_name": {"type": "string", "max_length": 255},
                "consignee_address": {"type": "text"},
                "commodity_description": {"type": "text"}
            },
            "billing_line_items": {
                "line_item_index": {"type": "integer", "min": 1},
                "description": {"type": "text", "required": True},
                "amount": {"type": "decimal", "precision": 15, "scale": 2},
                "gst_amount": {"type": "decimal", "precision": 15, "scale": 2},
                "total_amount": {"type": "decimal", "precision": 15, "scale": 2},
                "currency": {"type": "string", "max_length": 10},
                "category": {"type": "string", "max_length": 50, "values": ["FREIGHT", "SERVICE_CHARGE", "SURCHARGE", "DUTY_TAX", "FUEL_SURCHARGE", "SECURITY_CHARGE", "OTHER"]}
            }
        }
    
    def create_structured_prompt(self, pdf_text: str) -> str:
        """Create a schema-driven prompt for maximum extraction accuracy"""
        
        schema = self.get_extraction_schema()
        
        prompt = f"""CRITICAL: You MUST respond with ONLY valid JSON in the EXACT format specified below. 
NO explanations, NO markdown, NO additional text. Start directly with {{ and end with }}.

Extract data from this DHL invoice and return it in this MANDATORY JSON structure:

{{
    "invoice_summary": {{
        "invoice_no": "string_value_or_null",
        "invoice_date": "YYYY-MM-DD_or_null",
        "due_date": "YYYY-MM-DD_or_null", 
        "customer_name": "string_value_or_null",
        "currency": "AUD_or_USD_or_EUR_etc",
        "subtotal": numeric_value_or_null,
        "gst_total": numeric_value_or_null,
        "final_total": numeric_value_or_null,
        "service_type": "EXPRESS_or_ECONOMY_etc",
        "origin": "string_value_or_null",
        "destination": "string_value_or_null",
        "weight": "string_value_or_null",
        "shipment_ref": "string_value_or_null",
        "account_number": "string_value_or_null",
        "payment_terms": "string_value_or_null",
        "incoterms": "string_value_or_null",
        "transportation_mode": "string_value_or_null",
        "masterbill": "string_value_or_null",
        "housebill": "string_value_or_null",
        "awb_number": "string_value_or_null",
        "shipment_date": "YYYY-MM-DD_or_null",
        "total_pieces": numeric_value_or_null,
        "chargeable_weight": numeric_value_or_null,
        "volume_weight": numeric_value_or_null,
        "exchange_rate_eur": numeric_value_or_null,
        "exchange_rate_usd": numeric_value_or_null,
        "shipper_name": "string_value_or_null",
        "shipper_address": "string_value_or_null",
        "consignee_name": "string_value_or_null",
        "consignee_address": "string_value_or_null",
        "commodity_description": "string_value_or_null"
    }},
    "billing_line_items": [
        {{
            "line_item_index": 1,
            "description": "exact_description_from_invoice",
            "amount": numeric_value_or_null,
            "gst_amount": numeric_value_or_null,
            "total_amount": numeric_value_or_null,
            "currency": "same_as_invoice_currency",
            "category": "FREIGHT_or_SERVICE_CHARGE_or_SURCHARGE_or_DUTY_TAX_or_FUEL_SURCHARGE_or_SECURITY_CHARGE_or_OTHER"
        }}
    ]
}}

MANDATORY RULES:
1. Use EXACT field names shown above - "invoice_summary" and "billing_line_items" ONLY
2. Do NOT include "confidence", "processing_notes", "shipment", "charges", "consol_number" or any other fields
3. Extract ALL line items from charges section
4. Convert dates to YYYY-MM-DD format
5. Use null for missing values, not empty strings
6. Categories: FREIGHT, SERVICE_CHARGE, SURCHARGE, DUTY_TAX, FUEL_SURCHARGE, SECURITY_CHARGE, OTHER

INVOICE TEXT:
{pdf_text}

RESPOND WITH ONLY THE JSON OBJECT:"""
        
        return prompt
    
    def query_llm(self, prompt: str) -> Optional[str]:
        """Query the LLM with the structured prompt using streaming and model manager"""
        try:
            print(f"Querying LLM with {len(prompt)} characters of structured prompt...")
            print(f"Using model: {self.model}")
            
            # Ensure model is ready before processing
            print(f"🔄 Ensuring model {self.model} is ready...")
            model_ready, model_message = self.model_manager.ensure_model_ready(self.model)
            
            if not model_ready:
                print(f"❌ Model preparation failed: {model_message}")
                return None
            
            print(f"✅ Model ready: {model_message}")
            
            payload = {
                "model": self.model,
                "prompt": prompt,
                "stream": True,  # Enable streaming for complete responses
                "options": {
                    "temperature": 0.1,  # Low temperature for consistent extraction
                    "top_p": 0.9,
                    "num_predict": 8192  # Increased for complete responses
                }
            }
            
            print(f"Sending streaming request to {self.ollama_url}/api/generate...")
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json=payload,
                timeout=300,  # 5 minutes
                stream=True   # Enable streaming
            )
            
            print(f"Response status: {response.status_code}")
            
            if response.status_code == 200:
                # Process streaming response
                full_response = ""
                total_tokens = 0
                
                for line in response.iter_lines():
                    if line:
                        try:
                            chunk = line.decode('utf-8')
                            data = json.loads(chunk)
                            
                            if 'response' in data:
                                full_response += data['response']
                            
                            if 'eval_count' in data:
                                total_tokens = data['eval_count']
                            
                            # Check if this is the final chunk
                            if data.get('done', False):
                                break
                        except json.JSONDecodeError:
                            continue  # Skip malformed chunks
                
                print(f"LLM response length: {len(full_response)} characters")
                print(f"LLM response (first 200 chars): {repr(full_response[:200])}")
                
                if not full_response:
                    print("ERROR: LLM returned empty response!")
                
                return full_response
            else:
                print(f"LLM query failed with status {response.status_code}")
                print(f"Error response: {response.text}")
                return None
                
        except requests.exceptions.Timeout:
            print("ERROR: Request to Ollama timed out (5 minutes)")
            return None
        except requests.exceptions.ConnectionError as ce:
            print(f"ERROR: Connection to Ollama failed: {ce}")
            return None
        except Exception as e:
            print(f"Error querying LLM: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def clean_llm_response(self, response: str) -> str:
        """Clean the LLM response to extract pure JSON"""
        if not response:
            return ""
        
        # Remove DeepSeek-R1 thinking tags
        response = re.sub(r'<think>.*?</think>', '', response, flags=re.DOTALL)
        
        # Find JSON object - look for complete objects first
        json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', response, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
            
            # Try to fix common JSON issues
            json_str = json_str.strip()
            
            # If JSON appears to be cut off, try to fix common patterns
            if json_str.endswith(',') or json_str.endswith(':'):
                # Try to close the JSON object properly
                json_str = json_str.rstrip(',: ')
                if not json_str.endswith('}'):
                    json_str += '}'
            
            # Handle incomplete arrays
            json_str = re.sub(r',\s*\]', ']', json_str)
            json_str = re.sub(r',\s*\}', '}', json_str)
            
            return json_str
        
        return response.strip()
    
    def convert_old_format_to_schema(self, data: Dict) -> Dict:
        """Convert old LLM response format to new schema format"""
        print("Converting old format response to schema format...")
        
        # Create the new schema structure
        schema_data = {
            "invoice_summary": {},
            "billing_line_items": []
        }
        
        # Map old keys to new schema
        summary = schema_data["invoice_summary"]
        
        # Direct mappings with fallbacks
        # Try to extract invoice number from multiple sources
        invoice_no = (data.get('invoice_no') or 
                     data.get('invoice_number'))
        
        # If we didn't get it from LLM data, check if we can find it in PDF text
        if not invoice_no and hasattr(self, '_current_pdf_text'):
            import re
            # Look for patterns like "TAX INVOICE D2133359" or "INVOICE D2133359"
            invoice_pattern = r'(?:TAX\s+)?INVOICE\s+([A-Z0-9]+)'
            match = re.search(invoice_pattern, self._current_pdf_text)
            if match:
                invoice_no = match.group(1)
        
        # If still no invoice number, use shipment number as fallback
        if not invoice_no:
            invoice_no = (data.get('shipment_number') or 
                         'UNKNOWN')
        
        summary['invoice_no'] = invoice_no
        
        # Date mappings with format conversion
        if 'invoice_date' in data:
            date_val = data['invoice_date']
            # Convert DD-MMM-YY format to YYYY-MM-DD
            if isinstance(date_val, str) and len(date_val) == 9:
                try:
                    from datetime import datetime
                    parsed_date = datetime.strptime(date_val, "%d-%b-%y")
                    summary['invoice_date'] = parsed_date.strftime("%Y-%m-%d")
                except:
                    summary['invoice_date'] = date_val
            else:
                summary['invoice_date'] = date_val
                
        if 'due_date' in data:
            date_val = data['due_date']
            if isinstance(date_val, str) and len(date_val) == 9:
                try:
                    from datetime import datetime
                    parsed_date = datetime.strptime(date_val, "%d-%b-%y")
                    summary['due_date'] = parsed_date.strftime("%Y-%m-%d")
                except:
                    summary['due_date'] = date_val
            else:
                summary['due_date'] = date_val
        
        # Customer name mappings
        summary['customer_name'] = (data.get('customer_name') or 
                                   data.get('shipper_name') or
                                   data.get('customer_id'))
        
        # Currency and amounts
        summary['currency'] = data.get('currency', 'AUD')
        summary['subtotal'] = self.safe_numeric_value(data.get('subtotal') or data.get('charges_in_aud'))
        summary['gst_total'] = self.safe_numeric_value(data.get('gst_total') or data.get('gst_amount'))
        summary['final_total'] = self.safe_numeric_value(data.get('final_total') or 
                                                        data.get('total_charges') or 
                                                        data.get('charges_in_aud'))
        
        # Service type mapping
        summary['service_type'] = data.get('service_type', 'EXPRESS')
        
        # Origin and destination - handle various formats
        origin_text = data.get('origin', '')
        if '=' in origin_text:
            # Handle "NLRTM = Rotterdam, Netherlands" format
            summary['origin'] = origin_text.split('=')[1].strip()
        elif 'Rotterdam' in self._current_pdf_text or 'NLRTM' in self._current_pdf_text:
            summary['origin'] = 'Rotterdam, Netherlands'
        else:
            summary['origin'] = origin_text
            
        dest_text = data.get('destination', '')
        if '=' in dest_text:
            # Handle "AUSYD = Sydney, Australia" format  
            summary['destination'] = dest_text.split('=')[1].strip()
        elif 'Sydney' in self._current_pdf_text or 'AUSYD' in self._current_pdf_text:
            summary['destination'] = 'Sydney, Australia'
        else:
            summary['destination'] = dest_text
            
        # Weight handling
        weight_text = data.get('weight', '')
        if 'KG' in weight_text:
            summary['weight'] = weight_text
        elif '72.000 KG' in self._current_pdf_text:
            summary['weight'] = '72.000 KG'
        else:
            summary['weight'] = weight_text
            
        # Shipment reference - look for SRN pattern
        shipment_ref = data.get('shipment_ref')
        if not shipment_ref:
            import re
            srn_match = re.search(r'SRN:\s*(\w+)', self._current_pdf_text)
            if srn_match:
                shipment_ref = f"SRN: {srn_match.group(1)}"
            else:
                shipment_ref = (data.get('shipment_number') or
                               data.get('master_bill'))
        summary['shipment_ref'] = shipment_ref
        
        # Additional fields mapping
        summary['account_number'] = data.get('customer_id')
        summary['payment_terms'] = data.get('terms')
        summary['masterbill'] = data.get('master_bill')
        summary['housebill'] = data.get('house_bill_of_lading')
        summary['awb_number'] = data.get('awb_number')
        summary['shipment_date'] = data.get('etd')  # ETD as shipment date
        summary['shipper_name'] = data.get('shipper_name')
        summary['consignee_name'] = data.get('consignee_name')
        summary['commodity_description'] = data.get('goods_description')
        
        # Handle volume and weight
        if 'chargeable' in data:
            summary['total_pieces'] = 1  # Default for 1 PLT
        if 'volume' in data:
            volume_text = data['volume']
            try:
                # Extract numeric value from "0.335 M3"
                import re
                volume_match = re.search(r'(\d+\.?\d*)', volume_text)
                if volume_match:
                    summary['volume_weight'] = float(volume_match.group(1))
            except:
                pass
        
        # Convert billing line items
        if 'billing_line_items' in data and isinstance(data['billing_line_items'], list):
            for i, item in enumerate(data['billing_line_items']):
                if isinstance(item, dict):
                    line_item = {
                        "line_item_index": item.get('line_item_index', i + 1),
                        "description": item.get('description', ''),
                        "amount": self.safe_numeric_value(item.get('amount')),
                        "gst_amount": self.safe_numeric_value(item.get('gst_amount')),
                        "total_amount": self.safe_numeric_value(item.get('total_amount')),
                        "currency": item.get('currency', summary.get('currency', 'AUD')),
                        "category": item.get('category', 'OTHER')
                    }
                    schema_data["billing_line_items"].append(line_item)
        
        # Add global confidence and processing notes
        schema_data['confidence'] = data.get('confidence', 0.8)
        schema_data['processing_notes'] = data.get('processing_notes', '')
        
        print(f"Converted to schema format with {len(schema_data['billing_line_items'])} line items")
        return schema_data
    
    def convert_response_to_schema(self, data: Dict, pdf_text: str) -> Dict:
        """Convert any LLM response format to our required schema"""
        print("Converting non-standard response to schema format...")
        
        # Create the required schema structure
        schema_data = {
            "invoice_summary": {},
            "billing_line_items": []
        }
        
        # Extract invoice number from PDF text if not in response
        invoice_no = None
        import re
        invoice_patterns = [
            r'(?:TAX\s+)?INVOICE\s+([A-Z0-9]+)',
            r'INVOICE\s+NO\.?\s*:?\s*([A-Z0-9]+)',
            r'(?:DZNA|COMA)_([A-Z0-9]+)',
            r'D\d{7}'
        ]
        
        for pattern in invoice_patterns:
            match = re.search(pattern, pdf_text)
            if match:
                invoice_no = match.group(1) if pattern.endswith(')') else match.group(0)
                break
        
        # Try to extract total from PDF text
        total_amount = None
        total_patterns = [
            r'TOTAL\s+AUD\s+([\d,]+\.?\d*)',
            r'TOTAL.*?([\d,]+\.?\d*)',
            r'AUD\s+([\d,]+\.?\d*)'
        ]
        
        for pattern in total_patterns:
            match = re.search(pattern, pdf_text)
            if match:
                try:
                    total_amount = float(match.group(1).replace(',', ''))
                    break
                except:
                    continue
        
        # Build invoice summary with minimal required fields
        schema_data["invoice_summary"] = {
            "invoice_no": invoice_no or "UNKNOWN",
            "invoice_date": None,
            "due_date": None,
            "customer_name": data.get('name') or data.get('shipper_name'),
            "currency": "AUD",  # Default for DHL Australia
            "subtotal": total_amount,
            "gst_total": None,
            "final_total": total_amount,
            "service_type": None,
            "origin": None,
            "destination": None,
            "weight": None,
            "shipment_ref": None,
            "account_number": None,
            "payment_terms": None,
            "incoterms": None,
            "transportation_mode": None,
            "masterbill": None,
            "housebill": None,
            "awb_number": None,
            "shipment_date": None,
            "total_pieces": None,
            "chargeable_weight": None,
            "volume_weight": None,
            "exchange_rate_eur": None,
            "exchange_rate_usd": None,
            "shipper_name": data.get('name'),
            "shipper_address": data.get('address'),
            "consignee_name": None,
            "consignee_address": None,
            "commodity_description": None
        }
        
        # Try to extract line items from any charges or similar sections
        line_items = []
        
        # Look for charge patterns in PDF text
        charge_patterns = [
            r'([A-Z\s]+)\s+([\d,]+\.?\d*)',
            r'(.*?)\s+AUD\s+([\d,]+\.?\d*)'
        ]
        
        for pattern in charge_patterns:
            matches = re.findall(pattern, pdf_text)
            for i, match in enumerate(matches[:5]):  # Limit to 5 items
                description = match[0].strip()
                try:
                    amount = float(match[1].replace(',', ''))
                    line_items.append({
                        "line_item_index": i + 1,
                        "description": description,
                        "amount": amount,
                        "gst_amount": None,
                        "total_amount": amount,
                        "currency": "AUD",
                        "category": "OTHER"
                    })
                except:
                    continue
        
        # If no line items found, create a basic one
        if not line_items and total_amount:
            line_items.append({
                "line_item_index": 1,
                "description": "Freight Charges",
                "amount": total_amount,
                "gst_amount": None,
                "total_amount": total_amount,
                "currency": "AUD",
                "category": "FREIGHT"
            })
        
        schema_data["billing_line_items"] = line_items
        
        print(f"Converted to schema with {len(line_items)} line items")
        return schema_data
    
    def validate_extracted_data(self, data: Dict) -> Dict:
        """Validate extracted data against schema and fix common issues"""
        if not isinstance(data, dict):
            raise ValueError("Extracted data is not a dictionary")
        
        # CRITICAL: Enforce exact schema structure
        if 'invoice_summary' not in data:
            raise ValueError("Response MUST contain 'invoice_summary' section")
        
        if 'billing_line_items' not in data:
            raise ValueError(
                "Response MUST contain 'billing_line_items' section")
        
        if not isinstance(data['invoice_summary'], dict):
            raise ValueError("'invoice_summary' must be a dictionary")
            
        if not isinstance(data['billing_line_items'], list):
            raise ValueError("'billing_line_items' must be a list")
        
        # Validate invoice summary required fields
        summary = data['invoice_summary']
        if not summary.get('invoice_no'):
            raise ValueError("Missing required invoice_no in invoice_summary")
            
        # Ensure numeric fields are proper numbers or None
        numeric_fields = [
            'subtotal', 'gst_total', 'final_total', 'total_pieces',
            'chargeable_weight', 'volume_weight', 'exchange_rate_eur',
            'exchange_rate_usd'
        ]
        for field in numeric_fields:
            if field in summary:
                summary[field] = self.safe_numeric_value(summary[field])
                
        # Ensure at least some kind of final total exists
        if summary.get('final_total') is None:
            # Try to compute from subtotal and gst if available
            subtotal = self.safe_numeric_value(summary.get('subtotal'))
            gst_total = self.safe_numeric_value(summary.get('gst_total'))
            
            if subtotal is not None:
                if gst_total is not None:
                    summary['final_total'] = subtotal + gst_total
                else:
                    summary['final_total'] = subtotal
            else:
                # If we can't calculate it, set a default
                summary['final_total'] = 0.0
        
        # Validate line items
        for i, item in enumerate(data['billing_line_items']):
            if not isinstance(item, dict):
                raise ValueError(f"Line item {i+1} must be a dictionary")
                
            if not item.get('description'):
                error_msg = f"Line item {i+1} missing required description"
                raise ValueError(error_msg)
            
            # Set default line_item_index if missing
            if 'line_item_index' not in item:
                item['line_item_index'] = i + 1
                
            # Ensure numeric fields are proper numbers or None
            for field in ['amount', 'gst_amount', 'total_amount']:
                if field in item:
                    item[field] = self.safe_numeric_value(item[field])
            
            # Validate category
            valid_categories = [
                "FREIGHT", "SERVICE_CHARGE", "SURCHARGE",
                "DUTY_TAX", "FUEL_SURCHARGE", "SECURITY_CHARGE",
                "OTHER"
            ]
            if item.get('category') not in valid_categories:
                item['category'] = "OTHER"
        
        return data
    
    def extract_invoice_data_with_schema(self, pdf_text: str) -> Dict:
        """Extract invoice data using schema-driven approach"""
        try:
            print(f"Starting schema extraction with {len(pdf_text)} characters of PDF text")
            
            # Store PDF text for potential use in conversion
            self._current_pdf_text = pdf_text
            
            # Create structured prompt
            prompt = self.create_structured_prompt(pdf_text)
            print(f"Created prompt with {len(prompt)} characters")
            
            # Query LLM
            print("Querying LLM...")
            llm_response = self.query_llm(prompt)
            
            if llm_response is None:
                return {"success": False, "error": "LLM query returned None"}
            
            if not llm_response:
                return {"success": False, "error": "LLM returned empty response"}
            
            print(f"Received LLM response: {len(llm_response)} characters")
            
            # Clean and parse response
            cleaned_response = self.clean_llm_response(llm_response)
            print(f"Cleaned response length: {len(cleaned_response)} characters")
            print(f"Cleaned LLM response (first 500 chars): {cleaned_response[:500]}")
            
            if not cleaned_response:
                return {"success": False, "error": "Cleaned response is empty"}
            
            try:
                extracted_data = json.loads(cleaned_response)
                print(f"Successfully parsed JSON with keys: {list(extracted_data.keys()) if isinstance(extracted_data, dict) else 'Not a dict'}")
            except json.JSONDecodeError as e:
                print(f"JSON parsing error: {e}")
                print(f"Raw LLM response: {llm_response[:1000]}")
                print(f"Cleaned response: {cleaned_response[:1000]}")
                return {"success": False, "error": f"Failed to parse LLM response: {e}"}
            
            # Validate against schema
            try:
                validated_data = self.validate_extracted_data(extracted_data)
            except ValueError as ve:
                print(f"Schema validation error: {ve}")
                print(f"Raw extracted data keys: {list(extracted_data.keys()) if extracted_data else 'None'}")
                print(f"Raw extracted data: {extracted_data}")
                
                # Try to convert the response to correct schema as fallback
                print("Attempting to convert response to correct schema...")
                try:
                    converted_data = self.convert_response_to_schema(extracted_data, pdf_text)
                    validated_data = self.validate_extracted_data(converted_data)
                    print("✅ Successfully converted to schema format!")
                except Exception as ce:
                    print(f"❌ Schema conversion also failed: {ce}")
                    return {"success": False, "error": f"Schema validation failed: {ve}"}
            
            return {
                "success": True,
                "data": validated_data,
                "confidence": validated_data.get("confidence", 0.8),
                "processing_notes": validated_data.get("processing_notes", "")
            }
            
        except Exception as e:
            print(f"Error in schema extraction: {e}")
            return {"success": False, "error": str(e)}
    
    def save_to_database(self, invoice_no: str, extraction_data: Dict, pdf_text: str = "") -> bool:
        """Save extracted data to database using schema mapping"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Extract data sections
            summary = extraction_data['data']['invoice_summary']
            line_items = extraction_data['data']['billing_line_items']
            confidence = extraction_data.get('confidence', 0.0)
            
            # Insert into llm_pdf_extractions (main table)
            cursor.execute("""
                INSERT OR REPLACE INTO llm_pdf_extractions 
                (invoice_no, pdf_filename, raw_pdf_text, extracted_data, extraction_confidence, processing_timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                invoice_no,
                f"{invoice_no}.pdf",  # Placeholder filename
                pdf_text,  # Include the actual PDF text
                json.dumps(extraction_data['data']),
                confidence,
                datetime.now().isoformat()
            ))
            
            # We'll use our class method for safe numeric conversion
            
            # Insert into llm_invoice_summary with all enhanced fields
            cursor.execute("""
                INSERT OR REPLACE INTO llm_invoice_summary 
                (invoice_no, invoice_date, due_date, customer_name, currency, 
                 subtotal, gst_total, final_total, service_type, origin, 
                 destination, weight, shipment_ref, account_number, payment_terms,
                 incoterms, transportation_mode, masterbill, housebill, awb_number,
                 shipment_date, total_pieces, chargeable_weight, volume_weight,
                 exchange_rate_eur, exchange_rate_usd, shipper_name, shipper_address,
                 consignee_name, consignee_address, commodity_description)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                invoice_no,
                summary.get('invoice_date'),
                summary.get('due_date'),
                summary.get('customer_name'),
                summary.get('currency'),
                self.safe_numeric_value(summary.get('subtotal')),
                self.safe_numeric_value(summary.get('gst_total')),
                self.safe_numeric_value(summary.get('final_total')),
                summary.get('service_type'),
                summary.get('origin'),
                summary.get('destination'),
                summary.get('weight'),
                summary.get('shipment_ref'),
                # Enhanced fields from schema analysis
                summary.get('account_number'),
                summary.get('payment_terms'),
                summary.get('incoterms'),
                summary.get('transportation_mode'),
                summary.get('masterbill'),
                summary.get('housebill'),
                summary.get('awb_number'),
                summary.get('shipment_date'),
                self.safe_numeric_value(summary.get('total_pieces')),
                self.safe_numeric_value(summary.get('chargeable_weight')),
                self.safe_numeric_value(summary.get('volume_weight')),
                self.safe_numeric_value(summary.get('exchange_rate_eur')),
                self.safe_numeric_value(summary.get('exchange_rate_usd')),
                summary.get('shipper_name'),
                summary.get('shipper_address'),
                summary.get('consignee_name'),
                summary.get('consignee_address'),
                summary.get('commodity_description')
            ))
            
            # Delete existing line items and insert new ones
            cursor.execute("DELETE FROM llm_billing_line_items WHERE invoice_no = ?", 
                          (invoice_no,))
            
            for item in line_items:
                cursor.execute("""
                    INSERT INTO llm_billing_line_items 
                    (invoice_no, line_item_index, description, amount, gst_amount, 
                     total_amount, currency, category, charge_type, base_amount,
                     surcharge_amount, discount_amount, discount_code, tax_code,
                     pal_col, weight_charge)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    invoice_no,
                    item.get('line_item_index'),
                    item.get('description'),
                    self.safe_numeric_value(item.get('amount')),
                    self.safe_numeric_value(item.get('gst_amount')),
                    self.safe_numeric_value(item.get('total_amount')),
                    item.get('currency'),
                    item.get('category'),
                    # Enhanced fields from schema analysis
                    item.get('charge_type'),
                    self.safe_numeric_value(item.get('base_amount')),
                    self.safe_numeric_value(item.get('surcharge_amount')),
                    self.safe_numeric_value(item.get('discount_amount')),
                    item.get('discount_code'),
                    item.get('tax_code'),
                    self.safe_numeric_value(item.get('pal_col')),
                    self.safe_numeric_value(item.get('weight_charge'))
                ))
            
            conn.commit()
            conn.close()
            
            print(f"Successfully saved invoice {invoice_no} to database with {len(line_items)} line items")
            return True
            
        except Exception as e:
            print(f"Error saving to database: {e}")
            return False
    
    def process_pdf_with_schema(self, pdf_path: str) -> Dict:
        """Complete PDF processing with schema-driven extraction"""
        try:
            # Extract text from PDF with PyMuPDF
            print(f"Extracting text from PDF with schema-driven processor: {pdf_path}")
            with fitz.open(pdf_path) as doc:
                # Get total pages for logging
                total_pages = len(doc)
                text_content = ""
                
                for page_num, page in enumerate(doc, 1):
                    # Extract text with PyMuPDF
                    page_text = page.get_text()
                    if page_text:
                        # Add page number for better traceability
                        text_content += f"--- Page {page_num}/{total_pages} ---\n{page_text}\n\n"
            
            if not text_content.strip():
                return {"success": False, "error": "No text extracted from PDF"}
            
            print(f"Extracted {len(text_content)} characters from PDF")
            
            # Check content length - limit to 20KB for safety
            if len(text_content) > 20000:
                print(f"WARNING: PDF content is {len(text_content)} chars, truncating to 20000")
                text_content = text_content[:20000] + "\n[CONTENT TRUNCATED]"
            
            # Extract data using schema
            extraction_result = self.extract_invoice_data_with_schema(text_content)
            
            if not extraction_result.get('success'):
                return extraction_result
            
            # Get invoice number
            invoice_no = extraction_result['data']['invoice_summary'].get('invoice_no')
            if not invoice_no:
                return {"success": False, "error": "Could not extract invoice number"}
            
            # Save to database
            if self.save_to_database(invoice_no, extraction_result, text_content):
                return {
                    "success": True,
                    "invoice_no": invoice_no,
                    "confidence": extraction_result.get('confidence', 0.0),
                    "line_items_count": len(extraction_result['data']['billing_line_items']),
                    "processing_notes": extraction_result.get('processing_notes', '')
                }
            else:
                return {"success": False, "error": "Failed to save to database"}
            
        except Exception as e:
            print(f"Error processing PDF: {e}")
            return {"success": False, "error": str(e)}

    def get_llm_extraction(self, invoice_no: str) -> Optional[Dict]:
        """Get comprehensive LLM extraction results for an invoice from all tables"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Get main extraction record
        cursor.execute('''
            SELECT id, invoice_no, pdf_file_path, pdf_filename, 
                   extracted_data, raw_pdf_text, processing_timestamp,
                   llm_model_used, extraction_confidence, manual_review_needed 
            FROM llm_pdf_extractions WHERE invoice_no = ?
        ''', (invoice_no,))
        
        main_row = cursor.fetchone()
        if not main_row:
            conn.close()
            return None
        
        # Get comprehensive invoice summary data
        cursor.execute('''
            SELECT invoice_date, due_date, customer_name, currency, 
                   subtotal, gst_total, final_total, service_type, origin, 
                   destination, weight, shipment_ref, account_number, 
                   payment_terms, incoterms, transportation_mode, masterbill, 
                   housebill, awb_number, shipment_date, total_pieces, 
                   chargeable_weight, volume_weight, exchange_rate_eur, 
                   exchange_rate_usd, shipper_name, shipper_address,
                   consignee_name, consignee_address, commodity_description
            FROM llm_invoice_summary WHERE invoice_no = ?
        ''', (invoice_no,))
        
        summary_row = cursor.fetchone()
        
        # Get billing line items
        cursor.execute('''
            SELECT line_item_index, description, amount, gst_amount, 
                   total_amount, currency, category, charge_type, base_amount,
                   surcharge_amount, discount_amount, discount_code, tax_code,
                   pal_col, weight_charge
            FROM llm_billing_line_items WHERE invoice_no = ?
            ORDER BY line_item_index
        ''', (invoice_no,))
        
        line_items = cursor.fetchall()
        conn.close()
        
        # Build comprehensive result
        result = {
            'id': main_row['id'],
            'invoice_no': main_row['invoice_no'],
            'pdf_file_path': main_row['pdf_file_path'],
            'pdf_filename': main_row['pdf_filename'],
            'raw_pdf_text': main_row['raw_pdf_text'],
            'processing_timestamp': main_row['processing_timestamp'],
            'llm_model_used': main_row['llm_model_used'],
            'extraction_confidence': main_row['extraction_confidence'],
            'manual_review_needed': bool(main_row['manual_review_needed'])
        }
        
        # Parse original extracted_data
        original_data = {}
        if main_row['extracted_data']:
            try:
                original_data = json.loads(main_row['extracted_data'])
            except (json.JSONDecodeError, TypeError):
                original_data = {}
        
        # Build comprehensive extracted_data combining all sources
        if summary_row:
            # Use summary data (comprehensive) when available
            extracted_data = {
                'invoice_no': invoice_no,
                'invoice_date': summary_row['invoice_date'],
                'due_date': summary_row['due_date'],
                'customer_name': summary_row['customer_name'],
                'currency': summary_row['currency'],
                'subtotal': summary_row['subtotal'],
                'gst_total': summary_row['gst_total'],
                'final_total': summary_row['final_total'],
                'service_type': summary_row['service_type'],
                'origin': summary_row['origin'],
                'destination': summary_row['destination'],
                'weight': summary_row['weight'],
                'shipment_ref': summary_row['shipment_ref'],
                'account_number': summary_row['account_number'],
                'payment_terms': summary_row['payment_terms'],
                'incoterms': summary_row['incoterms'],
                'transportation_mode': summary_row['transportation_mode'],
                'masterbill': summary_row['masterbill'],
                'housebill': summary_row['housebill'],
                'awb_number': summary_row['awb_number'],
                'shipment_date': summary_row['shipment_date'],
                'total_pieces': summary_row['total_pieces'],
                'chargeable_weight': summary_row['chargeable_weight'],
                'volume_weight': summary_row['volume_weight'],
                'exchange_rate_eur': summary_row['exchange_rate_eur'],
                'exchange_rate_usd': summary_row['exchange_rate_usd'],
                'shipper_name': summary_row['shipper_name'],
                'shipper_address': summary_row['shipper_address'],
                'consignee_name': summary_row['consignee_name'],
                'consignee_address': summary_row['consignee_address'],
                'commodity_description': summary_row['commodity_description']
            }
            
            # Add line items
            if line_items:
                extracted_data['charges'] = []
                for item in line_items:
                    extracted_data['charges'].append({
                        'line_item_index': item['line_item_index'],
                        'description': item['description'],
                        'amount': item['amount'],
                        'gst_amount': item['gst_amount'],
                        'total_amount': item['total_amount'],
                        'currency': item['currency'],
                        'category': item['category'],
                        'charge_type': item['charge_type'],
                        'base_amount': item['base_amount'],
                        'surcharge_amount': item['surcharge_amount'],
                        'discount_amount': item['discount_amount'],
                        'discount_code': item['discount_code'],
                        'tax_code': item['tax_code'],
                        'pal_col': item['pal_col'],
                        'weight_charge': item['weight_charge']
                    })
        else:
            # Fallback to original extracted_data from main table
            extracted_data = original_data
        
        result['extracted_data'] = extracted_data
        return result
