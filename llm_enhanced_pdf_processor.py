#!/usr/bin/env python3
"""
LLM-Enhanced PDF Invoice Processor using Ollama with DeepSeek-R1
"""

import os
import json
import sqlite3
import requests
from typing import Dict, List, Optional
import pdfplumber
import fitz  # PyMuPDF
from datetime import datetime
from model_manager import ModelManager


class LLMEnhancedPDFProcessor:
    """
    PDF processor enhanced with local LLM capabilities via Ollama (DeepSeek-R1)
    """
    
    def __init__(self, db_path: str = 'dhl_audit.db', model_name: str = None):
        self.db_path = db_path
        # Model Configuration - defaults to faster Llama3.2
        self.ollama_url = "http://localhost:11434"
        self.model_name = model_name or "llama3.2:latest"
        self.timeout = 300  # Increased to 5 minutes for complex PDFs
        self.temperature = 0.1
        self.top_p = 0.9
        self.num_predict = 8192  # Increased to ensure complete JSON responses
        self.init_database()
        
        # Initialize model manager for automatic model loading
        self.model_manager = ModelManager()
        
        # Available models with their characteristics
        self.available_models = {
            'llama3.2:latest': {
                'name': 'Llama 3.2', 
                'size': '2.0 GB', 
                'speed': 'Fast', 
                'accuracy': 'Good',
                'best_for': 'General invoice processing'
            },
            'deepseek-r1:latest': {
                'name': 'DeepSeek-R1', 
                'size': '4.7 GB', 
                'speed': 'Slow', 
                'accuracy': 'Excellent',
                'best_for': 'Complex invoices requiring reasoning'
            },
            'deepseek-r1:1.5b': {
                'name': 'DeepSeek-R1 1.5B', 
                'size': '1.1 GB', 
                'speed': 'Very Fast', 
                'accuracy': 'Good',
                'best_for': 'Quick processing of simple invoices'
            },
            'mistral:latest': {
                'name': 'Mistral', 
                'size': '4.1 GB', 
                'speed': 'Medium', 
                'accuracy': 'Very Good',
                'best_for': 'Balanced speed and accuracy'
            },
            'qwen2.5-coder:1.5b': {
                'name': 'Qwen2.5 Coder 1.5B', 
                'size': '986 MB', 
                'speed': 'Very Fast', 
                'accuracy': 'Good',
                'best_for': 'Structured data extraction'
            },
            'gemma3:1b': {
                'name': 'Gemma 3 1B', 
                'size': '815 MB', 
                'speed': 'Very Fast', 
                'accuracy': 'Fair',
                'best_for': 'Simple invoice processing'
            }
        }

    def init_database(self):
        """Initialize database tables for LLM-enhanced processing"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Enhanced LLM processing table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS llm_pdf_extractions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_no VARCHAR(50) NOT NULL,
                pdf_file_path VARCHAR(500),
                pdf_filename VARCHAR(255),
                extracted_data TEXT,  -- JSON with LLM extracted data
                raw_pdf_text TEXT(2000000),  -- Original PDF text with increased size limit (2MB)
                processing_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                llm_model_used VARCHAR(100),
                extraction_confidence FLOAT,
                manual_review_needed BOOLEAN DEFAULT 0,
                UNIQUE(invoice_no)
            )
        ''')
        
        # Detailed billing line items table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS llm_billing_line_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_no VARCHAR(50) NOT NULL,
                line_item_index INTEGER,
                description TEXT,
                amount DECIMAL(15,2),
                gst_amount DECIMAL(15,2),
                total_amount DECIMAL(15,2),
                currency VARCHAR(10),
                category VARCHAR(50),
                extraction_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (invoice_no) REFERENCES llm_pdf_extractions(invoice_no)
            )
        ''')
        
        # Invoice summary table for LLM extractions
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS llm_invoice_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                invoice_no VARCHAR(50) NOT NULL,
                invoice_date DATE,
                due_date DATE,
                customer_name VARCHAR(255),
                currency VARCHAR(10),
                subtotal DECIMAL(15,2),
                gst_total DECIMAL(15,2),
                final_total DECIMAL(15,2),
                service_type VARCHAR(100),
                origin VARCHAR(100),
                destination VARCHAR(100),
                weight VARCHAR(50),
                shipment_ref VARCHAR(100),
                extraction_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(invoice_no),
                FOREIGN KEY (invoice_no) REFERENCES llm_pdf_extractions(invoice_no)
            )
        ''')
        
        conn.commit()
        conn.close()

    def get_available_models(self):
        """Get list of available models with their characteristics"""
        return self.available_models
    
    def set_model(self, model_name: str):
        """Set the model to use for processing"""
        if model_name in self.available_models:
            self.model_name = model_name
            return True
        return False

    def check_ollama_connection(self) -> bool:
        """Check if Ollama is running and DeepSeek-R1 model is available"""
        try:
            # Check if Ollama is running
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=5)
            if response.status_code != 200:
                return False
                
            # Check if DeepSeek-R1 model is available
            models = response.json().get('models', [])
            model_names = [model['name'] for model in models]
            
            if self.model_name not in model_names:
                print(f"Model {self.model_name} not found.")
                print(f"Available models: {model_names}")
                return False
                
            return True
            
        except Exception as e:
            print(f"Ollama connection error: {e}")
            return False
            
    def query_ollama(self, prompt: str) -> str:
        """
        Query the Ollama API directly with a custom prompt
        This method is used for direct testing of the LLM
        """
        try:
            # Check connection first
            if not self.check_ollama_connection():
                print("ERROR: Ollama connection check failed")
                raise Exception("Ollama connection failed - service not available")
            
            # Prepare the API request
            url = f"{self.ollama_url}/api/generate"
            payload = {
                "model": self.model_name,
                "prompt": prompt,
                "stream": False
            }
            
            print(f"Querying Ollama API at {url} with {len(prompt)} characters...")
            
            # Send the request with improved error handling
            try:
                response = requests.post(url, json=payload, timeout=120)
                response.raise_for_status()
            except requests.exceptions.ConnectionError as ce:
                print(f"Connection error to Ollama: {ce}")
                raise Exception(f"Cannot connect to Ollama service at {self.ollama_url}. Please ensure it's running.")
            except requests.exceptions.Timeout as te:
                print(f"Timeout error with Ollama: {te}")
                raise Exception("Ollama request timed out. The model may be too busy or the prompt too complex.")
            except requests.exceptions.HTTPError as he:
                print(f"HTTP error from Ollama: {he}")
                raise Exception(f"HTTP error {response.status_code} from Ollama: {response.text}")
            
            # Parse the response
            try:
                response_json = response.json()
                response_text = response_json.get('response', '')
                if not response_text:
                    print("Warning: Empty response text received from Ollama")
            except ValueError as ve:
                print(f"JSON parsing error: {ve}, Response content: {response.text[:200]}")
                raise Exception("Failed to parse Ollama response as JSON")
            
            # Process the response
            print(f"Response received: {len(response_text)} characters")
            return response_text
            
        except Exception as e:
            print(f"Error querying Ollama: {e}")
            raise e

    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """Extract text from PDF using PyMuPDF (fitz) with enhanced capacity for large documents"""
        import time
        time_start = time.time()
        text_content = ""
        
        try:
            # Open the PDF with PyMuPDF
            time_before_open = time.time()
            with fitz.open(pdf_path) as doc:
                time_after_open = time.time()
                
                # Get total pages for logging
                total_pages = len(doc)
                print(f"Extracting text from PDF with {total_pages} pages using PyMuPDF...")
                print(f"[TIMING] PDF open time: {(time_after_open - time_before_open):.3f}s")
                
                page_times = []
                for page_num, page in enumerate(doc, 1):
                    time_before_page = time.time()
                    # Extract text with PyMuPDF
                    page_text = page.get_text()
                    time_after_page = time.time()
                    page_times.append(time_after_page - time_before_page)
                    
                    if page_text:
                        # Add page number for better traceability
                        text_content += f"--- Page {page_num}/{total_pages} ---\n{page_text}\n\n"
                
                # Report page extraction timing statistics
                if page_times:
                    avg_time = sum(page_times) / len(page_times)
                    max_time = max(page_times)
                    max_page = page_times.index(max_time) + 1
                    print(f"[TIMING] Average page extraction time: {avg_time:.3f}s")
                    print(f"[TIMING] Slowest page was #{max_page}: {max_time:.3f}s")
                
                # Log the extraction size
                print(f"Extracted {len(text_content)} characters from PDF")
        except Exception as e:
            print(f"Error extracting text from PDF: {e}")
            return ""
        
        return text_content

    def query_llm(self, prompt: str, pdf_text: str) -> Optional[str]:
        """Query the local LLM via Ollama with streaming support"""
        import time
        
        try:
            time_start = time.time()
            full_prompt = f"{prompt}\n\nINVOICE TEXT:\n{pdf_text}"
            print(f"Querying LLM with {len(full_prompt)} characters of prompt...")
            
            payload = {
                "model": self.model_name,
                "prompt": full_prompt,
                "stream": True,  # Enable streaming for complete responses
                "options": {
                    "temperature": self.temperature,
                    "top_p": self.top_p,
                    "num_predict": self.num_predict,
                    # GPU optimization options
                    "num_ctx": 8192,
                    "num_batch": 512,
                    "num_gpu": 1,
                    "gpu_layers": -1  # Use all GPU layers
                }
            }
            
            print(f"Sending streaming request to {self.ollama_url}/api/generate...")
            
            time_before_llm_request = time.time()
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json=payload,
                timeout=self.timeout,
                stream=True  # Enable streaming
            )
            
            print(f"LLM response status: {response.status_code}")
            
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
                
                time_after_llm_request = time.time()
                llm_request_time = time_after_llm_request - time_before_llm_request
                print(f"[TIMING] LLM request time: {llm_request_time:.3f}s")
                
                # Calculate tokens per second
                if total_tokens > 0:
                    tokens_per_second = total_tokens / llm_request_time if llm_request_time > 0 else 0
                    print(f"[TIMING] Processed {total_tokens} tokens at {tokens_per_second:.1f} tokens/sec")
                
                # Calculate total time for LLM processing
                time_complete = time.time()
                total_time = time_complete - time_start
                print(f"[TIMING] Total LLM time: {total_time:.3f}s")
                print(f"LLM response received: {len(full_response)} characters")
                return full_response
            else:
                print(f"LLM query failed: {response.status_code}")
                print(f"Response: {response.text}")
                return None
                
        except Exception as e:
            print(f"Error querying LLM: {e}")
            import traceback
            traceback.print_exc()
            return None

    def extract_invoice_data_with_llm(self, pdf_text: str) -> Dict:
        """Extract structured invoice data using DeepSeek-R1 LLM"""
        
        prompt = """You are an expert at extracting billing details from DHL invoices.

Analyze this DHL invoice text and extract essential billing information in JSON format.

Return ONLY a valid JSON object with these EXACT fields:
{
    "invoice_no": "string",
    "invoice_date": "DD-MMM-YY format",
    "customer_name": "string",
    "currency": "string (AUD, USD, etc.)",
    "final_total": number
}

Keep response short and focused. Extract only these 5 fields."""
        
        llm_response = self.query_llm(prompt, pdf_text)
        
        if llm_response:
            try:
                # DeepSeek-R1 often wraps responses in <think> tags, extract actual content
                response_text = llm_response.strip()
                
                # Remove <think> tags if present
                if '<think>' in response_text and '</think>' in response_text:
                    # Extract content after </think>
                    think_end = response_text.find('</think>')
                    if think_end != -1:
                        response_text = response_text[think_end + 8:].strip()
                
                # Remove markdown code blocks if present
                if '```json' in response_text:
                    start_marker = '```json'
                    start_idx = response_text.find(start_marker) + len(start_marker)
                    end_idx = response_text.find('```', start_idx)
                    if end_idx != -1:
                        response_text = response_text[start_idx:end_idx].strip()
                    else:
                        # Handle case where closing ``` is missing (truncated response)
                        print("⚠️  Warning: Closing ``` marker not found, response may be truncated")
                        response_text = response_text[start_idx:].strip()
                elif '```' in response_text:
                    # Handle generic code blocks
                    lines = response_text.split('\n')
                    in_code_block = False
                    json_lines = []
                    for line in lines:
                        if line.strip().startswith('```'):
                            in_code_block = not in_code_block
                            continue
                        if in_code_block:
                            json_lines.append(line)
                    if json_lines:
                        response_text = '\n'.join(json_lines).strip()
                
                # Find JSON object in response - use a more robust approach
                json_start = response_text.find('{')
                if json_start != -1:
                    # Count braces to find the complete JSON object
                    brace_count = 0
                    json_end = -1
                    for i, char in enumerate(response_text[json_start:], json_start):
                        if char == '{':
                            brace_count += 1
                        elif char == '}':
                            brace_count -= 1
                            if brace_count == 0:
                                json_end = i
                                break
                    
                    if json_end != -1:
                        json_str = response_text[json_start:json_end + 1]
                    else:
                        print("⚠️  Warning: Incomplete JSON object detected, attempting to parse anyway")
                        json_str = response_text[json_start:]
                        # Try to add closing brace if needed
                        if not json_str.rstrip().endswith('}'):
                            json_str = json_str.rstrip() + '}'
                    
                    # Clean up common JSON issues from LLM responses
                    json_str = json_str.replace('\n', ' ')  # Remove newlines
                    json_str = json_str.replace('  ', ' ')  # Remove double spaces
                    
                    # Remove comments from JSON (// comments)
                    import re
                    json_str = re.sub(r'//[^\n]*', '', json_str)
                    
                    # Remove any special unicode characters that might break JSON
                    json_str = re.sub(r'[^\x00-\x7F]+', ' ', json_str)
                    
                    # Fix trailing commas before closing braces/brackets
                    json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
                    
                    # Fix undefined values and nulls
                    json_str = json_str.replace('null', '""')
                    json_str = json_str.replace('undefined', '""')
                    
                    # Fix quote issues - ensure proper JSON string quoting
                    json_str = re.sub(r':\s*([^",\[\]{}]+?)\s*([,}])', r': "\1"\2', json_str)
                    
                    print(f"Cleaned JSON string: {json_str[:500]}...")  # Debug
                    
                    extracted_data = json.loads(json_str)
                    
                    # Ensure numeric fields are properly typed
                    if 'final_total' in extracted_data:
                        try:
                            extracted_data['final_total'] = float(extracted_data['final_total'])
                        except (ValueError, TypeError):
                            pass  # Keep as string if conversion fails
                    
                    return extracted_data
                else:
                    print("No valid JSON found in LLM response")
                    print(f"Response text: {response_text[:1000]}...")  # Debug
                    return {}
                    
            except json.JSONDecodeError as e:
                print(f"JSON parsing error: {e}")
                print(f"Attempted to parse: {json_str[:500] if 'json_str' in locals() else 'N/A'}")
                print(f"LLM Response: {llm_response[:1000]}...")
                return {}
        else:
            return {}

    def process_pdf_with_llm(self, pdf_path: str, invoice_no: str = None) -> Dict:
        """Process a PDF file with LLM enhanced extraction"""
        import time
        time_start = time.time()
        timing = {}
        
        try:
            print(f"[TIMING] Starting PDF processing for {pdf_path}")
            print(f"🤖 Using model: {self.model_name}")
            
            # Ensure model is loaded and ready before processing
            print(f"🔄 Ensuring model {self.model_name} is ready...")
            time_before_model_prep = time.time()
            model_ready, model_message = self.model_manager.ensure_model_ready(self.model_name)
            time_after_model_prep = time.time()
            timing['model_preparation'] = round(time_after_model_prep - time_before_model_prep, 3)
            print(f"[TIMING] Model preparation: {timing['model_preparation']:.3f}s")
            
            if not model_ready:
                return {
                    'success': False, 
                    'error': f'Model preparation failed: {model_message}',
                    'timing': timing
                }
            
            print(f"✅ Model ready: {model_message}")
            
            # Extract text from PDF
            time_before_extraction = time.time()
            pdf_text = self.extract_text_from_pdf(pdf_path)
            time_after_extraction = time.time()
            extraction_time = time_after_extraction - time_before_extraction
            timing['pdf_extraction'] = round(extraction_time, 3)
            print(f"[TIMING] PDF text extraction: {extraction_time:.3f}s")
            
            if not pdf_text:
                return {'success': False, 'error': 'Could not extract text from PDF'}
            
            # Auto-detect invoice number if not provided
            time_before_detection = time.time()
            if not invoice_no:
                import re
                filename = os.path.basename(pdf_path)
                invoice_match = re.search(r'([A-Z]\d{7,})', filename)
                invoice_no = invoice_match.group(1) if invoice_match else filename.replace('.pdf', '')
            time_after_detection = time.time()
            timing['invoice_detection'] = round(time_after_detection - time_before_detection, 3)
            
            # Extract structured data using LLM
            time_before_llm = time.time()
            extracted_data = self.extract_invoice_data_with_llm(pdf_text)
            time_after_llm = time.time()
            llm_time = time_after_llm - time_before_llm
            timing['llm_processing'] = round(llm_time, 3)
            print(f"[TIMING] LLM extraction: {llm_time:.3f}s")
            
            # Calculate confidence
            time_before_confidence = time.time()
            confidence = self.calculate_extraction_confidence(extracted_data)
            time_after_confidence = time.time()
            timing['confidence_calculation'] = round(time_after_confidence - time_before_confidence, 3)
            
            manual_review_needed = confidence < 0.7
            
            # Save to database
            time_before_save = time.time()
            self.save_llm_extraction(
                invoice_no, 
                pdf_path, 
                os.path.basename(pdf_path),
                extracted_data, 
                pdf_text, 
                confidence, 
                manual_review_needed,
                timing  # Pass timing data to be saved
            )
            time_after_save = time.time()
            save_time = time_after_save - time_before_save
            timing['database_save'] = round(save_time, 3)
            print(f"[TIMING] Database save: {save_time:.3f}s")
            
            # Calculate total time
            time_end = time.time()
            total_time = time_end - time_start
            timing['total_processing'] = round(total_time, 3)
            print(f"[TIMING] Total PDF processing time: {total_time:.3f}s")
            
            # Add timing to the result
            result = {
                'success': True,
                'invoice_no': invoice_no,
                'extracted_data': extracted_data,
                'confidence': confidence,
                'manual_review_needed': manual_review_needed,
                'timing': timing
            }
            
            return result
            manual_review_needed = confidence < 0.7
            
            # Save to database
            self.save_llm_extraction(
                invoice_no=invoice_no,
                pdf_path=pdf_path,
                pdf_filename=os.path.basename(pdf_path),
                extracted_data=extracted_data,
                raw_pdf_text=pdf_text,
                confidence=confidence,
                manual_review_needed=manual_review_needed
            )
            
            return {
                'success': True,
                'invoice_no': invoice_no,
                'extracted_data': extracted_data,
                'confidence': confidence,
                'manual_review_needed': manual_review_needed,
                'llm_model': self.model_name
            }
            
        except Exception as e:
            return {'success': False, 'error': f'Processing error: {str(e)}'}

    def calculate_extraction_confidence(self, data: Dict) -> float:
        """Calculate confidence score based on completeness of extracted data"""
        score = 0.0
        max_score = 10.0
        
        # Check key fields
        if data.get('invoice_no'): 
            score += 1.0
        if data.get('invoice_date'): 
            score += 1.0
        if data.get('customer_name'): 
            score += 1.0
        if data.get('currency'): 
            score += 1.0
        if data.get('final_total') and data.get('final_total') > 0: 
            score += 2.0
        if data.get('charges') and len(data.get('charges', [])) > 0: 
            score += 2.0
        if data.get('service_type'): 
            score += 1.0
        if data.get('shipment_details'): 
            score += 1.0
        
        return min(score / max_score, 1.0)

    def save_llm_extraction(self, invoice_no: str, pdf_path: str, pdf_filename: str,
                           extracted_data: Dict, raw_pdf_text: str, confidence: float,
                           manual_review_needed: bool, timing_data: Dict = None):
        """Save LLM extraction results to database"""
        import time
        db_timing = {}
        
        try:
            time_db_connect = time.time()
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            time_after_connect = time.time()
            db_timing['connect'] = round(time_after_connect - time_db_connect, 3)
            
            print(f"Saving extraction for invoice: {invoice_no}")
            print(f"Extracted data keys: {list(extracted_data.keys())}")
            print(f"[TIMING] Database connect: {db_timing['connect']:.3f}s")
            
            # Include timing data in the extracted_data if provided
            if timing_data:
                print(f"Adding timing data to extracted_data: {timing_data}")
                extracted_data['timing'] = timing_data
            
            # Save main extraction record
            time_before_main_save = time.time()
            cursor.execute('''
                INSERT OR REPLACE INTO llm_pdf_extractions
                (invoice_no, pdf_file_path, pdf_filename, extracted_data, raw_pdf_text,
                 llm_model_used, extraction_confidence, manual_review_needed)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                invoice_no,
                pdf_path,
                pdf_filename,
                json.dumps(extracted_data, indent=2),
                raw_pdf_text,
                self.model_name,
                confidence,
                manual_review_needed
            ))
            time_after_main_save = time.time()
            db_timing['main_record'] = round(time_after_main_save - time_before_main_save, 3)
            
            print("Main extraction record saved")
            print(f"[TIMING] Main record save: {db_timing['main_record']:.3f}s")
            
            # Save detailed billing line items
            time_before_line_items = time.time()
            cursor.execute('DELETE FROM llm_billing_line_items WHERE invoice_no = ?',
                          (invoice_no,))
            
            charges = extracted_data.get('charges', [])
            print(f"Processing {len(charges)} charges")
            
            for idx, charge in enumerate(charges):
                print(f"Processing charge {idx}: {charge}")
                # Ensure all values are properly converted to basic types
                description = str(charge.get('description', '')) if charge.get('description') else ''
                amount = float(charge.get('amount', 0)) if charge.get('amount') not in (None, '', 'null') else 0.0
                gst_amount = float(charge.get('gst_amount', 0)) if charge.get('gst_amount') not in (None, '', 'null') else 0.0
                currency = str(extracted_data.get('currency', 'AUD'))
                category = str(charge.get('category', 'OTHER'))
                
                cursor.execute('''
                    INSERT INTO llm_billing_line_items
                    (invoice_no, line_item_index, description, amount, gst_amount,
                     total_amount, currency, category, charge_type, base_amount,
                     surcharge_amount, discount_amount, discount_code, tax_code,
                     pal_col, weight_charge)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    invoice_no,
                    idx,
                    description,
                    amount,
                    gst_amount,
                    amount + gst_amount,
                    currency,
                    category,
                    # Enhanced fields - might be None for older data
                    charge.get('charge_type'),
                    float(charge.get('base_amount', 0)) if charge.get('base_amount') not in (None, '') else None,
                    float(charge.get('surcharge_amount', 0)) if charge.get('surcharge_amount') not in (None, '') else None,
                    float(charge.get('discount_amount', 0)) if charge.get('discount_amount') not in (None, '') else None,
                    charge.get('discount_code'),
                    charge.get('tax_code'),
                    int(charge.get('pal_col', 0)) if charge.get('pal_col') not in (None, '') else None,
                    float(charge.get('weight_charge', 0)) if charge.get('weight_charge') not in (None, '') else None
                ))
            
            time_after_line_items = time.time()
            db_timing['line_items'] = round(time_after_line_items - time_before_line_items, 3)
            print("Line items saved")
            print(f"[TIMING] Line items save: {db_timing['line_items']:.3f}s")
            
            # Save invoice summary
            time_before_summary = time.time()
            cursor.execute('DELETE FROM llm_invoice_summary WHERE invoice_no = ?',
                          (invoice_no,))
            
            shipment_details = extracted_data.get('shipment_details', {})
            
            # Ensure all values are properly converted to basic types
            def safe_str(value):
                return str(value) if value not in (None, '') else None
                
            def safe_float(value):
                try:
                    return float(value) if value not in (None, '') else 0.0
                except (ValueError, TypeError):
                    return 0.0
            
            cursor.execute('''
                INSERT INTO llm_invoice_summary
                (invoice_no, invoice_date, due_date, customer_name, currency,
                 subtotal, gst_total, final_total, service_type, origin,
                 destination, weight, shipment_ref, account_number, payment_terms,
                 incoterms, transportation_mode, masterbill, housebill, awb_number,
                 shipment_date, total_pieces, chargeable_weight, volume_weight,
                 exchange_rate_eur, exchange_rate_usd, shipper_name, shipper_address,
                 consignee_name, consignee_address, commodity_description)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                invoice_no,
                safe_str(extracted_data.get('invoice_date')),
                safe_str(extracted_data.get('due_date')),
                safe_str(extracted_data.get('customer_name')),
                safe_str(extracted_data.get('currency', 'AUD')),
                safe_float(extracted_data.get('subtotal', 0)),
                safe_float(extracted_data.get('gst_total', 0)),
                safe_float(extracted_data.get('final_total', 0)),
                safe_str(extracted_data.get('service_type')),
                safe_str(shipment_details.get('origin')),
                safe_str(shipment_details.get('destination')),
                safe_str(shipment_details.get('weight')),
                safe_str(shipment_details.get('shipment_ref')),
                # Enhanced fields - these might be None but that's ok
                safe_str(extracted_data.get('account_number')),
                safe_str(extracted_data.get('payment_terms')),
                safe_str(extracted_data.get('incoterms')),
                safe_str(extracted_data.get('transportation_mode')),
                safe_str(extracted_data.get('masterbill')),
                safe_str(extracted_data.get('housebill')),
                safe_str(extracted_data.get('awb_number')),
                safe_str(extracted_data.get('shipment_date')),
                safe_float(extracted_data.get('total_pieces')),
                safe_float(extracted_data.get('chargeable_weight')),
                safe_float(extracted_data.get('volume_weight')),
                safe_float(extracted_data.get('exchange_rate_eur')),
                safe_float(extracted_data.get('exchange_rate_usd')),
                safe_str(extracted_data.get('shipper_name')),
                safe_str(extracted_data.get('shipper_address')),
                safe_str(extracted_data.get('consignee_name')),
                safe_str(extracted_data.get('consignee_address')),
                safe_str(extracted_data.get('commodity_description'))
            ))
            
            time_after_summary = time.time()
            db_timing['summary'] = round(time_after_summary - time_before_summary, 3)
            print(f"[TIMING] Summary save: {db_timing['summary']:.3f}s")
            
            # Commit transaction
            time_before_commit = time.time()
            conn.commit()
            time_after_commit = time.time()
            db_timing['commit'] = round(time_after_commit - time_before_commit, 3)
            print(f"[TIMING] Commit: {db_timing['commit']:.3f}s")
            
            conn.close()
            
            # Calculate total database time
            total_db_time = sum(db_timing.values())
            print(f"[TIMING] Total database operations: {total_db_time:.3f}s")
            print("Invoice summary saved")
            print("Database save completed successfully")
            
        except Exception as e:
            print(f"Database error in save_llm_extraction: {e}")
            import traceback
            traceback.print_exc()
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            raise e

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


# Example usage
if __name__ == "__main__":
    processor = LLMEnhancedPDFProcessor()
    
    # Test with a PDF file
    pdf_path = "uploads/sample_invoice.pdf"
    if os.path.exists(pdf_path):
        result = processor.process_pdf_with_llm(pdf_path)
        print(json.dumps(result, indent=2))
    else:
        print(f"PDF file not found: {pdf_path}")
        print(f"Current model: {processor.model_name}")
