#!/usr/bin/env python3
"""
LLM Training Data Generator for DHL Invoice Processing
Creates training datasets from your high-quality examples
"""

import json
import os
from typing import Dict, List, Tuple
from schema_driven_llm_processor import SchemaDrivenLLMProcessor
import PyPDF2


class LLMTrainingDataGenerator:
    """Generate training data for LLM fine-tuning"""
    
    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path
        self.processor = SchemaDrivenLLMProcessor()
        
        # Your high-quality examples
        self.golden_examples = [
            {
                "pdf_file": "uploads/ANDA_DZNA_D2159508.pdf",
                "expected_output": {
                    "invoice_summary": {
                        "invoice_no": "D2159508",
                        "invoice_date": "2025-07-14",
                        "due_date": "2025-10-12",
                        "customer_name": "OUTDOOR WIRELESS NETWORKS AUSTRALIA PTY LTD",
                        "currency": "AUD",
                        "subtotal": 3328.3,
                        "gst_total": 0.0,
                        "final_total": 3328.3,
                        "service_type": None,
                        "origin": "AUSYD = Sydney, Australia",
                        "destination": "NZAKL Auckland, New Zealand",
                        "weight": "693.000 KG",
                        "shipment_ref": "S2503229764",
                        "account_number": "AUOU0003/AUOU0003",
                        "payment_terms": "90 days from Inv. Date",
                        "incoterms": None,
                        "transportation_mode": None,
                        "masterbill": "255655431",
                        "housebill": "DZ1395970",
                        "awb_number": None,
                        "shipment_date": "2025-07-15",
                        "total_pieces": 5,
                        "chargeable_weight": 7.791,
                        "volume_weight": 7.791,
                        "exchange_rate_eur": None,
                        "exchange_rate_usd": 0.624795,
                        "shipper_name": "OUTDOOR WIRELESS NETWORKS AUSTRALIA PTY LTD",
                        "shipper_address": None,
                        "consignee_name": "AVW LIMITED",
                        "consignee_address": None,
                        "commodity_description": "telecommunication cable"
                    },
                    "billing_line_items": [
                        {
                            "line_item_index": 1,
                            "description": "PICK UP OF THIS SHIPMENT Greater of (Min Rate USD 125.22, 7.791 Cubic Metre(s) @ USD 44.70/M3) USD 348.26 @ 0.624795",
                            "amount": 557.40,
                            "gst_amount": None,
                            "total_amount": None,
                            "currency": "AUD",
                            "category": "FREIGHT"
                        },
                        {
                            "line_item_index": 2,
                            "description": "ORIGIN CHARGES ALL IN Greater of (Min Rate USD 199.66, 7.791 Cubic Metre(s) @ USD $55.78/M3)$ USD 434.58 @ 0.624795",
                            "amount": 695.56,
                            "gst_amount": None,
                            "total_amount": None,
                            "currency": "AUD",
                            "category": "SERVICE_CHARGE"
                        },
                        {
                            "line_item_index": 3,
                            "description": "Freight Greater of (Min Rate USD 55.00, 7.791 Cubic Metre(s) @ USD 55.00/M3) USD 428.51 @ 0.624795",
                            "amount": 685.84,
                            "gst_amount": None,
                            "total_amount": None,
                            "currency": "AUD",
                            "category": "FREIGHT"
                        },
                        {
                            "line_item_index": 4,
                            "description": "DELIVERY ALL IN Greater of (Min Rate USD 31.10, 7.791 Cubic Metre(s) @ USD 31.10/M3) USD 242.30 @ 0.624795",
                            "amount": 387.81,
                            "gst_amount": None,
                            "total_amount": None,
                            "currency": "AUD",
                            "category": "SERVICE_CHARGE"
                        },
                        {
                            "line_item_index": 5,
                            "description": "DESTINATION CHARGES ALL IN Greater of (Min Rate USD 192.78, 7.791 Cubic Metre(s) @ USD 80.33/M3) USD 625.85 @ 0.624795",
                            "amount": 1001.69,
                            "gst_amount": None,
                            "total_amount": None,
                            "currency": "AUD",
                            "category": "SERVICE_CHARGE"
                        }
                    ]
                }
            },
            {
                "pdf_file": "uploads/ANDA_DZNA_D2133359.pdf",
                "expected_output": {
                    "invoice_summary": {
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
                        "incoterms": None,
                        "transportation_mode": None,
                        "masterbill": None,
                        "housebill": "RTMB13675",
                        "awb_number": None,
                        "shipment_date": "2025-04-11",
                        "total_pieces": 1,
                        "chargeable_weight": 0.335,
                        "volume_weight": 0.335,
                        "exchange_rate_eur": 0.55081,
                        "exchange_rate_usd": None,
                        "shipper_name": "COMMSCOPE TECHNOLOGIES UK",
                        "shipper_address": None,
                        "consignee_name": "OUTDOOR WIRELESS NETWORKS AUSTRALIA PTY LTD",
                        "consignee_address": None,
                        "commodity_description": "TELECOMMUNICATION EQUIPMENT"
                    },
                    "billing_line_items": [
                        {
                            "line_item_index": 1,
                            "description": "Org. Fumigation Fee EUR 180.00 @ 0.550810",
                            "amount": 326.79,
                            "gst_amount": None,
                            "total_amount": None,
                            "currency": "AUD",
                            "category": "SERVICE_CHARGE"
                        }
                    ]
                }
            }
        ]
    
    def extract_pdf_text(self, pdf_path: str) -> str:
        """Extract text from PDF file"""
        try:
            with open(pdf_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n"
                return text.strip()
        except Exception as e:
            print(f"Error extracting text from {pdf_path}: {e}")
            return ""
    
    def generate_jsonl_training_data(self, output_file: str = "dhl_training_data.jsonl"):
        """Generate JSONL format training data for OpenAI-style fine-tuning"""
        training_data = []
        
        for example in self.golden_examples:
            pdf_path = example["pdf_file"]
            expected_output = example["expected_output"]
            
            if not os.path.exists(pdf_path):
                print(f"Warning: PDF file not found: {pdf_path}")
                continue
            
            # Extract PDF text
            pdf_text = self.extract_pdf_text(pdf_path)
            if not pdf_text:
                continue
            
            # Create training example
            training_example = {
                "messages": [
                    {
                        "role": "system",
                        "content": "You are an expert DHL invoice data extraction specialist. Extract invoice data in the exact JSON format specified."
                    },
                    {
                        "role": "user", 
                        "content": f"Extract data from this DHL invoice and return it in the required JSON format:\n\n{pdf_text}"
                    },
                    {
                        "role": "assistant",
                        "content": json.dumps(expected_output, indent=2)
                    }
                ]
            }
            
            training_data.append(training_example)
        
        # Write JSONL file
        with open(output_file, 'w', encoding='utf-8') as f:
            for example in training_data:
                f.write(json.dumps(example) + '\n')
        
        print(f"Generated {len(training_data)} training examples in {output_file}")
        return output_file
    
    def generate_claude_training_data(self, output_file: str = "dhl_claude_training.json"):
        """Generate training data format for Claude/Anthropic"""
        training_data = {
            "task_description": "Extract structured data from DHL invoices",
            "examples": []
        }
        
        for example in self.golden_examples:
            pdf_path = example["pdf_file"]
            expected_output = example["expected_output"]
            
            if not os.path.exists(pdf_path):
                continue
            
            pdf_text = self.extract_pdf_text(pdf_path)
            if not pdf_text:
                continue
            
            training_example = {
                "input": pdf_text,
                "output": expected_output,
                "classification_rules": {
                    "FREIGHT": ["PICK UP", "Freight", "freight charges"],
                    "SERVICE_CHARGE": ["ORIGIN CHARGES", "DELIVERY", "DESTINATION CHARGES", "Fumigation Fee"],
                    "SURCHARGE": ["fuel", "security", "peak season"],
                    "DUTY_TAX": ["duty", "tax", "customs"],
                    "OTHER": ["miscellaneous", "other charges"]
                }
            }
            
            training_data["examples"].append(training_example)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(training_data, f, indent=2)
        
        print(f"Generated Claude training data in {output_file}")
        return output_file
    
    def generate_validation_set(self):
        """Test current model against golden examples to measure improvement"""
        results = []
        
        for example in self.golden_examples:
            pdf_path = example["pdf_file"]
            expected_output = example["expected_output"]
            
            if not os.path.exists(pdf_path):
                continue
            
            print(f"Testing against golden example: {pdf_path}")
            
            # Run current model
            result = self.processor.extract_invoice_data_with_schema(
                self.extract_pdf_text(pdf_path)
            )
            
            if result.get('success'):
                actual_output = result.get('data')
                
                # Compare key fields
                comparison = self.compare_extractions(expected_output, actual_output)
                results.append({
                    "file": pdf_path,
                    "expected": expected_output,
                    "actual": actual_output,
                    "comparison": comparison
                })
            else:
                results.append({
                    "file": pdf_path,
                    "error": result.get('error')
                })
        
        return results
    
    def compare_extractions(self, expected: Dict, actual: Dict) -> Dict:
        """Compare expected vs actual extraction results"""
        comparison = {
            "invoice_summary_match": 0,
            "billing_items_match": 0,
            "total_score": 0
        }
        
        # Compare invoice summary fields
        if 'invoice_summary' in expected and 'invoice_summary' in actual:
            expected_summary = expected['invoice_summary']
            actual_summary = actual['invoice_summary']
            
            matches = 0
            total_fields = len(expected_summary)
            
            for key, expected_value in expected_summary.items():
                actual_value = actual_summary.get(key)
                if expected_value == actual_value:
                    matches += 1
                elif expected_value is None and actual_value is None:
                    matches += 1
            
            comparison["invoice_summary_match"] = matches / total_fields if total_fields > 0 else 0
        
        # Compare billing line items
        if 'billing_line_items' in expected and 'billing_line_items' in actual:
            expected_items = expected['billing_line_items']
            actual_items = actual['billing_line_items']
            
            if len(expected_items) == len(actual_items):
                item_matches = 0
                for i, expected_item in enumerate(expected_items):
                    if i < len(actual_items):
                        actual_item = actual_items[i]
                        # Check key fields
                        if (expected_item.get('description') == actual_item.get('description') and
                            expected_item.get('amount') == actual_item.get('amount') and
                            expected_item.get('category') == actual_item.get('category')):
                            item_matches += 1
                
                comparison["billing_items_match"] = item_matches / len(expected_items)
            else:
                comparison["billing_items_match"] = 0
        
        # Overall score
        comparison["total_score"] = (comparison["invoice_summary_match"] + comparison["billing_items_match"]) / 2
        
        return comparison


def main():
    """Generate training data files"""
    generator = LLMTrainingDataGenerator()
    
    print("Generating training data for LLM fine-tuning...")
    
    # Generate different formats
    generator.generate_jsonl_training_data("dhl_training_openai.jsonl")
    generator.generate_claude_training_data("dhl_training_claude.json")
    
    # Test current model performance
    print("\nTesting current model against golden examples...")
    results = generator.validate_against_golden_examples()
    
    for result in results:
        if 'comparison' in result:
            score = result['comparison']['total_score']
            print(f"{result['file']}: {score:.2%} accuracy")
        else:
            print(f"{result['file']}: ERROR - {result.get('error')}")


if __name__ == "__main__":
    main()
