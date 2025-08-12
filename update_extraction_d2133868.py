#!/usr/bin/env python3
"""
Save improved LLM extraction for invoice D2133868 to database
"""

import json
import sqlite3
from datetime import datetime

# The extracted JSON data from our LLM test
extracted_json = {
  "invoice_summary": {
    "invoice_no": "D2133868",
    "invoice_date": "2025-06-25",
    "due_date": "2025-09-23",
    "customer_name": "OUTDOOR WIRELESS NETWORKS AUSTRALIA PTY LTD",
    "currency": "AUD",
    "subtotal": 93.75,
    "gst_total": 9.38,
    "final_total": 103.13,
    "service_type": None,
    "origin": "CNSHA = Shanghai, China",
    "destination": "AUSYD = Sydney, Australia",
    "weight": "7779.400 KG",
    "shipment_ref": "S2502322386",
    "account_number": "AUOU0003/AUOU0003",
    "payment_terms": "90 days from Inv. Date",
    "incoterms": None,
    "transportation_mode": None,
    "masterbill": "757510358700",
    "housebill": "SZVC11136",
    "awb_number": None,
    "shipment_date": "2025-06-01",
    "total_pieces": 40,
    "chargeable_weight": 52.075,
    "volume_weight": 52.075,
    "exchange_rate_eur": None,
    "exchange_rate_usd": None,
    "shipper_name": "COMMSCOPE TELECOMMUNICATIONS CHINA CO LTD",
    "shipper_address": None,
    "consignee_name": "OUTDOOR WIRELESS NETWORKS AUSTRALIA PTY LTD",
    "consignee_address": None,
    "commodity_description": "ANTENNA"
  },
  "billing_line_items": [
    {
      "line_item_index": 1,
      "description": "Dest. Waiting Time Fee 45.00 Mins",
      "amount": 93.75,
      "gst_amount": 9.38,
      "total_amount": 103.13,
      "currency": "AUD",
      "category": "SERVICE_CHARGE"
    }
  ]
}

# Connect to the database
conn = sqlite3.connect('dhl_audit.db')
cursor = conn.cursor()

# Update the existing record for D2133868
try:
    # First, update the main extraction record
    cursor.execute("""
        UPDATE llm_pdf_extractions
        SET extracted_data = ?,
            extraction_confidence = ?,
            processing_timestamp = ?,
            llm_model_used = ?
        WHERE invoice_no = ?
    """, (
        json.dumps(extracted_json),
        0.95,  # High confidence since we manually verified
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'llama3.2:latest',
        'D2133868'
    ))
    
    # Next, update the invoice summary table if it exists
    try:
        invoice_summary = extracted_json["invoice_summary"]
        
        # Check if record exists in llm_invoice_summary
        cursor.execute("SELECT invoice_no FROM llm_invoice_summary WHERE invoice_no = ?", ('D2133868',))
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
                'D2133868'
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
                'D2133868',
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
    except Exception as e:
        print(f"Error updating invoice summary: {e}")
    
    # Update the billing line items if that table exists
    try:
        # First delete any existing line items for this invoice
        cursor.execute("DELETE FROM llm_billing_line_items WHERE invoice_no = ?", ('D2133868',))
        
        # Then insert the new line items
        for item in extracted_json.get("billing_line_items", []):
            cursor.execute("""
                INSERT INTO llm_billing_line_items
                (invoice_no, line_item_index, description, amount, gst_amount, 
                total_amount, currency, category)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                'D2133868',
                item.get("line_item_index"),
                item.get("description"),
                item.get("amount"),
                item.get("gst_amount"),
                item.get("total_amount"),
                item.get("currency"),
                item.get("category")
            ))
    except Exception as e:
        print(f"Error updating billing line items: {e}")
    
    # Commit the changes
    conn.commit()
    print(f"Successfully updated extraction for invoice D2133868")
    
except Exception as e:
    conn.rollback()
    print(f"Error updating database: {e}")

finally:
    conn.close()
