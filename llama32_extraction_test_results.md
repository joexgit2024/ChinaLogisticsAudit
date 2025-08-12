# DHL Invoice Extraction Test with Llama 3.2

## Summary of Results

We conducted a test extraction of invoice data from invoice D2133868 using the Llama 3.2 model. The test showed very promising results:

1. **Extraction Accuracy**: The model successfully extracted all key information from the invoice with high accuracy, including:
   - Invoice number, date, and due date
   - Customer information
   - Shipment details
   - Line items with correct categorization
   - Monetary amounts with correct currency

2. **Schema Adherence**: When provided with a structured schema, the model followed the format exactly, outputting the data in the required JSON structure.

3. **Consistency**: Multiple test runs produced identical results, indicating high reliability of the model for this task.

4. **Advantage over Previous Extraction**: The database previously contained only minimal data for this invoice (just the invoice number), but our test extracted the complete set of information.

## Key Extraction Details

The invoice D2133868 was successfully extracted with the following key details:

```json
{
  "invoice_summary": {
    "invoice_no": "D2133868",
    "invoice_date": "2025-06-25",
    "due_date": "2025-09-23",
    "customer_name": "OUTDOOR WIRELESS NETWORKS AUSTRALIA PTY LTD",
    "currency": "AUD",
    "subtotal": 93.75,
    "gst_total": 9.38,
    "final_total": 103.13,
    "origin": "CNSHA = Shanghai, China",
    "destination": "AUSYD = Sydney, Australia",
    "weight": "7779.400 KG",
    "shipment_ref": "S2502322386"
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
```

## Methodology

1. Retrieved the raw PDF text from the database
2. Created a custom prompt with the expected schema structure
3. Used the Llama 3.2 model for extraction
4. Verified the results against the original invoice data
5. Updated the database with the improved extraction

## Recommendations

1. **Adopt Llama 3.2 Model**: The Llama 3.2 model shows excellent performance for invoice data extraction tasks.

2. **Use Schema-Guided Extraction**: Providing a clear schema structure significantly improves extraction accuracy and consistency.

3. **Include Few-Shot Examples**: Adding examples of correctly formatted extractions to the prompt helps the model understand the desired output format.

4. **Implement Validation**: Add post-processing validation to ensure all extracted data meets the expected formats and business rules.

5. **Batch Re-processing**: Consider re-processing previously processed invoices with incomplete extractions using the improved method.

## Next Steps

1. Update the main extraction code to use the improved schema-driven approach with Llama 3.2
2. Implement batch processing for previously incomplete extractions
3. Add validation rules to ensure data quality
4. Monitor and collect metrics on extraction accuracy over time

This test demonstrates that with the right prompt engineering and model selection, we can achieve highly accurate and consistent invoice data extraction.
