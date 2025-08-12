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
