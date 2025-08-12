import sqlite3

conn = sqlite3.connect('dhl_audit.db')
cursor = conn.cursor()

# Fixed lookup function using 30.0kg as the boundary for rate calculation
def calculate_express_rate(weight, zone):
    # For weights over 30kg, we need to use the multiplier formula
    if weight > 30:
        # Get base rate at 30kg
        cursor.execute(f'''
            SELECT zone_{zone} 
            FROM dhl_express_rate_cards 
            WHERE service_type = 'Import'
            AND weight_from = 30
        ''')
        
        base_result = cursor.fetchone()
        if not base_result:
            print(f"Could not find exact 30kg rate, trying closest match")
            # Try to find closest rate to 30kg
            cursor.execute(f'''
                SELECT weight_from, weight_to, zone_{zone} 
                FROM dhl_express_rate_cards 
                WHERE service_type = 'Import'
                AND is_multiplier = 0
                AND zone_{zone} IS NOT NULL
                ORDER BY ABS(weight_from - 30)
                LIMIT 1
            ''')
            
            closest = cursor.fetchone()
            if closest:
                print(f"Found closest rate at weight {closest[0]}kg: ${closest[2]}")
                base_rate = closest[2]
            else:
                print("Could not find any base rate")
                return None
        else:
            base_rate = base_result[0]
            
        # Get multiplier rate for the weight
        cursor.execute(f'''
            SELECT weight_from, weight_to, zone_{zone} 
            FROM dhl_express_rate_cards 
            WHERE service_type = 'Import'
            AND is_multiplier = 1
            AND zone_{zone} IS NOT NULL
            AND weight_from <= ? AND weight_to > ?
        ''', (weight, weight))
        
        multiplier_result = cursor.fetchone()
        if not multiplier_result:
            print("Could not find multiplier rate, using default")
            return None
            
        multiplier = multiplier_result[2]
        
        # Calculate total rate
        half_kg_increments = (weight - 30) / 0.5
        total_rate = base_rate + (half_kg_increments * multiplier)
        
        return {
            'base_rate_at_30kg': base_rate,
            'multiplier_per_half_kg': multiplier,
            'weight_from': multiplier_result[0],
            'weight_to': multiplier_result[1],
            'half_kg_increments': half_kg_increments,
            'total_calculated_rate': total_rate
        }
    else:
        # For weights under 30kg, just find the direct rate
        cursor.execute(f'''
            SELECT weight_from, weight_to, zone_{zone} 
            FROM dhl_express_rate_cards 
            WHERE service_type = 'Import'
            AND is_multiplier = 0
            AND zone_{zone} IS NOT NULL
            AND weight_from <= ? AND weight_to >= ?
        ''', (weight, weight))
        
        rate_result = cursor.fetchone()
        if rate_result:
            return {
                'weight_from': rate_result[0],
                'weight_to': rate_result[1],
                'direct_rate': rate_result[2]
            }
        else:
            return None

# Test with our 51kg shipment
print("TESTING EXPRESS RATE CALCULATION FOR 51kg, ZONE 3")
print("-" * 50)

result = calculate_express_rate(51, 3)
print("\nCalculation result:")
if result:
    for key, value in result.items():
        print(f"  {key}: {value}")
        
    # Compare to invoice amount
    cursor.execute('''
        SELECT amount
        FROM dhl_express_invoices
        WHERE invoice_no LIKE "%MELIR00831127%"
        AND dhl_product_description LIKE "%EXPRESS WORLDWIDE%"
    ''')
    
    invoice_amount = cursor.fetchone()
    if invoice_amount:
        print(f"\nInvoice comparison:")
        print(f"  Invoice amount: ${invoice_amount[0]}")
        print(f"  Calculated amount: ${result['total_calculated_rate']}")
        print(f"  Variance: ${result['total_calculated_rate'] - invoice_amount[0]}")
        variance_pct = (result['total_calculated_rate'] - invoice_amount[0]) / invoice_amount[0] * 100
        print(f"  Variance percentage: {variance_pct:.2f}%")
        
        if abs(variance_pct) <= 5:
            print("\n✅ AUDIT PASSED - Variance within 5%")
        elif abs(variance_pct) <= 15:
            print("\n⚠️ AUDIT NEEDS REVIEW - Variance between 5-15%")
        else:
            print("\n❌ AUDIT FAILED - Variance exceeds 15%")
else:
    print("Could not calculate rate")
