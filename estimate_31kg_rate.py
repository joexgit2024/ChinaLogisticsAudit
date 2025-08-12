"""
Test calculation for 31kg shipment based on 30kg rate
For MELIR00807085: DE -> AU, Zone 12, 31kg, Amount: $278.35

Expected calculation:
- 30kg base rate: $271.11
- Additional 1kg (2 × 0.5kg increments) 
- Need to estimate adder rate or use interpolation
"""

# From the rate card, we have:
# 29.5-30kg: $267.49
# 30-30kg: $271.11

# The difference suggests an increment of about $3.62 per 0.5kg
# So for 31kg (2 additional 0.5kg increments):
base_30kg = 271.11
increment_per_half_kg = 3.62  # Estimated from 29.5kg to 30kg difference
additional_weight = 1.0  # 31kg - 30kg
increments = additional_weight / 0.5  # 2 increments
estimated_adder = increment_per_half_kg * increments

estimated_31kg_rate = base_30kg + estimated_adder
invoiced_amount = 278.35
variance = estimated_31kg_rate - invoiced_amount

print(f"30kg base rate: ${base_30kg:.2f}")
print(f"Estimated increment per 0.5kg: ${increment_per_half_kg:.2f}")
print(f"Additional weight: {additional_weight}kg ({increments} increments)")
print(f"Estimated adder: ${estimated_adder:.2f}")
print(f"Estimated 31kg rate: ${estimated_31kg_rate:.2f}")
print(f"Invoiced amount: ${invoiced_amount:.2f}")
print(f"Variance: ${variance:.2f}")

if abs(variance) <= invoiced_amount * 0.05:
    result = "PASS"
elif abs(variance) <= invoiced_amount * 0.15:
    result = "REVIEW"
else:
    result = "FAIL"
    
print(f"Audit result: {result}")
