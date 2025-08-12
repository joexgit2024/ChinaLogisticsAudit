"""
CAPTURED DATA vs DATABASE MAPPING ANALYSIS
===========================================

From the shipment details image, here are the fields we're capturing vs. what we have in our DB:

=== CAPTURED DATA FIELDS ===
1. Client: ANDA
2. Carrier: DZNA  
3. Account: AUOU0003
4. Account Period: ANDA302125
5. Billed To: PP-PREPAID
6. Tracking Number: G097923
7. Invoice Number: D2142163
8. Invoice Date: 2025-06-30
9. Invoice Amount: 1599.43
10. Net Charge: 1599.43
11. Invoice Status: OPEN
12. Audit Status: EXCEPTION
13. BOL: (empty)
14. PO: (empty)
15. Ref: 9010031679
16. Ref.2: (empty)

=== SHIPPER/RECIPIENT/THIRD PARTY/BILL TO ===
17. Shipper Company: COMMSCOPE TELECOMMUNICATIONS CHINA CO LTD
18. Shipper Address: NO 68 SU HONG XI ROAD
19. Shipper City: SUZHOU
20. Shipper State: 32
21. Shipper Zip: 215021
22. Shipper Country: CHN

23. Recipient Company: ONE NEW ZEALAND GROUP LIMITED
24. Recipient Address: 74 TAHAROTO ROAD
25. Recipient City: AUCKLAND
26. Recipient State: (empty)
27. Recipient Zip: 0622
28. Recipient Country: NZL

29. Bill To Company: OUTDOOR WIRELESS NETWORKS AUSTRALIA PTY LTD
30. Bill To Address: 22 INDUSTRY BOULEVARD
31. Bill To City: CARRUM DOWNS
32. Bill To State: VI
33. Bill To Zip: 3201
34. Bill To Country: AUS

=== GENERAL INFORMATION ===
35. Currency: AUD
36. Check Number: (empty)
37. Check Date: (empty)
38. Ship Date: 2025-06-13
39. Delivery Date: 2025-06-24
40. Shipment Entered: (empty)
41. Invoice Created Date: 2025-06-30
42. Shipping Mode: AIR FREIGHT
43. Service: ACDA
44. Del. Commitment: (empty)
45. Declared Value: (empty)
46. Bill Weight: 117
47. Ship Weight: 72
48. Volume: (empty)
49. Commodity: GEN

=== ADDITIONAL INFORMATION ===
50. reference_additional: 9010031679
51. container_number_additional: (empty)
52. container_type_additional: (empty)
53. seal_number_additional: (empty)
54. work_order_number_additional: (empty)
55. equipment_number_additional: (empty)

=== BOTTOM SECTION DATA ===
56. shipper_type: ANDA
57. pc_all_carr_chg_codes: [DZNA+400, DZNA+CHC, DZNA+DEL, DZNA+PUC, DZNA+THC]
58. pc_all_carr_acct_chg_codes: [DZNA+AUOU0003+400, DZNA+AUOU0003+CHC, DZNA+AUOU0003+DEL, DZNA+AUOU0003+PUC, DZNA+AUOU0003+THC]
59. vendor_number: 873532
60. cust_vat_reg_copy: 22679985221
61. sap_plant: CN10
62. shipper_co_code: 3601
63. mode: AIR
64. allocation_percentage: 100
65. master_shipper_address: NO.68 SUHONG XI ROAD SUZHOU INDUSTRIAL PARK
66. company_code: 1326
67. shipper_description: MFG
68. gl_account: 5121500
69. carrier_name: DHL GLOBAL FORWARDING AUSTRALIA
70. direction: OUTBOUND
71. charge_group: NON_TAXABLE
72. recipient_description: N/A
73. partner_bank_type: AU1
74. profit_center: 30999
75. carrier_vat_reg: 62002636124
76. cust_vat_reg: 22679985221
77. recipient_type: N/A
78. carrier_country: AUS
79. shipper_plant: CN10
80. tax_code: O2

=== OUR CURRENT DATABASE SCHEMA ===

invoices table has:
- id, invoice_number ✓
- shipper_name, shipper_address, shipper_city, shipper_state, shipper_postal_code, shipper_country ✓
- consignee_name, consignee_address, consignee_city, consignee_state, consignee_postal_code, consignee_country ✓
- bill_to_name, bill_to_address, bill_to_city, bill_to_state, bill_to_postal_code, bill_to_country ✓
- vessel_name, container_number, bill_of_lading, booking_number ✓
- origin_port, destination_port ✓
- pickup_date, delivery_date, service_date ✓
- reference_number, pro_number ✓
- total_charges, weight, pieces, currency, exchange_rate ✓
- audit_status, audit_notes ✓
- raw_edi, uploaded_file_path, created_at, updated_at ✓

=== MISSING FIELDS THAT WE SHOULD ADD ===

1. **Client/Account Information:**
   - client_code (ANDA)
   - carrier_code (DZNA)
   - account_number (AUOU0003)
   - account_period (ANDA302125)
   - billed_to_type (PP-PREPAID)

2. **Tracking & Status:**
   - tracking_number (G097923)
   - invoice_date (separate from created_at)
   - invoice_status (OPEN/CLOSED)
   - audit_exception_status (EXCEPTION)

3. **Financial Information:**
   - net_charge (might be different from total_charges)
   - invoice_amount (1599.43)
   - check_number
   - check_date

4. **Shipping Details:**
   - ship_date (different from pickup_date)
   - shipment_entered_date
   - invoice_created_date (different from created_at)
   - shipping_mode (AIR FREIGHT)
   - service_type (ACDA)
   - delivery_commitment
   - declared_value
   - bill_weight (different from weight)
   - ship_weight
   - volume
   - commodity_type (GEN)

5. **Business Data:**
   - vendor_number (873532)
   - customer_vat_registration (22679985221)
   - sap_plant (CN10)
   - shipper_company_code (3601)
   - mode (AIR)
   - allocation_percentage (100)
   - master_shipper_address
   - company_code (1326)
   - shipper_description (MFG)
   - gl_account (5121500)
   - carrier_name (DHL GLOBAL FORWARDING AUSTRALIA)
   - direction (OUTBOUND)
   - charge_group (NON_TAXABLE)
   - recipient_description
   - partner_bank_type (AU1)
   - profit_center (30999)
   - carrier_vat_registration (62002636124)
   - recipient_type
   - carrier_country
   - shipper_plant
   - tax_code (O2)

6. **Charge Code Information:**
   - pc_all_carr_chg_codes (array of charge codes)
   - pc_all_carr_acct_chg_codes (array of account-specific charge codes)

=== RECOMMENDATION ===

We should enhance our database schema to capture these additional business-critical fields for comprehensive audit analysis.
"""
