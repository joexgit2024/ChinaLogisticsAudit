# FedEx Audit Database Schema Reference

**Database**: `fedex_audit.db`  
**Last Updated**: August 17, 2025  
**Purpose**: Complete field reference to avoid syntax errors in queries and scripts

---

## 📋 Table Summary

| Table Name | Columns | Purpose |
|------------|---------|---------|
| `fedex_invoices` | 29 | **PRIMARY TABLE** - Main invoice and AWB data |
| `fedex_rate_cards` | 13 | Rate lookup table with zones and pricing |
| `fedex_zone_matrix` | 6 | Country to zone mapping |
| `fedex_audit_results` | 14 | Detailed audit results (currently unused) |
| `fedex_surcharges` | 16 | Surcharge definitions and rates |
| `fedex_service_types` | 9 | Service type definitions |
| `fedex_country_zones` | 10 | Country and zone information |
| `fedex_fuel_surcharge_index` | 6 | Fuel surcharge rates by price range |
| `fedex_service_discounts` | 9 | Service discount rates |
| `fedex_zone_regions` | 6 | Zone region definitions |
| `fedex_rate_card_uploads` | 13 | Rate card upload tracking |
| `fedex_rate_card_versions` | 7 | Rate card version management |

---
FEDEX_DB_SCHEMA_REFERENCE.md
## 🎯 Primary Tables (Most Used)

### 1. `fedex_invoices` - Main Invoice Data
**Status**: ✅ ACTIVE - Contains all invoice and AWB data with audit results

| Column | Type | Nullable | Default | Notes |
|--------|------|----------|---------|--------|
| `id` | INTEGER | Yes | - | Primary Key |
| `invoice_no` | TEXT | Yes | - | **Invoice number (e.g., '948921914')** |
| `invoice_date` | TEXT | Yes | - | Invoice date |
| `awb_number` | TEXT | Yes | - | **AWB tracking number** |
| `service_type` | TEXT | Yes | - | Service code (e.g., '0401', '2P01') |
| `service_abbrev` | TEXT | Yes | - | Service abbreviation (e.g., 'IE', '2P') |
| `direction` | TEXT | Yes | - | 'inbound' or 'outbound' |
| `pieces` | INTEGER | Yes | - | Number of pieces |
| `actual_weight_kg` | REAL | Yes | - | **Actual weight in kg** |
| `chargeable_weight_kg` | REAL | Yes | - | FedEx chargeable weight |
| `dim_weight_kg` | REAL | Yes | - | Dimensional weight |
| `origin_country` | TEXT | Yes | - | **Origin country code (e.g., 'US', 'HK')** |
| `dest_country` | TEXT | Yes | - | **Destination country code (e.g., 'CN')** |
| `origin_loc` | TEXT | Yes | - | Origin location code |
| `ship_date` | TEXT | Yes | - | Ship date |
| `delivery_datetime` | TEXT | Yes | - | Delivery datetime |
| `exchange_rate` | REAL | Yes | - | **USD to CNY exchange rate** |
| `rated_amount_cny` | REAL | Yes | - | Base rated amount in CNY |
| `discount_amount_cny` | REAL | Yes | - | Discount amount |
| `fuel_surcharge_cny` | REAL | Yes | - | Fuel surcharge amount |
| `other_surcharge_cny` | REAL | Yes | - | Other surcharges |
| `vat_amount_cny` | REAL | Yes | - | VAT amount |
| `total_awb_amount_cny` | REAL | Yes | - | **Total claimed amount in CNY** |
| `raw_json` | TEXT | Yes | - | Original invoice JSON data |
| `audit_status` | TEXT | Yes | - | **'PASS'/'OVERCHARGE'/'UNDERCHARGE'/'FAIL'** |
| `expected_cost_cny` | REAL | Yes | - | **Audit calculated expected cost** |
| `variance_cny` | REAL | Yes | - | **Variance (claimed - expected)** |
| `audit_timestamp` | TEXT | Yes | - | When audit was performed |
| `audit_details` | TEXT | Yes | - | Audit calculation details |

### 2. `fedex_rate_cards` - Rate Lookup
**Status**: ✅ ACTIVE - Contains 2,496 rate entries

| Column | Type | Nullable | Default | Notes |
|--------|------|----------|---------|--------|
| `id` | INTEGER | Yes | - | Primary Key |
| `rate_card_name` | VARCHAR(200) | No | - | Rate card name |
| `service_type` | VARCHAR(50) | No | - | **'PRIORITY_EXPRESS'/'PRIORITY'/'ECONOMY_EXPRESS'** |
| `origin_region` | VARCHAR(20) | Yes | - | Origin region |
| `destination_region` | VARCHAR(20) | Yes | - | Destination region |
| `zone_code` | VARCHAR(10) | Yes | - | **Zone letter (A, B, C, ... P)** |
| `weight_from` | DECIMAL(10,3) | No | - | **Weight range start (exact weight match)** |
| `weight_to` | DECIMAL(10,3) | No | - | **Weight range end (same as weight_from)** |
| `rate_usd` | DECIMAL(10,2) | No | - | **Rate in USD** |
| `rate_type` | VARCHAR(20) | Yes | 'STANDARD' | **'IP'/'PAK'/'IPKG'/'IEKG'/'OL'** |
| `effective_date` | DATE | Yes | - | Effective date |
| `expiry_date` | DATE | Yes | - | Expiry date |
| `created_timestamp` | DATETIME | Yes | CURRENT_TIMESTAMP | Creation timestamp |

### 3. `fedex_zone_matrix` - Zone Mapping
**Status**: ✅ ACTIVE - Maps countries to zones

| Column | Type | Nullable | Default | Notes |
|--------|------|----------|---------|--------|
| `id` | INTEGER | Yes | - | Primary Key |
| `origin_country` | TEXT | No | - | **Full country name (e.g., 'United States, PR', 'Hong Kong')** |
| `destination_region` | TEXT | No | - | Destination region |
| `zone_letter` | TEXT | No | - | **Zone letter (A, B, C, etc.)** |
| `active` | INTEGER | Yes | 1 | Active flag |
| `created_timestamp` | TEXT | Yes | CURRENT_TIMESTAMP | Creation timestamp |

---

## 🔧 Common Query Patterns

### Get Invoice Data
```sql
SELECT invoice_no, awb_number, origin_country, dest_country, 
       actual_weight_kg, total_awb_amount_cny, exchange_rate,
       audit_status, expected_cost_cny, variance_cny
FROM fedex_invoices 
WHERE invoice_no = '948921914'
```

### Get Rate for Weight and Zone
```sql
SELECT rate_usd, rate_type 
FROM fedex_rate_cards 
WHERE service_type = 'PRIORITY_EXPRESS' 
AND zone_code = 'F' 
AND weight_from = 8.5 
AND weight_to = 8.5
AND rate_type = 'IP'
```

### Get Zone Mapping
```sql
SELECT zone_letter 
FROM fedex_zone_matrix 
WHERE origin_country = 'United States, PR' 
AND destination_region LIKE '%CN%'
```

### Count Audit Status
```sql
SELECT audit_status, COUNT(*) 
FROM fedex_invoices 
WHERE audit_status IS NOT NULL 
GROUP BY audit_status
```

---

## ⚠️ Important Notes

### Country Code Mapping
- **Database stores FULL country names**, not short codes
- **US** → `'United States, PR'`
- **HK** → `'Hong Kong'`
- **CN** → `'China'`
- **JP** → `'Japan'`

### Rate Card Structure
- **Weight matching**: Use exact weight values (8.5, 9.0, 9.5, etc.)
- **Rate types**: 
  - `'IP'` = Individual Package (≤20.5kg)
  - `'IPKG'` = Individual Package per kg (>20kg)
  - `'PAK'` = Pak rates
  - `'OL'` = Over Length

### Audit Status Values
- `'PASS'` = Within 2% tolerance
- `'OVERCHARGE'` = Claimed > Expected
- `'UNDERCHARGE'` = Claimed < Expected  
- `'FAIL'` = Audit failed
- `NULL` = Not audited yet

### Weight Rounding Rules
- **≤21kg**: Round up to next 0.5kg increment
- **>21kg**: Round up to next full kg

---

## 🚨 Common Mistakes to Avoid

1. **Wrong Country Names**: Don't use 'US', use 'United States, PR'
2. **Wrong Column Names**: 
   - ❌ `tracking_number` → ✅ `awb_number`
   - ❌ `invoice_id` → ✅ `invoice_no`
   - ❌ `weight_kg` → ✅ `actual_weight_kg`
3. **Wrong Zone Query**: Don't use `destination_country`, use `destination_region LIKE '%CN%'`
4. **Wrong Rate Query**: Don't use weight ranges, use exact weight match

---

## 📊 Data Validation

### Current Data Counts
- **Total AWBs**: 214
- **Total Invoices**: 73
- **Rate Cards**: 2,496 entries
- **Zone Mappings**: Available for major countries
- **Audited AWBs**: Updates dynamically

### Sample Valid Values
- **Service Types**: '0401', '2P01', '7001', '2P03', '0404'
- **Zones**: A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P
- **Countries**: 'US', 'HK', 'CN', 'JP' (but use full names in queries)
- **Rate Types**: 'IP', 'IPKG', 'PAK', 'IEKG', 'OL'

---

*This reference document should be consulted before writing any SQL queries, scripts, or database operations to ensure correct field names and data types.*
