# DHL Express Rate Card Import Mapping Guide

**File:** `uploads\ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx`  
**Last Updated:** August 2, 2025  
**Purpose:** Complete mapping of Excel sheets to database tables for DHL Express rate card system

---

## 📋 File Overview

This Excel file contains **17 sheets** with DHL Express pricing data for Australia-based operations. The system processes **6 core sheets** into database tables while **11 sheets** contain reference/administrative data.

### File Structure
```
ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx
├── AU Customer Cover                    [Reference]
├── AU TD Exp WW                        ✅ [Export Rates]
├── AU TD Imp WW                        ✅ [Import Rates]  
├── AU Zones TDI Export                 [Reference]
├── AU Zones TDI Import                 [Reference]
├── AU TD 3rdCty WW                     ✅ [3rd Party Rates]
├── AU Zones 3rdCty TD                  ✅ [3rd Party Zones]
├── AU Matrix TD 3rdCtry                ✅ [3rd Party Matrix]
├── AU TD 3rdCty DOMESTIC               [Reference]
├── AU Matrix TD 3rdCtry Dom            [Reference]
├── S&S Published                       ✅ [Service Charges]
├── S&S Special Agreement               [Reference]
├── Accounts                            [Reference]
├── AU Service Conditions               [Reference]
├── AU Commercial Terms                 [Reference]
├── Payment Terms                       [Reference]
└── DSX 20240712                        [Reference]
```

---

## 🗂️ Sheet-to-Table Mapping

### 1. Main Rate Cards (Import/Export)

#### **AU TD Imp WW** → `dhl_express_rate_cards`
- **Purpose:** Import rates for shipments destined to Australia
- **Service Type:** Import
- **Loader Script:** `improved_dhl_express_rate_card_loader.py`
- **Loader Method:** `_load_service_rates()`
- **Sections Processed:**
  - Documents (0.5-2kg): Fixed rate for document shipments
  - Non-documents (0.5-30kg): Weight-based rates by zone
  - Multiplier rates (30kg+): Multiplier-based pricing
- **Zone Coverage:** Zones 1-9
- **Usage:** Audit engine for shipments with AU destination

#### **AU TD Exp WW** → `dhl_express_rate_cards`
- **Purpose:** Export rates for shipments originating from Australia
- **Service Type:** Export
- **Loader Script:** `improved_dhl_express_rate_card_loader.py`
- **Loader Method:** `_load_service_rates()`
- **Sections Processed:**
  - Documents (0.5-2kg): Fixed rate for document shipments
  - Non-documents (0.5-30kg): Weight-based rates by zone
  - Multiplier rates (30kg+): Multiplier-based pricing
- **Zone Coverage:** Zones 1-9
- **Usage:** Audit engine for shipments with AU origin

### 2. Third Party Rates (Non-AU Shipments)

#### **AU TD 3rdCty WW** → `dhl_express_3rd_party_rates`
- **Purpose:** Rates for shipments neither originating nor destined to Australia
- **Service Type:** 3rd Party
- **Loader Script:** `complete_3rd_party_loader.py`
- **Loader Method:** `load_rate_data()`
- **Rate Structure:** Weight-based rates by zone (A-H)
- **Weight Range:** 0.5kg to 200kg+
- **Usage:** Audit engine for 3rd party shipments (e.g., JP→NZ)

#### **AU Zones 3rdCty TD** → `dhl_express_3rd_party_zones`
- **Purpose:** Country to zone mapping for 3rd party pricing
- **Service Type:** 3rd Party
- **Loader Script:** `complete_3rd_party_loader.py`
- **Loader Method:** `load_zone_mappings()`
- **Structure:** Country Code → Zone (1-5)
- **Examples:**
  - JP (Japan) → Zone 4
  - NZ (New Zealand) → Zone 5
- **Usage:** First step in 3rd party rate lookup

#### **AU Matrix TD 3rdCtry** → `dhl_express_3rd_party_matrix`
- **Purpose:** Zone intersection matrix for 3rd party rate zone determination
- **Service Type:** 3rd Party
- **Loader Script:** `complete_3rd_party_loader.py`
- **Loader Method:** `load_matrix_data()`
- **Structure:** Origin Zone × Destination Zone → Rate Zone (A-H)
- **Example:** Zone 4 × Zone 5 → Zone D
- **Usage:** Second step in 3rd party rate lookup

### 3. Service Charges

#### **S&S Published** → `dhl_express_services_surcharges`
- **Purpose:** Additional service charges and surcharges
- **Service Type:** All (applies to Import/Export/3rd Party)
- **Preprocessing Required:** ⚠️ **Must be demerged first using `demerge_services_surcharges.py`**
- **Demerged File:** `uploads\ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898_demerged.xlsx`
- **Loader Script:** `improved_dhl_express_rate_card_loader.py`
- **Loader Method:** `_load_service_charges()`
- **Content:** Published service charges beyond base shipping rates
- **Usage:** Additional charges in audit calculations

### 4. Premium Services

#### **Premium Services** → `dhl_express_premium_services`
- **Purpose:** Premium service offerings extracted from rate sheets
- **Service Type:** All
- **Loader Script:** `improved_dhl_express_rate_card_loader.py`
- **Loader Method:** `_load_premium_services()`
- **Source:** Embedded within AU TD Imp WW and AU TD Exp WW sheets
- **Usage:** Premium service rate calculations

---

## 🗃️ Database Tables Structure

### Primary Tables

```sql
-- Main rate cards for Import/Export
dhl_express_rate_cards (
    service_type,           -- 'Import' or 'Export'
    rate_section,           -- 'Documents', 'Non-documents', 'Multiplier'
    weight_from,            -- Starting weight
    weight_to,              -- Ending weight
    zone_1 to zone_9,       -- Rates for zones 1-9
    is_multiplier,          -- Boolean for multiplier rates
    weight_range_from,      -- Multiplier weight range start
    weight_range_to         -- Multiplier weight range end
)

-- 3rd party country-zone mappings
dhl_express_3rd_party_zones (
    country_code,           -- ISO country code (e.g., 'JP', 'NZ')
    zone,                   -- Zone number (1-5)
    region                  -- Optional region description
)

-- 3rd party zone intersection matrix
dhl_express_3rd_party_matrix (
    origin_zone,            -- Origin zone (1-5)
    destination_zone,       -- Destination zone (1-5)
    rate_zone              -- Rate zone letter (A-H)
)

-- 3rd party rates
dhl_express_3rd_party_rates (
    weight_kg,              -- Weight in kilograms
    zone_a to zone_h       -- Rates for zones A-H
)

-- Service charges
dhl_express_services_surcharges (
    service_name,           -- Name of service/surcharge
    description,            -- Service description
    rate_structure,         -- How the rate is calculated
    rate_value             -- Rate amount or percentage
)

-- Premium services
dhl_express_premium_services (
    service_type,           -- 'Import' or 'Export'
    service_name,           -- Premium service name
    rate_structure,         -- Rate calculation method
    base_rate              -- Base rate for service
)
```

---

## 🔄 Import Process Workflow

### Step 0: Preprocessing (S&S Published Only)
```bash
python demerge_services_surcharges.py
```
**Purpose:** Demerge the S&S Published sheet to make it computer-friendly
**Input:** `uploads\ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx`
**Output:** `uploads\ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898_demerged.xlsx`
**Note:** This step populates merged cells and creates a structured format for loading

### Step 1: Main Rate Cards
```bash
python improved_dhl_express_rate_card_loader.py
```
**Loads:**
- AU TD Imp WW → `dhl_express_rate_cards` (Import)
- AU TD Exp WW → `dhl_express_rate_cards` (Export)
- S&S Published (from demerged file) → `dhl_express_services_surcharges`
- Premium Services → `dhl_express_premium_services`

### Step 2: 3rd Party Data
```bash
python complete_3rd_party_loader.py
```
**Loads:**
- AU Zones 3rdCty TD → `dhl_express_3rd_party_zones`
- AU Matrix TD 3rdCtry → `dhl_express_3rd_party_matrix`
- AU TD 3rdCty WW → `dhl_express_3rd_party_rates`

### Step 3: Integration Test
```bash
python dhl_express_audit_engine.py
```
**Tests:** Complete audit system with all rate card types

---

## ⚠️ Important Preprocessing Note

The **S&S Published** sheet requires special preprocessing before it can be imported:

### Why Demerging is Required
- The original S&S Published sheet contains **merged cells** and irregular formatting
- Excel merged cells are not computer-friendly for data processing
- The `demerge_services_surcharges.py` script converts it to a structured format

### Demerging Process
1. **Script:** `demerge_services_surcharges.py`
2. **Input Sheet:** `S&S Published` from original Excel file
3. **Output:** Creates `*_demerged.xlsx` file with `Services_Demerged` sheet
4. **Result:** Populates all blank rows and unmerges cells for proper data loading

### File Flow
```
Original: uploads\ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898.xlsx
         ↓ (demerge_services_surcharges.py)
Demerged: uploads\ID_104249_01_AP_V01_Commscope_AU_20241113-074659-898_demerged.xlsx
         ↓ (improved_dhl_express_rate_card_loader.py)
Database: dhl_express_services_surcharges table
```

---

## 🎯 Audit Engine Logic

### Rate Card Selection Logic
```python
def determine_rate_card_type(origin_country, destination_country):
    if origin_country == 'AU':
        return 'Export'
    elif destination_country == 'AU':
        return 'Import'
    else:
        return '3rd Party'
```

### Import/Export Lookup Process
1. **Determine service type** (Import vs Export)
2. **Find rate section** (Documents vs Non-documents vs Multiplier)
3. **Calculate weight bracket** (0.5kg increments up to 30kg)
4. **Apply zone-based rate** (Zones 1-9)

### 3rd Party Lookup Process
1. **Country to Zone Mapping**
   - Origin country → Origin zone (1-5)
   - Destination country → Destination zone (1-5)

2. **Zone Matrix Lookup**
   - Origin zone × Destination zone → Rate zone (A-H)

3. **Rate Application**
   - Weight + Rate zone → Final rate

**Example:** JP→NZ, 15kg
- JP → Zone 4, NZ → Zone 5
- Zone 4 × Zone 5 → Zone D
- 15kg Zone D → $461.09

---

## 📊 Unused Sheets (Reference Only)

These sheets contain administrative or reference data not loaded into the audit system:

| Sheet Name | Content Type |
|------------|--------------|
| AU Customer Cover | Cover page/introduction |
| AU Zones TDI Export | Zone reference for Export |
| AU Zones TDI Import | Zone reference for Import |
| AU TD 3rdCty DOMESTIC | Domestic 3rd party rates |
| AU Matrix TD 3rdCtry Dom | Domestic matrix |
| S&S Special Agreement | Special agreement terms |
| Accounts | Account information |
| AU Service Conditions | Service terms |
| AU Commercial Terms | Commercial conditions |
| Payment Terms | Payment conditions |
| DSX 20240712 | Date-specific reference |

---

## 🔍 Validation & Testing

### Key Test Cases
1. **Import Test:** CN→AU, 1kg, Zone 6 → Expected rate validation
2. **Export Test:** AU→US, 2kg, Zone 2 → Rate card lookup
3. **3rd Party Test:** JP→NZ, 15kg → $461.09 (verified)

### Database Verification Queries
```sql
-- Check zone mappings
SELECT country_code, zone FROM dhl_express_3rd_party_zones 
WHERE country_code IN ('JP', 'NZ');

-- Check matrix lookup
SELECT rate_zone FROM dhl_express_3rd_party_matrix 
WHERE origin_zone = 4 AND destination_zone = 5;

-- Check rate lookup
SELECT zone_d FROM dhl_express_3rd_party_rates 
WHERE weight_kg = 15;
```

---

## 📝 Notes & Considerations

1. **Weight Increments:** Import/Export use 0.5kg increments, 3rd party varies
2. **Zone Coverage:** Import/Export use zones 1-9, 3rd party uses zones A-H
3. **Service Types:** System handles Import, Export, and 3rd Party distinctly
4. **Data Quality:** Original file had duplicate weight ranges, fixed in loader
5. **Rate Selection:** Uses `weight_from <= actual_weight <= weight_to` logic

---

## 🔧 Maintenance

### Regular Updates
- **Rate Card Refresh:** When new Excel files are received
- **Zone Mapping Updates:** When country assignments change
- **Service Charge Updates:** When published rates change
- **Demerged File Updates:** Re-run demerging when S&S Published format changes

### Troubleshooting
- **Missing Rates:** Check sheet name variations
- **Duplicate Entries:** Review weight range logic
- **Zone Mismatches:** Verify zone mapping accuracy
- **S&S Loading Issues:** Ensure demerging step was completed successfully
- **Merged Cell Problems:** Use `demerge_services_surcharges.py` before loading

### Complete Import Sequence
```bash
# 1. Preprocess S&S Published sheet
python demerge_services_surcharges.py

# 2. Load main rate cards (uses demerged file for S&S)
python improved_dhl_express_rate_card_loader.py

# 3. Load 3rd party data
python complete_3rd_party_loader.py

# 4. Test complete system
python dhl_express_audit_engine.py
```

---

*This document serves as the definitive reference for DHL Express rate card data mapping and should be updated when the system structure changes.*
