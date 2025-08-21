# DGF Air & Sea Freight Audit System

## Overview

The DGF (DGF China) audit system is designed to audit air and sea freight invoices against spot quotes, since DGF doesn't use traditional rate cards but rather issues spot quotes for each shipment lane.

## Key Features

### 1. Spot Quote Management
- Extracts spot quotes from Excel files as baseline rates
- Stores quotes per lane (origin-destination pairs)
- Handles both air freight (rate per kg) and sea freight (rate per CBM)
- Manages currency conversion with exchange rates

### 2. Invoice Processing
- Processes air and sea freight invoices from Excel files
- Links invoices to corresponding spot quotes by Quote ID
- Handles multi-currency invoicing with local conversion

### 3. Audit Engine
- Compares actual invoice charges against spot quote baselines
- Applies tolerance thresholds (5% for freight rates, 10% for fees)
- Identifies variances and exceptions
- Calculates financial impact (overcharges/undercharges)

### 4. Reporting & Analytics
- Generates detailed audit reports
- Provides variance analysis
- Tracks audit scores and pass rates
- Exports to Excel for further analysis

## File Structure

### Input Files

#### Air Freight File (`DGF Air.xlsx`)
Required columns:
- `报价单号` (Quote ID)
- `实际到港日期` (Actual Arrival Date)
- `分单号` (HBL Number)
- `件数` (Pieces)
- `毛重` (Gross Weight)
- `计费重` (Chargeable Weight)
- `条款` (Terms)
- `发货国` (Origin Country)
- `发货港` (Origin Port)
- `港到港` (Freight)
- `到港币种` (Origin Currency)
- `到港汇率` (Origin FX Rate)
- `目的港费用` (Destination Charges)
- `目的港币种` (Destination Currency)
- `目的港汇率` (Destination FX Rate)
- `总计` (Total CNY)

#### Sea Freight File (`DGF-CN10 billing.xlsx`)
Required columns:
- `报价单号` (Quote ID)
- `实际到港日期` (Actual Arrival Date)
- `分单号` (HBL Number)
- `件数` (Pieces)
- `立方数` (Volume CBM)
- `箱型、箱数` (Container Info)
- `条款` (Terms)
- `发货国` (Origin Country)
- `发货港` (Origin Port)
- `港到港` (Freight)
- `到港币种` (Origin Currency)
- `到港汇率` (Origin FX Rate)
- `目的港费用` (Destination Charges)
- `目的港币种` (Destination Currency)
- `目的港汇率J` (Destination FX Rate)
- `总计` (Total CNY)

## Database Schema

### Tables Created

#### `dgf_spot_quotes`
Stores spot quotes as baseline rates:
- Quote ID, mode (AIR/SEA), lane information
- Rates per kg (air) or per CBM (sea)
- Origin and destination handling fees
- Currency and exchange rate information

#### `dgf_invoices`
Stores processed invoices:
- Invoice details linked to quote IDs
- Shipment information (weight, volume, etc.)
- Charge breakdowns by origin/destination
- Currency conversion details

#### `dgf_audit_results`
Stores audit outcomes:
- Audit status (PASS/WARNING/FAIL)
- Variance percentages and amounts
- Financial impact calculations
- Audit scores and comments

#### `dgf_exchange_rates`
Manages currency exchange rates:
- Historical exchange rates by date
- Multi-currency support
- Rate source tracking

## Usage Instructions

### 1. Web Interface Access

Navigate to the DGF section in the web application:
- **Dashboard**: `/dgf` - Overview and quick actions
- **Upload Files**: `/dgf/upload` - Upload Excel files
- **View Quotes**: `/dgf/quotes` - Browse spot quotes
- **View Invoices**: `/dgf/invoices` - Browse processed invoices
- **Audit Results**: `/dgf/audit/results` - View audit outcomes

### 2. File Upload Process

1. Go to `/dgf/upload`
2. Select DGF Air freight Excel file (optional)
3. Select DGF Sea freight Excel file (optional)
4. Click "Upload and Process Files"
5. System will extract spot quotes and process invoices

### 3. Running Audits

**Via Web Interface:**
- Click "Run Audit" button on dashboard
- Or use dropdown menu: DGF Air & Sea > Run Audit

**Via Command Line:**
```python
from dgf_audit_system import DGFAuditSystem

audit_system = DGFAuditSystem()
results = audit_system.audit_all_invoices()
print(results)
```

### 4. Viewing Results

**Web Dashboard:**
- Summary statistics on main dashboard
- Detailed results at `/dgf/audit/results`
- Filter by mode (Air/Sea) and status (Pass/Warning/Fail)

**Excel Reports:**
- Click "Export Report" to download detailed Excel report
- Contains audit results, summary statistics, and quote analysis

## Audit Logic

### 1. Quote Extraction
- Extracts each row as a spot quote
- Calculates rate per kg (air) or rate per CBM (sea)
- Stores handling fees and surcharges
- Records currency and exchange rate information

### 2. Invoice Matching
- Links invoices to quotes by Quote ID
- Validates route consistency
- Checks weight/volume alignment

### 3. Variance Calculation

#### Freight Rate Variance
- **Air**: Compares (Chargeable Weight × Quote Rate/kg) vs Actual Freight
- **Sea**: Compares (Volume CBM × Quote Rate/CBM) vs Actual Freight
- **Tolerance**: 5% variance allowed

#### Fee Variance
- Compares handling fees between quote and invoice
- **Tolerance**: 10% variance allowed

#### Scoring
- Start with 100 points
- Deduct points for variances:
  - Major freight variance: up to 50 points
  - Fee variance: up to 25 points
- Final status based on score and variance severity

### 4. Status Assignment
- **PASS**: < 5% variance, score > 90
- **WARNING**: 5-15% variance, score 70-90
- **FAIL**: > 15% variance, score < 70

## Financial Impact Analysis

### Overcharge Detection
- Identifies when actual charges exceed quote baseline
- Calculates overcharge amount in CNY
- Tracks by shipment and aggregate totals

### Undercharge Detection
- Identifies when actual charges are below quote baseline
- May indicate billing errors or missed charges
- Important for revenue assurance

### Net Variance
- Overall financial impact per invoice
- Positive = overcharge, Negative = undercharge
- Aggregated for portfolio analysis

## API Endpoints

### REST API Routes
- `POST /dgf/audit/run` - Run audit on all invoices
- `GET /dgf/audit/invoice/{id}` - Audit single invoice
- `GET /dgf/api/stats` - Get audit statistics
- `GET /dgf/reports/generate` - Generate Excel report

### Response Format
```json
{
    "success": true,
    "results": {
        "total_invoices": 78,
        "audited": 78,
        "passed": 66,
        "warnings": 6,
        "failed": 6,
        "errors": 0,
        "total_overcharge": 28.61,
        "total_undercharge": 0.00
    }
}
```

## Configuration

### Tolerance Settings
Edit in `dgf_audit_system.py`:
```python
RATE_TOLERANCE = 0.05  # 5% for freight rates
FEE_TOLERANCE = 0.10   # 10% for handling fees
```

### Database Settings
Default database: `dhl_audit.db`
Can be changed in class initialization:
```python
audit_system = DGFAuditSystem(db_path='custom_audit.db')
```

## Troubleshooting

### Common Issues

1. **File Format Errors**
   - Ensure Excel files have correct sheet names ('Air' for air, 'Sheet1' for sea)
   - Verify column headers match expected Chinese names
   - Check for missing or corrupted data

2. **Quote Matching Failures**
   - Verify Quote IDs are consistent between quotes and invoices
   - Check for special characters or formatting issues
   - Ensure quote exists before processing invoice

3. **Currency Conversion Issues**
   - Verify exchange rates are provided in files
   - Check for zero or null exchange rates
   - Ensure currency codes are valid

4. **Performance Issues**
   - Large files may take time to process
   - Consider processing in batches for very large datasets
   - Monitor database size and optimize as needed

### Debug Mode
Enable detailed logging:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Best Practices

### 1. Data Quality
- Regularly validate input data quality
- Check for missing or inconsistent Quote IDs
- Verify exchange rates are current and accurate

### 2. Regular Auditing
- Run audits after each file upload
- Review high-variance shipments promptly
- Investigate systematic variances by lane or time period

### 3. Reporting
- Generate monthly audit reports
- Track trends in pass rates and variances
- Share findings with procurement and finance teams

### 4. Maintenance
- Backup database regularly
- Archive old audit results periodically
- Update tolerance thresholds based on business requirements

## Integration

### With Existing Systems
The DGF audit system integrates with the broader China Logistics Audit platform:
- Shared user authentication
- Common database infrastructure
- Unified reporting dashboard
- Consistent UI/UX patterns

### Data Export
- Excel reports for further analysis
- API endpoints for system integration
- Database views for business intelligence tools

## Future Enhancements

### Planned Features
1. **Automated Exchange Rate Updates**
   - Real-time FX rate integration
   - Historical rate validation

2. **Advanced Analytics**
   - Trend analysis and forecasting
   - Lane-level performance metrics
   - Vendor performance scorecards

3. **Machine Learning Integration**
   - Anomaly detection for unusual patterns
   - Predictive analytics for rate negotiations
   - Automated exception handling

4. **Enhanced Reporting**
   - Interactive dashboards
   - Custom report templates
   - Automated email notifications

This comprehensive audit system provides DGF with the tools needed to ensure their spot quote-based invoicing is accurate and compliant with their baseline rates.
