# China Logistics Audit Application

A comprehensive Python Flask web application for auditing China logistics and shipping invoices with advanced zone-based pricing validation, LLM-enhanced PDF processing, and multi-modal audit capabilities supporting major Chinese logistics providers.

## 🚀 Features

### Core Functionality
- **Multi-Format Processing**: EDI files (X12 format), CSV invoice files, and PDF documents
- **Invoice Data Extraction**: Extract shipping details, charges, and metadata from multiple sources
- **Zone-Based Rate Validation**: Advanced China logistics zone mapping with Import/Export/Domestic rate cards
- **China Domestic Audit Engine**: Complete Chinese domestic audit system with province-level coverage
- **LLM-Enhanced PDF Processing**: AI-powered PDF invoice extraction using local Ollama/DeepSeek-R1
- **Detailed Audit Analysis**: Line-by-line charge comparison with variance analysis
- **Multi-Modal Support**: Air freight and Ocean freight (FCL/LCL) auditing
- **Fuzzy Matching**: Advanced city/port matching for rate card selection

### China Logistics Audit System
- **Zone-Based Pricing**: Province-based domestic and international structure
- **Domestic Engine**: Comprehensive China domestic audit covering all provinces
- **Rate Card Types**: Export, Import, Domestic, and Cross-border rate validation
- **Weight-Based Calculations**: Accurate weight tier and adder rate calculations
- **Service Charge Validation**: Premium services and surcharge verification

### Advanced Processing
- **LLM Integration**: AI-powered PDF invoice extraction using Ollama/DeepSeek-R1
- **Batch Processing**: Multi-invoice batch audit with detailed reporting
- **Sortable Tables**: Interactive invoice listings with column sorting
- **Real-time Audit**: Live invoice validation with confidence scoring
- **Authentication System**: Corporate email-based secure access

### Transportation Modes
- **Air Freight**: Weight-based pricing with handling fees and surcharges
- **Ocean Freight**: Container-based pricing (20ft/40ft FCL) with detailed breakdown
- **LCL (Less Container Load)**: Volume-based pricing with minimum charges
- **China Express**: Province-based domestic express delivery audit
- **Cross-Border**: International shipment validation for China import/export
- **Domestic**: China domestic shipment validation across all provinces

### Audit Intelligence
- **Undercharge Logic**: Automatically passes invoices with favorable pricing
- **Variance Thresholds**: Configurable tolerance levels for cost differences
- **Status Classification**: PASS/WARNING/FAIL with detailed explanations
- **Rate Card Scoring**: Best match selection based on route accuracy
- **AI-Enhanced Extraction**: LLM-powered PDF data extraction with confidence scoring
- **Batch Processing**: Automated multi-invoice audit workflows

## 🏗️ Architecture

### Technology Stack
- **Backend**: Python 3.12, Flask
- **Database**: SQLite with SQLAlchemy ORM  
- **Frontend**: HTML5, Bootstrap, Jinja2 templates
- **Data Processing**: Pandas, NumPy for analysis
- **AI/ML**: Ollama API with DeepSeek-R1 for PDF processing
- **Authentication**: PBKDF2 password hashing, session management

### Key Components
```
app/
├── models.py                         # Database models (invoices, rate cards, charges)
├── edi_parser.py                     # EDI X12 format parsing engine
├── database.py                       # Database connection and CRUD operations  
├── auth_routes.py                    # Authentication system
├── routes/                           # Flask route handlers
├── dhl_express_audit_engine.py       # DHL Express zone-based audit engine
├── au_domestic_audit_engine.py       # AU domestic audit system
├── llm_enhanced_pdf_processor_new.py # AI-powered PDF processing
├── ytd_batch_audit_system.py         # Batch processing system
└── enhanced_invoice_auditor.py       # Advanced audit logic

templates/
├── dhl_express_invoices.html         # Sortable invoice interface
├── dhl_express_batch_results.html    # Batch audit results
├── auth/                             # Login/signup templates
└── ...                               # Additional web interfaces

uploads/                              # File upload storage
```

## 📊 Audit Process

### 1. Invoice Processing
- Parse EDI files, CSV imports, or PDF documents to extract invoice details
- AI-enhanced PDF extraction using local LLM (DeepSeek-R1 via Ollama)
- Normalize charge categories and currency conversion
- Extract origin/destination, weight, volume, container info
- Automatic invoice type detection (Express, Domestic, International)

### 2. Smart Rate Card Selection
- **DHL Express Detection**: Automatic routing to appropriate rate card system
- **AU Domestic Logic**: Melbourne→Sydney, Brisbane→Perth with 11-zone coverage
- **Zone Mapping**: 9-zone worldwide structure for Export/Import
- **3rd Party Rates**: Non-AU origin/destination combinations
- **Exact Matching**: Direct city and port name matches
- **Fuzzy Matching**: Partial city matching using `cities_included_origin/destination`
- **Country Fallback**: Country-level matching when city data unavailable
- **Route Scoring**: Prioritize rate cards with better geographic alignment

### 3. Advanced Cost Analysis
- **DHL Express**: Zone-based rates with weight tiers and adder calculations (>30kg)
- **AU Domestic**: 11-zone domestic matrix with precise city-to-zone mapping
- **Air Freight**: Per-kg rates with weight tiers, handling fees, fuel surcharges
- **Ocean FCL**: Per-container rates with pickup, handling, freight, bunker, delivery
- **Service Charges**: Premium services validation (Saturday delivery, remote area, etc.)
- **Variance Calculation**: Expected vs actual with percentage analysis
- **Status Determination**: Business rules for pass/fail criteria with tolerance thresholds

### 4. Undercharge Logic & Batch Processing
```python
if total_variance < 0:  # Customer paid less than rate card
    audit_status = 'PASS'
    reason = 'Undercharge - favorable pricing'
```

**Batch Processing Features:**
- Multi-invoice audit workflows
- Sortable result tables with filtering
- Export capabilities for audit reports
- Real-time progress tracking
- Confidence scoring for AI extractions

## 🛠️ Installation

### Prerequisites
- Python 3.12+
- SQLite3
- Ollama (for LLM-enhanced PDF processing)
- DeepSeek-R1 model (via Ollama)

### Setup
```bash
# Clone the repository
git clone https://github.com/yourusername/dhl-audit-app.git
cd dhl-audit-app

# Install dependencies
pip install -r requirements.txt

# Install Ollama (for PDF processing)
# Visit https://ollama.ai for installation instructions
ollama pull deepseek-r1:latest

# Initialize database
python app.py

# Run application
python app.py
```

### Configuration
- SQLite database (`dhl_audit.db`) automatically created on first run
- Corporate email domain (@andrew.com) enforced for authentication
- Ollama API endpoint: http://localhost:11434 (configurable)

## 💡 Usage

### Authentication
1. **Sign Up**: Navigate to `http://localhost:5000/signup` with corporate email (@andrew.com)
2. **Login**: Use corporate credentials at `http://localhost:5000/login`
3. **Session Management**: 24-hour session timeout with automatic renewal

### Web Interface
1. **Dashboard**: Navigate to `http://localhost:5000` for main overview
2. **DHL Express**: Access `http://localhost:5000/dhl-express` for Express audit system
3. **Upload Files**: 
   - EDI/CSV invoices via upload interface
   - PDF documents for AI extraction
   - Excel rate cards for validation rules
4. **Run Audits**: 
   - Individual invoice audits
   - Batch processing for multiple invoices
   - Real-time sorting and filtering of results
5. **View Results**: Comprehensive variance reports with downloadable Excel exports

### API Endpoints
```python
# DHL Express audit endpoints
GET /dhl-express/invoices                    # Sortable invoice list
GET /dhl-express/invoice/{invoice_no}        # Individual invoice details
GET /dhl-express/audit/{invoice_no}          # Run audit on specific invoice
GET /dhl-express/batch-audit/results         # Batch audit results

# YTD audit system
GET /improved_ytd_audit/audit/{invoice_no}   # Legacy audit system
GET /improved_ytd_audit/api/audit/{invoice_no} # API response

# LLM PDF processing
POST /llm-pdf/upload                         # Upload PDF for AI extraction
GET /llm-pdf/history                         # Processing history

# Authentication
POST /signup                                 # User registration
POST /login                                  # User authentication
POST /logout                                 # Session termination
```

### Sample Audit Results

#### DHL Express Zone-Based Audit
```json
{
  "invoice_no": "MELR001510911", 
  "service_type": "DHL Express",
  "audit_results": [
    {
      "charge_type": "EXPRESS WORLDWIDE nondoc",
      "origin": "Melbourne (Zone 1)",
      "destination": "Sydney (Zone 3)", 
      "weight": "1.5kg",
      "expected": 16.47,
      "actual": 18.00,
      "variance": 1.53,
      "variance_percent": "9.3%",
      "status": "REVIEW"
    }
  ],
  "total_variance": 1.53,
  "audit_status": "REVIEW",
  "confidence": 0.95
}
```

#### AU Domestic Audit
```json
{
  "invoice_no": "AU2024001",
  "transportation_mode": "AU Domestic",
  "route": "Melbourne → Sydney",
  "zone_mapping": "Zone 1 → Zone 3 = Rate Zone B", 
  "audit_results": [
    {
      "charge_type": "Domestic Express",
      "weight": "1.5kg",
      "rate_zone": "B",
      "expected": 16.47,
      "actual": 16.47,
      "variance": 0.00,
      "status": "PASS"
    }
  ],
  "audit_status": "PASS"
}
```

## � Audit Examples

### DHL Express Zone-Based Audit
```
Invoice: MELR001510911 - 1.5kg Melbourne → Sydney
Zone Lookup: Melbourne (Zone 1) → Sydney (Zone 3)
Rate Type: Export Non-documents
Expected: $16.47 (Zone 3 rate for 1.5kg)
Actual: $18.00  
Variance: +$1.53 (9.3% overcharge)
Status: REVIEW - Within tolerance but overcharged
```

### AU Domestic Comprehensive Audit
```
Invoice: AU domestic 1.5kg Melbourne → Sydney  
Zone Mapping: Zone 1 → Zone 3 = Rate Zone B
Rate Lookup: 1.5kg in Zone B = $16.47
Expected: $16.47
Actual: $16.47
Variance: $0.00 (0% variance)
Status: PASS - Exact match
```

### Air Freight Audit
```
Invoice: 500kg Shanghai → Sydney
Expected: $2,850.00 (500kg × $5.70/kg)
Actual: $2,645.00
Variance: -$205.00 (7.2% undercharge)
Status: PASS - Favorable pricing
```

### Ocean FCL Audit  
```
Invoice: 2×40ft containers Shanghai → Sydney
Pickup: $410.71 vs $547.04 expected (-$136.33)
Freight: $778.82 vs $3,178.00 expected (-$2,399.18) 
Total: $8,101.49 vs $8,030.90 expected (+$70.59)
Status: PASS - Within tolerance
```

## �📋 Data Models

### DHL Express System
- **Zone Mapping**: 9-zone worldwide structure with origin/destination mapping
- **Rate Cards**: Export, Import, 3rd Party with weight-based tiers
- **AU Domestic**: 11-zone Australian domestic system with rate matrix
- **Service Charges**: Premium services with code-based validation

### Invoice Structure
- **Basic Info**: Invoice number, date, transportation mode, service type
- **Route Details**: Origin/destination cities, ports, countries, zones
- **Shipment Info**: Weight, volume, container counts, AWB numbers
- **Charges**: Detailed breakdown by category (freight, handling, fuel, etc.)
- **AI Metadata**: Extraction confidence, processing timestamps, model versions

### Rate Card Structure
- **DHL Express Zones**: 9-zone matrix with country code mappings
- **AU Domestic Zones**: 11-zone domestic matrix (MEL=1, BNE=2, SYD=3, etc.)
- **Weight Tiers**: Base rates up to 30kg, adder rates for heavier shipments
- **Service Types**: Export, Import, 3rd Party with separate rate tables
- **Air Rates**: Weight-based tiers with origin/destination fees
- **Ocean FCL**: Container-based rates with comprehensive charge breakdown
- **Ocean LCL**: Volume-based with minimum charges
- **Geographic Data**: Cities included, port mappings, country codes

## 🧪 Testing

### Test Data
- Sample EDI files in `sample_dhl_invoices.edi`
- DHL Express test invoices with zone-based validation
- AU domestic test cases (Melbourne→Sydney example: MELR001510911)
- Test rate cards with various pricing structures
- PDF invoice samples for LLM extraction testing
- Edge cases: missing data, currency conversion, special routes

### Validation
- Cross-reference with manual calculations
- DHL Express zone mapping accuracy verification
- AU domestic 11-zone system validation
- Verify fuzzy matching accuracy
- Test undercharge/overcharge logic
- LLM extraction confidence scoring
- Batch processing performance testing

## 🐛 Troubleshooting

### Common Issues
1. **Missing Rate Cards**: Ensure rate cards uploaded for relevant routes
2. **DHL Express Zone Issues**: Verify zone mappings in `dhl_express_zone_mapping` table
3. **AU Domestic Mapping**: Check city/state recognition in address parsing
4. **City Matching**: Check `cities_included_origin/destination` fields
5. **Currency Issues**: Verify USD conversion rates
6. **Container Mismatch**: Confirm FCL container types (20ft/40ft)
7. **LLM Connection**: Ensure Ollama is running and DeepSeek-R1 model is available
8. **Authentication**: Corporate email domain (@andrew.com) required for access

### Debug Features
- Detailed variance breakdown with zone information
- Rate card matching scores and route analysis
- Geographic matching analysis with fallback logic
- Step-by-step audit trail with decision points
- LLM extraction confidence scores and raw text output
- Batch processing progress monitoring

## 🔮 Future Enhancements

### Planned Features
- [ ] Multi-currency support beyond USD
- [ ] Advanced reporting dashboard with charts and analytics
- [ ] Email notifications for audit exceptions  
- [ ] Enhanced batch processing with parallel execution
- [ ] Machine learning for improved rate matching accuracy
- [ ] Integration with ERP systems (SAP, Oracle)
- [ ] Mobile-responsive interface with touch optimization
- [ ] Real-time audit notifications and alerts
- [ ] Advanced filtering and search capabilities
- [ ] API rate limiting and authentication tokens

### DHL Express Improvements
- [ ] Additional zone mappings for emerging markets
- [ ] Time-based rate validity checking
- [ ] Seasonal surcharge handling and calendar integration
- [ ] Port-specific fee structures and local charges
- [ ] Fuel index integration with real-time updates
- [ ] Service charge rule engine expansion

### AU Domestic Enhancements
- [ ] Regional zone subdivision for more precise pricing
- [ ] Australia Post integration for comparative analysis
- [ ] Suburb-level zone mapping for metropolitan areas
- [ ] Dynamic zone boundary updates based on postal codes

### LLM & AI Features
- [ ] Support for multiple LLM providers (OpenAI, Anthropic, etc.)
- [ ] Advanced document classification and routing
- [ ] Multi-language invoice processing
- [ ] OCR enhancement for poor quality scans
- [ ] Automated rate card updates via document parsing

## 🤝 Contributing

### Development Guidelines
- Follow PEP 8 Python style guidelines
- Include comprehensive error handling with specific exception types
- Add type hints for better code clarity and IDE support
- Write unit tests for new features (pytest framework)
- Document complex business logic with inline comments
- Use proper logging instead of print statements
- Maintain backward compatibility for API endpoints

### DHL Express Development Notes
- Zone mappings stored in `dhl_express_zone_mapping` table
- Rate calculations support base rates + adder logic for >30kg
- AU domestic uses separate 11-zone system with matrix lookup
- Service charges validated against `dhl_express_service_charges` table

### LLM Integration Guidelines
- Validate Ollama connection before processing
- Implement fallback mechanisms for LLM failures
- Store raw extraction data for manual review
- Include confidence scoring for all AI extractions
- Test with various PDF formats and layouts

### EDI Processing Notes
- EDI segments delimited by `~`
- Elements separated by `*`
- Handle multiple transaction sets per file
- Validate data integrity before processing
- Support both X12 210 and 214 transaction types

## 📞 Support

For technical support or business questions:
- Create an issue in this repository with detailed information
- Include sample data and error logs for faster resolution
- Specify invoice numbers for audit-related questions
- For LLM issues, include Ollama version and model information
- For authentication problems, verify corporate email domain
- Attach screenshots for UI-related issues

### System Requirements
- **Minimum**: Python 3.12, 8GB RAM, 50GB disk space
- **Recommended**: Python 3.12, 16GB RAM, 100GB disk space, SSD storage
- **Ollama**: 4GB VRAM recommended for DeepSeek-R1 model
- **Network**: Internet connection required for model downloads

### Performance Optimization
- Regular database maintenance and indexing
- LLM model caching for improved response times
- Batch processing optimization for large invoice volumes
- Rate card caching for frequently accessed routes

## 🔐 Authentication & Access Policy

### Secure Corporate Access
- Only corporate users are permitted to access the DHL Invoice Audit Application.
- **Sign Up / Sign In:** Users must use their corporate email address to register and log in.
- The login and signup pages do not display the required domain for security reasons. Users are simply reminded to use their corporate email address.

### Backend Domain Enforcement
- The backend strictly enforces that only email addresses ending with `@andrew.com` are accepted for account creation and login.
- Any attempt to sign up or log in with a non-corporate email will be rejected with a generic error message.

### Security Features
- Strong password requirements (length, complexity, special characters)
- Encrypted password storage (PBKDF2 with salt)
- Session management with 24-hour expiration
- Account lockout after repeated failed attempts
- All application routes are protected and require authentication

### User Guidance
- Always use your corporate email address to sign up or sign in.
- If you encounter access issues, contact your administrator.

## 📄 License

This project is proprietary software for internal use.

## 🏆 Acknowledgments

Built for efficient DHL invoice processing and cost optimization through automated audit workflows.

### Key Achievements
- **Complete AU Domestic System**: 11-zone coverage with 100% accuracy for Melbourne→Sydney validation
- **Advanced Zone Logic**: 9-zone worldwide DHL Express system with comprehensive rate card support  
- **AI-Enhanced Processing**: Local LLM integration for intelligent PDF data extraction
- **Production Ready**: Robust authentication, batch processing, and sortable interfaces
- **Comprehensive Testing**: Validated against real-world invoice scenarios and edge cases

### Technology Integration
- **Ollama/DeepSeek-R1**: Advanced AI document processing capabilities
- **Flask Architecture**: Scalable web application with modular design
- **SQLite Optimization**: Efficient database design with proper indexing
- **Bootstrap UI**: Responsive, professional interface with interactive features

---
