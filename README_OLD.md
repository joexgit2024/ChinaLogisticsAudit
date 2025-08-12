# DHL Invoice Audit Application

A comprehensive Python Flask web application for auditing DHL shipping invoices by parsing EDI files and performing detailed cost analysis against rate cards.

## 🚀 Features

### Core Functionality
- **EDI File Processing**: Parse X12 EDI format files (210, 214 transaction sets)
- **Invoice Data Extraction**: Extract shipping details, charges, and metadata
- **Rate Card Matching**: Intelligent matching of invoices to negotiated rate cards
- **Detailed Audit Analysis**: Line-by-line charge comparison with variance analysis
- **Multi-Modal Support**: Air freight and Ocean freight (FCL/LCL) auditing
- **Fuzzy Matching**: Advanced city/port matching for rate card selection

### Transportation Modes
- **Air Freight**: Weight-based pricing with handling fees and surcharges
- **Ocean Freight**: Container-based pricing (20ft/40ft FCL) with detailed breakdown
- **LCL (Less Container Load)**: Volume-based pricing with minimum charges

### Audit Intelligence
- **Undercharge Logic**: Automatically passes invoices with favorable pricing
- **Variance Thresholds**: Configurable tolerance levels for cost differences
- **Status Classification**: PASS/WARNING/FAIL with detailed explanations
- **Rate Card Scoring**: Best match selection based on route accuracy
- **Export Functionality**: Export audit results to CSV/Excel

## Project Structure

```
DGFaudit/
├── app/
│   ├── __init__.py
│   ├── models.py          # Database models
│   ├── edi_parser.py      # EDI file parsing logic
│   ├── database.py        # Database operations
│   └── routes.py          # Flask routes
├── templates/
│   ├── base.html
│   ├── dashboard.html
│   └── upload.html
├── static/
│   ├── css/
│   └── js/
├── data/
│   └── test_edi_files/
├── requirements.txt
├── app.py                 # Main Flask application
└── README.md

```

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Initialize the database:
```bash
python app.py init-db
```

3. Run the application:
```bash
python app.py
```

## Usage

1. Access the web interface at `http://localhost:5000`
2. Upload EDI files through the dashboard
3. View parsed invoice data and audit results
4. Export reports as needed

## EDI Format Support

Currently supports X12 format with the following transaction sets:
- 210 (Motor Carrier Freight Details and Invoice)
- 214 (Transportation Carrier Shipment Status Message)

## Database Schema

The application stores the following data:
- Invoice header information
- Shipment details
- Charge breakdowns
- Audit flags and notes
