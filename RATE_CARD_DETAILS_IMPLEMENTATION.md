# DHL Express Rate Card Details Viewer Implementation

## Overview

This feature allows users to view detailed information about DHL Express rate cards directly from the rate card management interface. It includes:

1. A new API endpoint to fetch rate card details by service type and rate section
2. An interactive modal dialog to display the rate card data in a formatted table
3. Zone mapping information to help understand rate card structure
4. Export functionality to download rate card data as CSV

## Files Modified

- **dhl_express_routes.py**: Added a new API endpoint `/dhl-express/rate-card-details` to fetch detailed rate card information
- **templates/dhl_express_rate_cards.html**: Added modal dialog and JavaScript functionality to display rate card details

## Technical Details

### API Endpoint

The new endpoint `/dhl-express/rate-card-details` accepts two query parameters:
- `service_type`: The type of service (Export, Import, etc.)
- `rate_section`: The rate section (Documents, Non-documents)

It returns a JSON response containing:
- Basic rate card information (service_type, rate_section)
- List of rate entries with weight breaks and rates for each zone
- Zone mapping information to help understand which countries are in each zone

### User Interface

The UI includes:
- Eye icon buttons next to each rate card entry to view details
- Modal dialog showing rate data in a formatted table
- Visual highlighting for rate types (standard rates vs. multipliers)
- Zone mapping cards showing country pairs for each zone
- Download button to export rate card data as CSV

### Security

- The endpoint is protected with the same authentication middleware as the main rate card page
- Input validation ensures required parameters are provided
- Error handling for database queries and API responses

## How to Use

1. Navigate to the DHL Express Rate Cards page
2. Click the eye icon (👁️) button next to any rate card entry
3. View the detailed information in the modal dialog
4. Optionally download the data as CSV using the "Download as CSV" button

## Testing

A test script `test_rate_card_details_api.py` is provided to verify the API functionality.

To test the implementation:
1. Start the Flask application
2. Run the test script: `python test_rate_card_details_api.py`
3. Manually test by visiting the rate cards page and clicking the eye icons
