"""
China Logistics Audit Application - Main Flask App

Overall Purpose:
----------------
This file is the main entry point for the China Logistics Audit Application. It
configures the Flask app, sets up configuration, registers all blueprints for
modular route handling, and provides all web and API endpoints for invoice
auditing, uploads, dashboards, PDF/LLM processing, and more for Chinese logistics providers.

Where This File is Used:
------------------------
- Run directly (python app.py) to start the web server
- Referenced by setup scripts and deployment tools
- Used as the main entry for all web and API requests in the China logistics audit system
"""

import os
import sys
from flask import Flask, session
from app.database import init_database
from app.routes.rate_card_routes import rate_card_bp
from app.routes.audit_api import audit_api_bp
from app.routes.enhanced_audit_routes import audit_bp
from dhl_ytd_routes import dhl_ytd_bp
from ocean_rate_routes import ocean_rate_bp
from ytd_audit_engine import ytd_audit_bp
from updated_ytd_audit_engine import improved_ytd_audit_bp
from ytd_batch_audit_routes import ytd_batch_audit_bp
from advanced_pdf_routes import advanced_pdf_bp
from app.routes.core_routes import core_bp
from app.routes.invoice_routes import invoice_bp
from app.routes.download_routes import download_bp
from app.routes.validation_routes import validation_bp
from app.routes.api_routes import api_bp
from app.utils.template_filters import register_filters


app = Flask(__name__)
app.config['SECRET_KEY'] = 'your-secret-key-here'
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['DATABASE'] = 'china_logistics_audit.db'
app.config['TEMPLATES_AUTO_RELOAD'] = True
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0  # Disable cache


# Create upload directory if it doesn't exist
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)


# Register core application blueprints
app.register_blueprint(core_bp)
app.register_blueprint(invoice_bp)
app.register_blueprint(download_bp)
app.register_blueprint(validation_bp)
app.register_blueprint(api_bp)

# Register existing specialized blueprints
app.register_blueprint(rate_card_bp)
app.register_blueprint(audit_api_bp)
app.register_blueprint(audit_bp)
app.register_blueprint(dhl_ytd_bp)
app.register_blueprint(ocean_rate_bp)
app.register_blueprint(ytd_audit_bp)
app.register_blueprint(improved_ytd_audit_bp)
app.register_blueprint(ytd_batch_audit_bp)
app.register_blueprint(advanced_pdf_bp)

# Register LLM-enhanced PDF processing blueprint
try:
    sys.path.insert(0, '.')
    from llm_enhanced_routes import llm_pdf_bp
    app.register_blueprint(llm_pdf_bp)
    print(
        "LLM PDF processing routes registered successfully"
    )
except ImportError as e:
    print(f"LLM PDF processing routes not available: {e}")

# Register Model Management API blueprint
try:
    from model_management_routes import model_mgmt_bp
    app.register_blueprint(model_mgmt_bp)
    print("Model management API routes registered successfully")
except ImportError as e:
    print(f"Model management routes not available: {e}")

# Register PDF-YTD validation blueprint
try:
    from pdf_ytd_validation_routes import pdf_ytd_bp
    app.register_blueprint(pdf_ytd_bp)
    print("PDF-YTD validation routes registered successfully")
except ImportError as e:
    print(f"Warning: Could not import PDF-YTD routes: {e}")

# Register DHL Express blueprint
try:
    from dhl_express_routes import dhl_express_routes
    app.register_blueprint(dhl_express_routes)
except ImportError as e:
    print(f"Warning: Could not import DHL Express routes: {e}")

# Register Authentication blueprint
try:
    from auth_routes import auth_bp
    app.register_blueprint(auth_bp)
except ImportError as e:
    print(f"Warning: Could not import auth routes: {e}")

# Register YTD Reports blueprint
try:
    from ytd_reports_routes import ytd_reports_bp
    app.register_blueprint(ytd_reports_bp)
    print("YTD Reports routes registered successfully")
except ImportError as e:
    print(f"Warning: Could not import YTD Reports routes: {e}")

# Register Invoice Image blueprint
try:
    from invoice_image_routes import image_routes
    app.register_blueprint(image_routes)
    print("Invoice image routes registered successfully")
except ImportError as e:
    print(f"Warning: Could not import invoice image routes: {e}")

# Import auth decorator
try:
    from auth_routes import require_auth, require_auth_api
    from auth_database import AuthDatabase
    auth_db = AuthDatabase()
except ImportError:
    # Fallback if auth is not available
    def require_auth(f):
        return f

    def require_auth_api(f):
        return f
    auth_db = None


# Context processor to make user data available to all templates
@app.context_processor
def inject_user_data():
    """Inject user data into all templates"""
    if 'session_token' in session and auth_db:
        is_valid, user_data = auth_db.validate_session(
            session['session_token']
        )
        if is_valid:
            return {'user_data': user_data}
    return {'user_data': None}


# Register custom template filters
register_filters(app)


def init_db_command():
    """Initialize the database."""
    init_database()
    print('Database initialized successfully.')


@app.after_request
def add_header(response):
    """Add headers to prevent caching."""
    response.headers['Cache-Control'] = (
        'no-store, no-cache, must-revalidate, post-check=0, '
        'pre-check=0, max-age=0'
    )
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '-1'
    return response


if __name__ == '__main__':
    if len(sys.argv) > 1 and sys.argv[1] == 'init-db':
        init_db_command()
    else:
        # Initialize database if it doesn't exist
        if not os.path.exists(app.config['DATABASE']):
            init_database()
        
        app.run(debug=True, host='0.0.0.0', port=5000)
