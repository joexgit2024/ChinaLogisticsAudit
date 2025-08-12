"""
Template Filters for DHL Invoice Audit Application

Overall Purpose:
----------------
This module contains custom Jinja2 template filters used throughout the
application for data formatting and display in web templates.

Where This File is Used:
------------------------
- Imported and registered in the main Flask application (app.py)
- Used by Jinja2 templates for data formatting and JSON handling
- Provides consistent data presentation across all web pages
"""

import json


def tojsonpretty_filter(value):
    """Convert a Python object to pretty-printed JSON."""
    try:
        if isinstance(value, str):
            # If it's already a JSON string, parse it first
            value = json.loads(value)
        return json.dumps(
            value, indent=2, ensure_ascii=False
        )
    except (ValueError, TypeError):
        # If parsing fails, return the original value as a string
        return str(value)


def from_json_filter(value):
    """Parse a JSON string into a Python object."""
    try:
        if isinstance(value, str):
            return json.loads(value)
        return value
    except (ValueError, TypeError):
        # If parsing fails, return the original value
        return value


def register_filters(app):
    """Register all custom filters with the Flask app."""
    app.template_filter('tojsonpretty')(tojsonpretty_filter)
    app.template_filter('from_json')(from_json_filter)
