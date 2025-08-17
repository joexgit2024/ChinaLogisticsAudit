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


def _to_float(value, default=0.0):
    try:
        if value is None:
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def currency_filter(value, symbol='¥', decimals=2):
    """Format a number as currency with thousands separator.

    Usage in Jinja: {{ amount|currency('¥') }}
    """
    num = _to_float(value, 0.0)
    try:
        return f"{symbol}{num:,.{int(decimals)}f}"
    except Exception:
        # Fallback simple formatting
        return f"{symbol}{num:.2f}"


def number_format_filter(value, decimals=2, grouping=True):
    """Format a number with optional thousands separators.

    Usage: {{ value|number_format(2, True) }}
    """
    num = _to_float(value, 0.0)
    try:
        if grouping:
            return f"{num:,.{int(decimals)}f}"
        return f"{num:.{int(decimals)}f}"
    except Exception:
        return str(num)


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
    app.template_filter('currency')(currency_filter)
    app.template_filter('number_format')(number_format_filter)
