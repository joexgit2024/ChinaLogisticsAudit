#!/usr/bin/env python3
"""
API Route for Switching Default LLM Model
Enables switching between llama3.2:latest and other models
"""

from flask import Blueprint, jsonify, request, redirect, url_for
import sqlite3
from schema_driven_llm_processor import SchemaDrivenLLMProcessor

# Create blueprint
model_api_bp = Blueprint('model_api', __name__)

# Global processor instance
llm_processor = SchemaDrivenLLMProcessor(model_name="llama3.2:latest")

@model_api_bp.route('/api/switch-default-model', methods=['POST'])
def switch_default_model():
    """API endpoint to switch the default LLM model"""
    data = request.json
    if not data or 'model' not in data:
        return jsonify({'success': False, 'error': 'Missing model parameter'}), 400
    
    model_name = data['model']
    
    # Validate model name
    valid_models = ["llama3.2:latest", "llama3:8b", "deepseek-coder:latest"]
    if model_name not in valid_models:
        return jsonify({
            'success': False, 
            'error': f'Invalid model. Must be one of: {", ".join(valid_models)}'
        }), 400
    
    try:
        # Update global processor
        global llm_processor
        llm_processor = SchemaDrivenLLMProcessor(model_name=model_name)
        
        # Test model connection
        test_result = llm_processor.test_llm_connection()
        if not test_result.get('success'):
            return jsonify({
                'success': False,
                'error': f'Failed to connect to model: {test_result.get("error")}'
            }), 500
        
        # Update model preference in database
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Check if settings table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='app_settings'")
        if not cursor.fetchone():
            # Create settings table if it doesn't exist
            cursor.execute("""
                CREATE TABLE app_settings (
                    id INTEGER PRIMARY KEY,
                    setting_name TEXT UNIQUE,
                    setting_value TEXT,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
        
        # Update or insert default model setting
        cursor.execute("""
            INSERT INTO app_settings (setting_name, setting_value, updated_at)
            VALUES ('default_llm_model', ?, datetime('now'))
            ON CONFLICT(setting_name) 
            DO UPDATE SET setting_value=?, updated_at=datetime('now')
        """, (model_name, model_name))
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Default model switched to {model_name}',
            'model': model_name
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@model_api_bp.route('/api/get-default-model', methods=['GET'])
def get_default_model():
    """Get the current default LLM model"""
    try:
        # Get model from database
        conn = sqlite3.connect('dhl_audit.db')
        cursor = conn.cursor()
        
        # Check if settings table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='app_settings'")
        if not cursor.fetchone():
            # Return default if table doesn't exist
            return jsonify({
                'success': True,
                'model': 'llama3.2:latest',
                'is_default': True
            })
        
        # Get model from settings
        cursor.execute("""
            SELECT setting_value FROM app_settings 
            WHERE setting_name = 'default_llm_model'
        """)
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            model_name = result[0]
        else:
            model_name = 'llama3.2:latest'  # Default
        
        return jsonify({
            'success': True,
            'model': model_name,
            'is_default': model_name == 'llama3.2:latest'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
