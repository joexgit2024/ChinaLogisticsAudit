#!/usr/bin/env python3
"""
Model Management Routes for Ollama LLM Models
Provides API endpoints for managing model loading and status
"""

from flask import Blueprint, jsonify, request
from model_manager import ModelManager
import json

model_mgmt_bp = Blueprint('model_mgmt', __name__)
model_manager = ModelManager()

@model_mgmt_bp.route('/api/models/status')
def get_models_status():
    """Get status of all available models"""
    try:
        status = model_manager.get_system_status()
        
        # Add detailed model information
        detailed_models = {}
        for model_name in status['available_models']:
            model_status = model_manager.get_model_status(model_name)
            detailed_models[model_name] = model_status
        
        return jsonify({
            'success': True,
            'system_status': status,
            'models': detailed_models
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@model_mgmt_bp.route('/api/models/preload', methods=['POST'])
def preload_model():
    """Pre-load a specific model"""
    try:
        data = request.get_json()
        model_name = data.get('model_name')
        
        if not model_name:
            return jsonify({'success': False, 'error': 'model_name required'}), 400
        
        # Check if model is available
        available_models = model_manager.get_available_models()
        if model_name not in available_models:
            return jsonify({
                'success': False, 
                'error': f'Model {model_name} not found. Available: {available_models}'
            }), 404
        
        # Pre-load the model
        success, message = model_manager.ensure_model_ready(model_name)
        
        return jsonify({
            'success': success,
            'message': message,
            'model_name': model_name,
            'is_loaded': model_manager.is_model_loaded(model_name)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@model_mgmt_bp.route('/api/models/unload', methods=['POST'])
def unload_model():
    """Unload a specific model"""
    try:
        data = request.get_json()
        model_name = data.get('model_name')
        
        if not model_name:
            return jsonify({'success': False, 'error': 'model_name required'}), 400
        
        success = model_manager.unload_model(model_name)
        
        return jsonify({
            'success': success,
            'message': f'Model {model_name} {"unloaded" if success else "failed to unload"}',
            'model_name': model_name
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@model_mgmt_bp.route('/api/models/loaded')
def get_loaded_models():
    """Get currently loaded models"""
    try:
        loaded_models = model_manager.get_loaded_models()
        return jsonify({
            'success': True,
            'loaded_models': loaded_models,
            'count': len(loaded_models)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@model_mgmt_bp.route('/api/models/available')
def get_available_models():
    """Get available models that can be loaded"""
    try:
        available_models = model_manager.get_available_models()
        
        # Add model configurations
        models_with_config = {}
        for model_name in available_models:
            config = model_manager.model_configs.get(model_name, {})
            models_with_config[model_name] = {
                'name': config.get('name', model_name),
                'size': config.get('size', 'Unknown'),
                'speed': config.get('speed', 'Unknown'),
                'accuracy': config.get('accuracy', 'Unknown'),
                'best_for': config.get('best_for', 'General use'),
                'is_loaded': model_manager.is_model_loaded(model_name)
            }
        
        return jsonify({
            'success': True,
            'models': models_with_config,
            'count': len(available_models)
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@model_mgmt_bp.route('/api/models/<model_name>/status')
def get_model_status(model_name):
    """Get status of a specific model"""
    try:
        status = model_manager.get_model_status(model_name)
        return jsonify({
            'success': True,
            'model_status': status
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500

@model_mgmt_bp.route('/api/ollama/status')
def get_ollama_status():
    """Check if Ollama service is running"""
    try:
        is_running = model_manager.check_ollama_status()
        return jsonify({
            'success': True,
            'ollama_running': is_running,
            'message': 'Ollama is running' if is_running else 'Ollama is not running'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500
