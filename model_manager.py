#!/usr/bin/env python3
"""
Model Manager for Ollama LLM Models
Handles automatic loading, warming up, and managing LLM models
"""

import requests
import time
import subprocess
import json
from typing import Dict, List, Optional, Tuple

class ModelManager:
    """
    Manages Ollama model loading and availability
    """
    
    def __init__(self, ollama_url: str = "http://localhost:11434"):
        self.ollama_url = ollama_url
        self.default_keep_alive = "30m"  # Keep models loaded for 30 minutes
        
        # Model configurations with loading times and characteristics
        self.model_configs = {
            'deepseek-r1:latest': {
                'name': 'DeepSeek-R1',
                'size': '4.7 GB',
                'speed': 'Slow',
                'accuracy': 'Excellent',
                'expected_load_time': 45,  # seconds
                'warmup_prompt': 'Extract invoice number from: Invoice #12345',
                'keep_alive': '30m'
            },
            'deepseek-r1:1.5b': {
                'name': 'DeepSeek-R1 1.5B',
                'size': '1.1 GB',
                'speed': 'Very Fast',
                'accuracy': 'Good',
                'expected_load_time': 15,
                'warmup_prompt': 'Extract invoice number from: Invoice #12345',
                'keep_alive': '20m'
            },
            'llama3.2:latest': {
                'name': 'Llama 3.2',
                'size': '2.0 GB',
                'speed': 'Fast',
                'accuracy': 'Good',
                'expected_load_time': 20,
                'warmup_prompt': 'Hello, extract: Invoice #12345',
                'keep_alive': '15m'
            },
            'mistral:latest': {
                'name': 'Mistral',
                'size': '4.1 GB',
                'speed': 'Medium',
                'accuracy': 'Very Good',
                'expected_load_time': 30,
                'warmup_prompt': 'Extract data from: Invoice #12345',
                'keep_alive': '25m'
            },
            'qwen2.5-coder:1.5b': {
                'name': 'Qwen2.5 Coder 1.5B',
                'size': '986 MB',
                'speed': 'Very Fast',
                'accuracy': 'Good',
                'expected_load_time': 10,
                'warmup_prompt': 'Parse JSON: {"invoice": "12345"}',
                'keep_alive': '15m'
            }
        }
    
    def check_ollama_status(self) -> bool:
        """Check if Ollama service is running"""
        try:
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=5)
            return response.status_code == 200
        except:
            return False
    
    def get_loaded_models(self) -> List[Dict]:
        """Get currently loaded models in Ollama"""
        try:
            result = subprocess.run(['ollama', 'ps'], 
                                  capture_output=True, text=True, timeout=10)
            
            if result.returncode != 0:
                return []
            
            lines = result.stdout.strip().split('\n')
            if len(lines) < 2:  # No models loaded
                return []
            
            loaded_models = []
            for line in lines[1:]:  # Skip header
                parts = line.split()
                if len(parts) >= 4:
                    loaded_models.append({
                        'name': parts[0],
                        'id': parts[1],
                        'size': parts[2],
                        'processor': parts[3],
                        'until': ' '.join(parts[4:]) if len(parts) > 4 else 'Unknown'
                    })
            
            return loaded_models
            
        except Exception as e:
            print(f"Error getting loaded models: {e}")
            return []
    
    def is_model_loaded(self, model_name: str) -> bool:
        """Check if a specific model is currently loaded"""
        loaded_models = self.get_loaded_models()
        return any(model['name'] == model_name for model in loaded_models)
    
    def get_available_models(self) -> List[str]:
        """Get list of available models that can be loaded"""
        try:
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=10)
            if response.status_code == 200:
                data = response.json()
                return [model['name'] for model in data.get('models', [])]
            return []
        except:
            return []
    
    def preload_model(self, model_name: str, timeout: int = 120) -> Tuple[bool, str]:
        """
        Pre-load a model with warm-up to ensure it's ready for processing
        Returns (success, message)
        """
        print(f"🔄 Pre-loading model: {model_name}")
        
        # Check if model is already loaded
        if self.is_model_loaded(model_name):
            print(f"✅ Model {model_name} is already loaded")
            return True, f"Model {model_name} already loaded"
        
        # Get model config
        config = self.model_configs.get(model_name, {})
        expected_time = config.get('expected_load_time', 60)
        warmup_prompt = config.get('warmup_prompt', 'Hello')
        keep_alive = config.get('keep_alive', self.default_keep_alive)
        
        print(f"⏱️  Expected loading time: {expected_time} seconds")
        print(f"🔥 Warming up with prompt: '{warmup_prompt}'")
        
        try:
            start_time = time.time()
            
            # Use ollama run command to load and warm up the model
            # First load the model, then set keep-alive separately
            cmd = ['ollama', 'run', model_name, warmup_prompt]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            
            load_time = time.time() - start_time
            
            if result.returncode == 0:
                print(f"✅ Model {model_name} loaded successfully in {load_time:.1f}s")
                print(f"🔥 Model warmed up and ready for processing")
                return True, f"Model loaded in {load_time:.1f}s"
            else:
                error_msg = result.stderr or "Unknown error"
                print(f"❌ Failed to load model: {error_msg}")
                return False, f"Failed to load model: {error_msg}"
                
        except subprocess.TimeoutExpired:
            print(f"⏰ Model loading timed out after {timeout} seconds")
            return False, f"Model loading timed out after {timeout}s"
        except Exception as e:
            print(f"❌ Error loading model: {e}")
            return False, f"Error loading model: {str(e)}"
    
    def ensure_model_ready(self, model_name: str) -> Tuple[bool, str]:
        """
        Ensure a model is loaded and ready for processing
        This is the main function to call before processing
        """
        print(f"\n🚀 Ensuring model {model_name} is ready...")
        
        # Check Ollama status first
        if not self.check_ollama_status():
            return False, "Ollama service is not running"
        
        # Check if model is available
        available_models = self.get_available_models()
        if model_name not in available_models:
            return False, f"Model {model_name} is not installed"
        
        # Pre-load the model if not already loaded
        if not self.is_model_loaded(model_name):
            success, message = self.preload_model(model_name)
            if not success:
                return False, message
        
        # Final verification
        if self.is_model_loaded(model_name):
            print(f"✅ Model {model_name} is ready for processing")
            return True, f"Model {model_name} ready"
        else:
            return False, "Model failed to load properly"
    
    def get_model_status(self, model_name: str) -> Dict:
        """Get comprehensive status information for a model"""
        return {
            'name': model_name,
            'is_loaded': self.is_model_loaded(model_name),
            'is_available': model_name in self.get_available_models(),
            'config': self.model_configs.get(model_name, {}),
            'ollama_running': self.check_ollama_status()
        }
    
    def unload_model(self, model_name: str) -> bool:
        """Unload a specific model to free up memory"""
        try:
            # Set keep-alive to 0 to unload immediately
            cmd = ['ollama', 'run', model_name, '--keep-alive', '0', 'bye']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            return result.returncode == 0
        except:
            return False
    
    def get_system_status(self) -> Dict:
        """Get overall system status"""
        loaded_models = self.get_loaded_models()
        available_models = self.get_available_models()
        
        return {
            'ollama_running': self.check_ollama_status(),
            'loaded_models': loaded_models,
            'available_models': available_models,
            'total_loaded': len(loaded_models),
            'total_available': len(available_models)
        }

# Utility functions for easy access
def ensure_model_ready(model_name: str) -> Tuple[bool, str]:
    """Quick function to ensure a model is ready for use"""
    manager = ModelManager()
    return manager.ensure_model_ready(model_name)

def preload_model(model_name: str) -> Tuple[bool, str]:
    """Quick function to pre-load a model"""
    manager = ModelManager()
    return manager.preload_model(model_name)

# Example usage
if __name__ == "__main__":
    manager = ModelManager()
    
    print("=== MODEL MANAGER TEST ===")
    print(f"Ollama Status: {manager.check_ollama_status()}")
    print(f"Available Models: {manager.get_available_models()}")
    print(f"Loaded Models: {manager.get_loaded_models()}")
    
    # Test pre-loading DeepSeek-R1
    print("\n=== TESTING DEEPSEEK-R1 PRE-LOADING ===")
    success, message = manager.ensure_model_ready('deepseek-r1:latest')
    print(f"Result: {success} - {message}")
