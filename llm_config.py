#!/usr/bin/env python3
"""
LLM Configuration for PDF Processing
"""

# LLM Configuration Dictionary
LLM_CONFIG = {
    # Model Configuration
    "model": "deepseek-r1:latest",
    "base_url": "http://localhost:11434",
    
    # GPU Optimization Settings
    "options": {
        "temperature": 0.1,
        "top_p": 0.9,
        "num_predict": 4096,
        "num_ctx": 8192,        # Context window size
        "num_batch": 512,       # Batch size for GPU processing
        "num_gpu": 1,           # Number of GPUs to use
        "gpu_layers": -1,       # Use all GPU layers (-1 = all)
        "num_thread": 8,        # CPU threads when not using GPU
    },
    
    # Request Settings
    "timeout": 120,             # Timeout in seconds
    "stream": False,            # Don't stream responses
}

def get_llm_config():
    """Get the LLM configuration dictionary"""
    return LLM_CONFIG

def get_model_name():
    """Get the configured model name"""
    return LLM_CONFIG["model"]

def get_ollama_url():
    """Get the Ollama service URL"""
    return LLM_CONFIG["base_url"]

# Validation function
def validate_config():
    """Validate the LLM configuration"""
    required_keys = ["model", "base_url", "options"]
    for key in required_keys:
        if key not in LLM_CONFIG:
            raise ValueError(f"Missing required configuration key: {key}")
    
    if not LLM_CONFIG["model"]:
        raise ValueError("Model name cannot be empty")
    
    if not LLM_CONFIG["base_url"]:
        raise ValueError("Base URL cannot be empty")
    
    return True

# Test function
if __name__ == "__main__":
    try:
        validate_config()
        print("✅ LLM Configuration is valid")
        print(f"Model: {get_model_name()}")
        print(f"URL: {get_ollama_url()}")
        print(f"GPU Layers: {LLM_CONFIG['options']['gpu_layers']}")
        print(f"Context Size: {LLM_CONFIG['options']['num_ctx']}")
    except Exception as e:
        print(f"❌ Configuration error: {e}")
