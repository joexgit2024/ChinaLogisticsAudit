#!/usr/bin/env python3
"""
Retrain ML Models Script
========================

This script retrains the ML models with the current scikit-learn version
to eliminate version compatibility warnings.
"""

import os
import pickle
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from advanced_pdf_processor import AdvancedPDFProcessor

def retrain_models():
    """Retrain ML models with current scikit-learn version"""
    print("Retraining ML models with current scikit-learn version...")
    
    # Remove old model files
    models_path = 'ml_models'
    vectorizer_path = os.path.join(models_path, 'tfidf_vectorizer.pkl')
    classifier_path = os.path.join(models_path, 'charge_classifier.pkl')
    
    if os.path.exists(vectorizer_path):
        os.remove(vectorizer_path)
        print("Removed old vectorizer model")
    
    if os.path.exists(classifier_path):
        os.remove(classifier_path)
        print("Removed old classifier model")
    
    # Create new processor instance (this will create new models)
    processor = AdvancedPDFProcessor()
    
    print("Successfully retrained ML models with current scikit-learn version!")
    print("The version warnings should no longer appear on startup.")

if __name__ == "__main__":
    retrain_models()
