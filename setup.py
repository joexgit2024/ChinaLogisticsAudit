#!/usr/bin/env python3
"""
DHL Audit App Setup Script
==========================

This script helps new developers set up the DHL Invoice Audit Application
"""

import os
import sys
import subprocess
import sqlite3

def run_command(command, description):
    """Run a command and handle errors"""
    print(f"🔄 {description}...")
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ {description} completed successfully")
            return True
        else:
            print(f"❌ Error in {description}: {result.stderr}")
            return False
    except Exception as e:
        print(f"❌ Error in {description}: {str(e)}")
        return False

def check_python_version():
    """Check Python version"""
    print("🔍 Checking Python version...")
    version = sys.version_info
    if version.major == 3 and version.minor >= 12:
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} is supported")
        return True
    else:
        print(f"❌ Python {version.major}.{version.minor}.{version.micro} is not supported. Please use Python 3.12+")
        return False

def install_dependencies():
    """Install Python dependencies"""
    if os.path.exists('requirements.txt'):
        return run_command('pip install -r requirements.txt', 'Installing dependencies')
    else:
        print("❌ requirements.txt not found")
        return False

def setup_database():
    """Initialize SQLite database"""
    print("🔄 Setting up database...")
    try:
        # Check if database exists
        if not os.path.exists('dhl_audit.db'):
            print("📊 Creating new database...")
            conn = sqlite3.connect('dhl_audit.db')
            conn.close()
            print("✅ Database created successfully")
        else:
            print("✅ Database already exists")
        return True
    except Exception as e:
        print(f"❌ Database setup error: {str(e)}")
        return False

def create_uploads_directory():
    """Create uploads directory if it doesn't exist"""
    print("🔄 Setting up uploads directory...")
    try:
        if not os.path.exists('uploads'):
            os.makedirs('uploads')
            print("✅ Uploads directory created")
        else:
            print("✅ Uploads directory already exists")
        return True
    except Exception as e:
        print(f"❌ Error creating uploads directory: {str(e)}")
        return False

def test_application():
    """Test if the application can start"""
    print("🔄 Testing application startup...")
    try:
        # Try to import the main app
        import app
        print("✅ Application imports successfully")
        return True
    except ImportError as e:
        print(f"❌ Import error: {str(e)}")
        return False
    except Exception as e:
        print(f"❌ Application test error: {str(e)}")
        return False

def main():
    """Main setup function"""
    print("🚀 DHL Invoice Audit Application Setup")
    print("=" * 50)
    
    success_count = 0
    total_checks = 6
    
    # Check Python version
    if check_python_version():
        success_count += 1
    
    # Install dependencies  
    if install_dependencies():
        success_count += 1
    
    # Setup database
    if setup_database():
        success_count += 1
    
    # Create uploads directory
    if create_uploads_directory():
        success_count += 1
    
    # Test application
    if test_application():
        success_count += 1
    
    # Final check
    if success_count == total_checks - 1:  # -1 because we haven't done final test yet
        print("\n🎉 Setup completed successfully!")
        print("\nNext steps:")
        print("1. Run 'python app.py' to start the application")
        print("2. Open http://localhost:5000 in your browser")
        print("3. Upload EDI files and rate cards to begin auditing")
        print("\n📚 Documentation:")
        print("- README.md: Project overview and features")
        print("- CONTRIBUTING.md: Development guidelines")
        print("- .github/copilot-instructions.md: AI coding assistant context")
        success_count += 1
    else:
        print(f"\n❌ Setup completed with {total_checks - success_count} errors")
        print("Please review the errors above and try again")
    
    print(f"\n📊 Setup Summary: {success_count}/{total_checks} checks passed")

if __name__ == "__main__":
    main()
