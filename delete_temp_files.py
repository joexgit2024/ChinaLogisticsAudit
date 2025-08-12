#!/usr/bin/env python3
"""
Delete Temporary Files Script
============================

This script safely deletes all Python files starting with "temp" in the current directory.
"""

import os
import glob

def delete_temp_files():
    """Delete all Python files starting with 'temp'"""
    
    # Find all temp*.py files in current directory
    temp_files = glob.glob("temp*.py")
    
    if not temp_files:
        print("No temporary Python files found starting with 'temp'")
        return
    
    print(f"Found {len(temp_files)} temporary Python files:")
    for file in temp_files:
        print(f"  - {file}")
    
    print("\nDeleting temporary files...")
    
    deleted_count = 0
    for file in temp_files:
        try:
            if os.path.exists(file):
                os.remove(file)
                print(f"✓ Deleted: {file}")
                deleted_count += 1
            else:
                print(f"✗ File not found: {file}")
        except Exception as e:
            print(f"✗ Error deleting {file}: {str(e)}")
    
    print(f"\nCompleted: {deleted_count} files deleted successfully")

if __name__ == "__main__":
    delete_temp_files()
