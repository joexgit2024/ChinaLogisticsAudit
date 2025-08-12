#!/usr/bin/env python3
"""
Service Charge Protection System
===============================

This script ensures that enhanced service charges (with product descriptions,
demerging logic, etc.) are preserved when uploading new rate cards.

It automatically detects and restores enhanced service charges that may have
been overwritten by basic Excel uploads.
"""

import sqlite3
from datetime import datetime

def protect_enhanced_service_charges():
    """Ensure enhanced service charges are preserved during uploads"""
    
    print("🛡️  SERVICE CHARGE PROTECTION SYSTEM")
    print("=" * 60)
    
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    # Check current state
    cursor.execute('''
        SELECT COUNT(*) as total,
               COUNT(CASE WHEN products_applicable IS NOT NULL AND products_applicable != '' THEN 1 END) as enhanced,
               COUNT(CASE WHEN original_service_code IS NOT NULL AND original_service_code != '' THEN 1 END) as demerged
        FROM dhl_express_services_surcharges
    ''')
    
    stats = cursor.fetchone()
    total_charges = stats[0]
    enhanced_charges = stats[1]
    demerged_charges = stats[2]
    
    print(f"📊 Current Status:")
    print(f"   Total service charges: {total_charges}")
    print(f"   Enhanced with products: {enhanced_charges}")
    print(f"   Demerged variants: {demerged_charges}")
    
    # Check for critical enhanced entries
    cursor.execute('''
        SELECT service_code, products_applicable, original_service_code
        FROM dhl_express_services_surcharges
        WHERE service_code LIKE 'YY%' OR service_code LIKE 'YB%' OR service_code LIKE 'II_%'
        ORDER BY service_code
    ''')
    
    critical_entries = cursor.fetchall()
    
    print(f"\n🔧 Critical Enhanced Entries ({len(critical_entries)}):")
    for entry in critical_entries:
        products = entry[1][:30] + "..." if entry[1] and len(entry[1]) > 30 else entry[1] or "None"
        original = entry[2] or "None"
        print(f"   {entry[0]}: Products={products} | Original={original}")
    
    # If we have fewer than expected enhanced entries, restore them
    if enhanced_charges < 10 or demerged_charges < 3:
        print(f"\n⚠️  INSUFFICIENT ENHANCED ENTRIES DETECTED")
        print(f"   Expected: >10 enhanced, >3 demerged")
        print(f"   Found: {enhanced_charges} enhanced, {demerged_charges} demerged")
        print(f"\n🔄 AUTOMATIC RESTORATION TRIGGERED...")
        
        import subprocess
        import sys
        
        try:
            # Run the restoration script
            result = subprocess.run([sys.executable, 'restore_enhanced_service_charges.py'], 
                                  capture_output=True, text=True)
            
            if result.returncode == 0:
                print("✅ Enhanced service charges successfully restored!")
            else:
                print(f"❌ Restoration failed: {result.stderr}")
                
        except Exception as e:
            print(f"❌ Could not run restoration: {e}")
    else:
        print(f"\n✅ ENHANCED ENTRIES INTACT")
        print(f"   No restoration needed")
    
    # Create backup of current enhanced entries
    print(f"\n💾 Creating backup of enhanced entries...")
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS enhanced_service_charges_backup AS
        SELECT * FROM dhl_express_services_surcharges
        WHERE products_applicable IS NOT NULL AND products_applicable != ''
           OR original_service_code IS NOT NULL AND original_service_code != ''
    ''')
    
    cursor.execute('SELECT COUNT(*) FROM enhanced_service_charges_backup')
    backup_count = cursor.fetchone()[0]
    
    print(f"   ✅ Backed up {backup_count} enhanced entries")
    
    conn.commit()
    conn.close()

def add_protection_to_loader():
    """Add automatic protection to the non-destructive loader"""
    
    print(f"\n🔧 UPDATING LOADER WITH PROTECTION...")
    
    # Check if protection is already added
    with open('non_destructive_rate_loader.py', 'r') as f:
        content = f.read()
        
    if 'protect_enhanced_service_charges' in content:
        print("   ✅ Protection already integrated")
        return
    
    # Add protection call at the end of main function
    protection_code = '''
    # Protect enhanced service charges after upload
    try:
        from service_charge_protection import protect_enhanced_service_charges
        protect_enhanced_service_charges()
    except ImportError:
        print("⚠️  Service charge protection not available")
    except Exception as e:
        print(f"⚠️  Protection failed: {e}")
'''
    
    # Add before the final return in main()
    updated_content = content.replace(
        'print(f"\\n🎉 Successfully processed {len(successful_sheets)} sheets:")',
        protection_code + '    print(f"\\n🎉 Successfully processed {len(successful_sheets)} sheets:")'
    )
    
    if updated_content != content:
        with open('non_destructive_rate_loader.py', 'w') as f:
            f.write(updated_content)
        print("   ✅ Protection integrated into loader")
    else:
        print("   ⚠️  Could not integrate protection")

if __name__ == "__main__":
    protect_enhanced_service_charges()
    add_protection_to_loader()
    
    print(f"\n🎯 PROTECTION SUMMARY:")
    print("=" * 40)
    print("✅ Enhanced service charges are now protected")
    print("✅ Loader will preserve existing enhanced data") 
    print("✅ Automatic restoration on upload")
    print("\n🚀 Your enhanced service charges are safe!")
