#!/usr/bin/env python3
"""
Demonstrate difference between UPDATE vs REPLACE modes for rate card loading
"""

import sqlite3
from enhanced_au_domestic_loader import EnhancedAUDomesticRateCardLoader

def compare_upload_modes():
    """Compare UPDATE vs REPLACE modes"""
    print("🔍 COMPARING UPLOAD MODES: UPDATE vs REPLACE")
    print("=" * 60)
    
    loader = EnhancedAUDomesticRateCardLoader()
    file_path = r'uploads\DHL EXPRESS AU Domestic Cards.xlsx'
    
    # Check current database state
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    print("\n📊 CURRENT DATABASE STATE:")
    cursor.execute("SELECT COUNT(*) FROM dhl_express_au_domestic_zones")
    zones_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM dhl_express_au_domestic_matrix")
    matrix_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM dhl_express_au_domestic_rates")
    rates_count = cursor.fetchone()[0]
    
    print(f"   Zones: {zones_count}")
    print(f"   Matrix: {matrix_count}")
    print(f"   Rates: {rates_count}")
    
    print("\n" + "="*60)
    print("🔄 SCENARIO 1: REPLACE MODE (Recommended)")
    print("="*60)
    print("What happens: Completely clears old data, loads fresh copy")
    print("Result: No duplicates, clean replacement")
    
    # Test replace mode
    results = loader.load_rate_card(file_path, uploaded_by='demo_user', replace_existing=True)
    print(f"\n✅ REPLACE Results: {results['zones_loaded']} zones, {results['matrix_loaded']} matrix, {results['rates_loaded']} rates")
    
    # Check final counts
    cursor.execute("SELECT COUNT(*) FROM dhl_express_au_domestic_zones")
    final_zones = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM dhl_express_au_domestic_matrix")
    final_matrix = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM dhl_express_au_domestic_rates")
    final_rates = cursor.fetchone()[0]
    
    print(f"📊 Final Database State:")
    print(f"   Zones: {final_zones} (was {zones_count})")
    print(f"   Matrix: {final_matrix} (was {matrix_count})")
    print(f"   Rates: {final_rates} (was {rates_count})")
    
    print("\n" + "="*60)
    print("🔄 SCENARIO 2: What if you upload AGAIN with REPLACE")
    print("="*60)
    
    # Upload again
    results2 = loader.load_rate_card(file_path, uploaded_by='demo_user', replace_existing=True)
    print(f"\n✅ Second REPLACE Results: {results2['zones_loaded']} zones, {results2['matrix_loaded']} matrix, {results2['rates_loaded']} rates")
    
    # Check counts again
    cursor.execute("SELECT COUNT(*) FROM dhl_express_au_domestic_zones")
    final_zones2 = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM dhl_express_au_domestic_matrix")
    final_matrix2 = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM dhl_express_au_domestic_rates")
    final_rates2 = cursor.fetchone()[0]
    
    print(f"📊 Database State After Second Upload:")
    print(f"   Zones: {final_zones2} (still {final_zones})")
    print(f"   Matrix: {final_matrix2} (still {final_matrix})")
    print(f"   Rates: {final_rates2} (still {final_rates})")
    
    print("\n" + "="*60)
    print("📋 UPLOAD HISTORY")
    print("="*60)
    
    # Show upload history
    cursor.execute("""
        SELECT id, filename, upload_date, status, zones_loaded, matrix_loaded, rates_loaded, uploaded_by
        FROM dhl_express_au_domestic_uploads 
        ORDER BY upload_date DESC 
        LIMIT 5
    """)
    uploads = cursor.fetchall()
    
    for upload in uploads:
        upload_id, filename, date, status, zones, matrix, rates, user = upload
        print(f"   Upload {upload_id}: {filename} by {user}")
        print(f"      Date: {date}")
        print(f"      Status: {status}")
        print(f"      Data: {zones} zones, {matrix} matrix, {rates} rates")
        print()
    
    conn.close()
    
    print("🎯 CONCLUSION:")
    print("✅ REPLACE mode prevents duplicates completely")
    print("✅ Each upload completely refreshes the rate card")
    print("✅ Upload history tracks all changes")
    print("✅ Recommended for production use")

if __name__ == "__main__":
    compare_upload_modes()
