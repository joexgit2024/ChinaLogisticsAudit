#!/usr/bin/env python3
"""
DGF Audit System Demo Script

This script demonstrates how to use the DGF audit system to:
1. Process DGF air and sea freight files
2. Extract spot quotes as baselines
3. Audit invoices against quotes
4. Generate reports

Usage: python dgf_demo.py
"""

from dgf_audit_system import DGFAuditSystem
import sqlite3
import pandas as pd

def demo_dgf_audit():
    """Demonstrate the DGF audit system."""
    print("🚢 DGF Air & Sea Freight Audit System Demo")
    print("=" * 50)
    
    # Initialize the audit system
    print("\n1. Initializing DGF Audit System...")
    audit_system = DGFAuditSystem()
    print("✅ System initialized successfully")
    
    # File paths
    air_file = r'uploads\DGF AIR\DGF Air.xlsx'
    sea_file = r'uploads\DGF SEA\DGF-CN10 billing.xlsx'
    
    # Process files
    print("\n2. Processing DGF Files...")
    print(f"   📄 Air file: {air_file}")
    print(f"   📄 Sea file: {sea_file}")
    
    audit_system.load_and_process_dgf_files(air_file, sea_file)
    print("✅ Files processed and quotes extracted")
    
    # Show quote statistics
    print("\n3. Spot Quote Summary...")
    conn = sqlite3.connect('dhl_audit.db')
    
    # Count quotes by mode
    quote_stats = pd.read_sql_query('''
        SELECT 
            mode,
            COUNT(*) as quote_count,
            AVG(CASE WHEN mode = 'AIR' THEN rate_per_kg ELSE rate_per_cbm END) as avg_rate,
            COUNT(DISTINCT lane) as unique_lanes
        FROM dgf_spot_quotes 
        WHERE status = 'ACTIVE'
        GROUP BY mode
    ''', conn)
    
    print(quote_stats.to_string(index=False))
    
    # Show invoice statistics
    print("\n4. Invoice Summary...")
    invoice_stats = pd.read_sql_query('''
        SELECT 
            mode,
            COUNT(*) as invoice_count,
            AVG(total_cny) as avg_total_cny,
            MIN(actual_arrival_date) as earliest_date,
            MAX(actual_arrival_date) as latest_date
        FROM dgf_invoices 
        WHERE status = 'PROCESSED'
        GROUP BY mode
    ''', conn)
    
    print(invoice_stats.to_string(index=False))
    
    # Run audit
    print("\n5. Running Audit Against Spot Quotes...")
    audit_results = audit_system.audit_all_invoices()
    
    print(f"📊 Audit Results Summary:")
    print(f"   • Total Invoices: {audit_results['total_invoices']}")
    print(f"   • Successfully Audited: {audit_results['audited']}")
    print(f"   • ✅ Passed: {audit_results['passed']}")
    print(f"   • ⚠️  Warnings: {audit_results['warnings']}")
    print(f"   • ❌ Failed: {audit_results['failed']}")
    print(f"   • 🔴 Errors: {audit_results['errors']}")
    print(f"   • 💰 Total Overcharge: ¥{audit_results['total_overcharge']:.2f}")
    print(f"   • 💸 Total Undercharge: ¥{audit_results['total_undercharge']:.2f}")
    
    # Show detailed variance analysis
    print("\n6. Variance Analysis...")
    variance_analysis = pd.read_sql_query('''
        SELECT 
            i.mode,
            ar.overall_status,
            COUNT(*) as count,
            AVG(ar.audit_score) as avg_score,
            SUM(ar.net_variance) as total_variance,
            AVG(ABS(ar.freight_variance_pct)) as avg_freight_var_pct
        FROM dgf_audit_results ar
        JOIN dgf_invoices i ON ar.invoice_id = i.id
        GROUP BY i.mode, ar.overall_status
        ORDER BY i.mode, ar.overall_status
    ''', conn)
    
    print(variance_analysis.to_string(index=False))
    
    # Show top variances
    print("\n7. Top Variances (by amount)...")
    top_variances = pd.read_sql_query('''
        SELECT 
            i.quote_id,
            i.mode,
            i.hbl_number,
            ar.overall_status,
            ar.audit_score,
            ar.net_variance,
            ar.freight_variance_pct
        FROM dgf_audit_results ar
        JOIN dgf_invoices i ON ar.invoice_id = i.id
        WHERE ABS(ar.net_variance) > 0
        ORDER BY ABS(ar.net_variance) DESC
        LIMIT 10
    ''', conn)
    
    print(top_variances.to_string(index=False))
    
    # Generate report
    print("\n8. Generating Excel Report...")
    report_file = audit_system.generate_audit_report('dgf_demo_report.xlsx')
    print(f"📋 Report saved: {report_file}")
    
    # Show sample spot quotes
    print("\n9. Sample Spot Quotes...")
    sample_quotes = pd.read_sql_query('''
        SELECT 
            quote_id,
            mode,
            lane,
            rate_per_kg,
            rate_per_cbm,
            origin_handling_fee,
            dest_handling_fee
        FROM dgf_spot_quotes 
        WHERE status = 'ACTIVE'
        ORDER BY quote_date DESC
        LIMIT 5
    ''', conn)
    
    print(sample_quotes.to_string(index=False))
    
    # Show sample audit results
    print("\n10. Sample Audit Results...")
    sample_audits = pd.read_sql_query('''
        SELECT 
            i.quote_id,
            i.mode,
            i.hbl_number,
            ar.overall_status,
            ar.audit_score,
            ar.freight_variance_pct,
            ar.net_variance
        FROM dgf_audit_results ar
        JOIN dgf_invoices i ON ar.invoice_id = i.id
        ORDER BY ar.audit_date DESC
        LIMIT 10
    ''', conn)
    
    print(sample_audits.to_string(index=False))
    
    conn.close()
    
    print("\n" + "=" * 50)
    print("🎉 DGF Audit Demo Complete!")
    print(f"📊 View detailed results in: {report_file}")
    print("🌐 Access web interface at: http://localhost:5000/dgf")
    print("=" * 50)

def show_audit_methodology():
    """Explain the audit methodology."""
    print("\n📚 DGF Audit Methodology")
    print("-" * 30)
    print("""
🔍 How DGF Audit Works:

1. SPOT QUOTE EXTRACTION
   • Each row in Excel files represents a spot quote
   • Quotes are baseline rates for specific lanes
   • Air freight: Rate per KG
   • Sea freight: Rate per CBM

2. INVOICE PROCESSING
   • Invoices linked to quotes by Quote ID
   • Actual charges extracted from invoice data
   • Currency conversion applied using provided FX rates

3. VARIANCE CALCULATION
   • Freight Rate: Compare (Weight/Volume × Quote Rate) vs Actual
   • Handling Fees: Compare quote fees vs invoice fees
   • Tolerance: 5% for freight, 10% for fees

4. AUDIT SCORING
   • Start with 100 points
   • Deduct for variances:
     - Major freight variance: up to 50 points
     - Fee variance: up to 25 points
   • Status: PASS (>90), WARNING (70-90), FAIL (<70)

5. FINANCIAL IMPACT
   • Overcharge: Actual > Expected
   • Undercharge: Actual < Expected
   • Net variance in CNY for portfolio analysis
   
💡 Key Benefits:
   ✅ Validates spot quote compliance
   ✅ Identifies billing errors
   ✅ Tracks financial impact
   ✅ Enables rate negotiation insights
    """)

if __name__ == '__main__':
    try:
        demo_dgf_audit()
        show_audit_methodology()
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        print("Make sure DGF files exist in the uploads folder")
