# PowerShell script to delete test and debug files
# This script deletes files that don't directly impact the application

$filesToDelete = @(
    # Previously identified files
    "check_30kg_rate.py",
    "check_country_codes.py",
    "check_rate_card.py",
    "debug_excel_structure.py",
    "debug_line_audit.py",
    "debug_rate_card_details.py",
    "test-dhl-express-audit-fixed.py",
    "test-dhl-express-simple.py",
    "test-dhl-final-summary.py",
    "test-results-format.py",
    "test_audit_status.py",
    "test_country_zone_mapping.py",
    "test_dhl_express_audit_api.py",
    "test_dhl_express_audit_comprehensive.py",
    "test_dhl_express_rate_cards.py",
    "test_express_audit_with_negative_variance.py",
    "test_invoice_51kg.py",
    "test_rate_card.py",
    "test_rate_card_details_api.py",
    "test_rate_card_details_api_direct.py",
    "test_rate_card_viewer.py",
    "test_updated_zone_lookup.py",
    "test_zone_lookup.py",
    
    # Files from repository
    "check_audit_details.py",
    "check_comprehensive_status.py",
    "check_database.py",
    "check_db_structure.py",
    "check_invoice.py",
    "check_invoice_d1975996.py",
    "check_invoice_d1976040.py",
    "check_invoice_d2151088.py",
    "check_pricing_table.py",
    "check_tables.py",
    "debug_audit_details.py",
    "debug_audit_flow.py",
    "debug_audit_structure.py",
    "debug_batch_integration.py",
    "debug_invoice_d2077375.py",
    "debug_lane_matching.py",
    "debug_latest_batch.py",
    "debug_lcl_rates.py",
    "debug_ocean_engine.py",
    "debug_weight_data.py",
    "run_test_batch_audit.py",
    "test_advanced_features.py",
    "test_air_freight_audit.py",
    "test_audit_analysis.py",
    "test_audit_path_debug.py",
    "test_audit_process_analysis.py",
    "test_auth.py",
    "test_batch_audit.py",
    "test_batch_audit_d2031045.py",
    "test_batch_audit_fix.py",
    "test_batch_summary.py",
    "test_charge_breakdown.py",
    "test_complete_audit.py",
    "test_comprehensive_breakdown.py",
    "test_comprehensive_extraction.py",
    "test_detailed_analysis_fix.py",
    "test_detailed_breakdown.py",
    "test_endblock_issue.py",
    "test_enhanced_charge_extraction.py",
    "test_enhanced_processor.py",
    "test_extraction.py",
    "test_find_shanghai_sydney.py",
    "test_fix_template.py",
    "test_fixed_audit_engine.py",
    "test_improved_lane_matching.py",
    "test_integrated_audit.py",
    "test_jinja_check.py",
    "test_new_features.py",
    "test_passthrough_charges.py",
    "test_port_code_mapping.py",
    "test_rate_structure.py",
    "test_service_type_issue.py",
    "test_template_check.py",
    
    # Other test/debug files
    "update_usa_zone3_rates.py",
    "setup_zone_mappings.py"
)

# Delete each file
foreach ($file in $filesToDelete) {
    if (Test-Path $file) {
        Write-Host "Deleting $file"
        Remove-Item $file -Force
    }
    else {
        Write-Host "$file does not exist, skipping"
    }
}

Write-Host "Cleanup completed!"
