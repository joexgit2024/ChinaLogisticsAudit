#!/usr/bin/env python3
"""
Get detailed audit breakdown for invoice D1976040
"""

import sqlite3
import json

def get_audit_details():
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()

    # Get the audit details for D1976040
    cursor.execute('SELECT audit_details FROM ytd_audit_results WHERE invoice_no = ? ORDER BY created_at DESC LIMIT 1', ('D1976040',))
    result = cursor.fetchone()

    if result and result[0]:
        details = json.loads(result[0])
        
        if 'audit_results' in details:
            for i, audit_result in enumerate(details['audit_results']):
                print(f'=== Audit Result {i+1} ===')
                print(f'Service: {audit_result.get("service", "N/A")}')
                print(f'Lane: {audit_result.get("lane_description", "N/A")}')
                print(f'Status: {audit_result.get("audit_status", "N/A")}')
                
                print('\nCharges breakdown:')
                for variance in audit_result.get('variances', []):
                    print(f'  {variance.get("charge_type", "N/A")}: Expected ${variance.get("expected", 0):.2f}, Actual ${variance.get("actual", 0):.2f}, Variance ${variance.get("variance", 0):.2f}')
                    
                print(f'\nTotal Expected: ${audit_result.get("total_expected", 0):.2f}')
                print(f'Total Actual: ${audit_result.get("total_actual", 0):.2f}')
                print(f'Total Variance: ${audit_result.get("total_variance", 0):.2f}')
                print('')
    else:
        print('No audit details found')

    conn.close()

if __name__ == '__main__':
    get_audit_details()
