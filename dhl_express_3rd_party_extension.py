#!/usr/bin/env python3
"""
DHL Express Audit Engine Extension for 3rd Party Charges
========================================================

Extends the existing DHLExpressAuditEngine with 3rd party charge logic.
"""

import sqlite3
from typing import Dict, Optional


class DHLExpressAuditEngineExtended:
    """Extended DHL Express Audit Engine with 3rd Party Support"""
    
    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path
    
    def _audit_express_rate_extended(self, line: Dict, conn) -> Dict:
        """Extended express rate audit with 3rd party support"""
        cursor = conn.cursor()
        
        product_desc = line.get('description', '').upper()
        weight = line.get('weight', 0)
        amount = line.get('amount', 0)
        awb = line.get('awb_number')
        
        # Check if this is a 3rd party charge
        if self._is_3rd_party_charge(product_desc):
            return self._audit_3rd_party_rate(line, conn)
        else:
            # Use existing logic for regular DHL Express charges
            return self._audit_regular_express_rate(line, conn)
    
    def _is_3rd_party_charge(self, product_desc: str) -> bool:
        """Determine if this is a 3rd party charge"""
        # Check for 3rd party indicators in product description
        third_party_indicators = [
            '3RD PARTY',
            'THIRD PARTY', 
            'EXPRESS WORLDWIDE',
            'EXPRESS 3RDCTY',
            'THIRD COUNTRY'
        ]
        
        for indicator in third_party_indicators:
            if indicator in product_desc.upper():
                return True
        
        return False
    
    def _audit_3rd_party_rate(self, line: Dict, conn) -> Dict:
        """Audit 3rd party charge using our new logic"""
        cursor = conn.cursor()
        
        weight = line.get('weight', 0)
        amount = line.get('amount', 0)
        awb = line.get('awb_number')
        
        try:
            # Get origin and destination countries
            cursor.execute('''
                SELECT shipper_details, receiver_details
                FROM dhl_express_invoices
                WHERE awb_number = ?
                LIMIT 1
            ''', (awb,))
            
            result = cursor.fetchone()
            if not result:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': ['No shipment details found for 3rd party audit']
                }
            
            shipper_details, receiver_details = result
            
            # Extract country codes
            origin_country = self._extract_country_code(shipper_details)
            dest_country = self._extract_country_code(receiver_details)
            
            if not origin_country or not dest_country:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': ['Could not extract country codes for 3rd party audit']
                }
            
            # Step 1: Get zones from 3rd party mapping
            cursor.execute('''
                SELECT zone FROM dhl_express_3rd_party_zones 
                WHERE country_code = ?
            ''', (origin_country,))
            origin_result = cursor.fetchone()
            
            cursor.execute('''
                SELECT zone FROM dhl_express_3rd_party_zones 
                WHERE country_code = ?
            ''', (dest_country,))
            dest_result = cursor.fetchone()
            
            if not origin_result or not dest_result:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': [f'3rd party zones not found for {origin_country} → {dest_country}']
                }
            
            origin_zone = origin_result[0]
            dest_zone = dest_result[0]
            
            # Step 2: Get rate zone from matrix
            cursor.execute('''
                SELECT rate_zone FROM dhl_express_3rd_party_matrix 
                WHERE origin_zone = ? AND destination_zone = ?
            ''', (origin_zone, dest_zone))
            matrix_result = cursor.fetchone()
            
            if not matrix_result:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': [f'No 3rd party matrix entry for Zone {origin_zone} × Zone {dest_zone}']
                }
            
            rate_zone = matrix_result[0]
            
            # Step 3: Get rate for weight and zone
            zone_column = f"zone_{rate_zone.lower()}"
            cursor.execute(f'''
                SELECT {zone_column} FROM dhl_express_3rd_party_rates 
                WHERE weight_kg = ?
            ''', (weight,))
            rate_result = cursor.fetchone()
            
            if not rate_result or rate_result[0] is None:
                return {
                    'expected_amount': amount,
                    'variance': 0,
                    'audit_result': 'REVIEW',
                    'comments': [f'No 3rd party rate found for {weight}kg Zone {rate_zone}']
                }
            
            expected_amount = float(rate_result[0])
            variance = expected_amount - amount
            
            # Determine audit result
            if abs(variance) <= amount * 0.05:  # Within 5%
                audit_result = 'PASS'
            elif abs(variance) <= amount * 0.15:  # Within 15%
                audit_result = 'REVIEW'
            else:
                audit_result = 'FAIL'
            
            comments = [
                f'3rd Party: {origin_country} Zone {origin_zone} → {dest_country} Zone {dest_zone} = Zone {rate_zone}',
                f'Weight: {weight}kg, Expected: ${expected_amount:.2f}, Variance: ${variance:.2f}'
            ]
            
            return {
                'expected_amount': expected_amount,
                'variance': variance,
                'audit_result': audit_result,
                'comments': comments
            }
            
        except Exception as e:
            return {
                'expected_amount': amount,
                'variance': 0,
                'audit_result': 'ERROR',
                'comments': [f'3rd party audit error: {str(e)}']
            }
    
    def _extract_country_code(self, address_details: str) -> str:
        """Extract country code from address details - same as existing logic"""
        if not address_details:
            return None
        
        # Split by semicolon and get the last meaningful part
        parts = address_details.split(';')
        
        # Look for 2-letter country code
        for part in reversed(parts):
            part = part.strip()
            if len(part) == 2 and part.isupper() and part.isalpha():
                return part
        
        # Fallback mappings
        address_upper = address_details.upper()
        country_mappings = {
            'JAPAN': 'JP', 'NEW ZEALAND': 'NZ', 'AUSTRALIA': 'AU',
            'UNITED STATES': 'US', 'USA': 'US', 'CHINA': 'CN'
            # Add more as needed
        }
        
        for country_name, country_code in country_mappings.items():
            if country_name in address_upper:
                return country_code
                
        return None


# Integration example - how to modify the existing engine
def integrate_3rd_party_into_existing_engine():
    """Example of how to integrate 3rd party logic into existing engine"""
    
    # You would modify the _audit_express_rate method in dhl_express_audit_engine.py
    # to include this 3rd party detection and processing logic
    
    print("""
    To integrate 3rd party support into the existing engine:
    
    1. Add the _is_3rd_party_charge() method to DHLExpressAuditEngine
    2. Add the _audit_3rd_party_rate() method to DHLExpressAuditEngine  
    3. Modify _audit_express_rate() to check for 3rd party charges first
    4. Update the database tables (already done)
    
    This approach maintains all existing functionality while adding 3rd party support.
    """)


if __name__ == "__main__":
    integrate_3rd_party_into_existing_engine()
