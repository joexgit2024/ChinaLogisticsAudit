#!/usr/bin/env python3
"""
DHL Express China Invoice Loader
===============================

Loads Chinese DHL Express invoices from Excel files
Handles the 68-column format with proper data mapping
"""
import pandas as pd
import sqlite3
import os
from datetime import datetime
from typing import Dict, List, Optional

class DHLExpressChinaInvoiceLoader:
    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path
    
    def load_invoices_from_excel(self, excel_path: str, uploaded_by: str = 'system') -> Dict:
        """Load Chinese DHL invoices from Excel file"""
        try:
            print(f"📄 Loading Chinese DHL invoices from: {excel_path}")
            
            # Record upload start
            upload_id = self._record_upload_start(excel_path, uploaded_by)
            
            # Read Excel file
            df = pd.read_excel(excel_path, sheet_name='Table1')
            print(f"   📊 Found {len(df)} invoice records")
            
            # Clean and validate data
            df_clean = self._clean_invoice_data(df)
            print(f"   🧹 Cleaned data: {len(df_clean)} valid records")
            
            # Load into database
            loaded_count = self._load_to_database(df_clean)
            
            # Update upload record
            self._record_upload_complete(upload_id, loaded_count, len(df), 0)
            
            return {
                'success': True,
                'records_loaded': loaded_count,
                'total_records': len(df),
                'filename': os.path.basename(excel_path),
                'upload_id': upload_id
            }
            
        except Exception as e:
            error_msg = f"Error loading Chinese DHL invoices: {str(e)}"
            print(f"   ❌ {error_msg}")
            
            # Update upload record with error
            if 'upload_id' in locals():
                self._record_upload_complete(upload_id, 0, 0, 1, error_msg)
            
            return {
                'success': False,
                'error': error_msg,
                'records_loaded': 0
            }
    
    def _clean_invoice_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate invoice data"""
        print("   🧹 Cleaning invoice data...")
        
        # Drop rows with missing critical fields
        df_clean = df.dropna(subset=['Air waybill', 'Invoice Number'])
        
        # Convert dates and handle NaT values
        date_columns = ['Shipment Date', 'Load Date', 'Invoice Date', 'Original Invoice Dt.']
        for col in date_columns:
            if col in df_clean.columns:
                # Convert to datetime, then to string for SQLite compatibility
                df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                df_clean[col] = df_clean[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
        
        # Convert numeric columns
        numeric_columns = [
            'Pieces', 'Customer Weight', 'DHL Weight', 'Customer Vol. Weight', 
            'DHL Vol. Weight', 'Billed Weight (Kilos)', 'Billed Weight (Pounds)',
            'Weight Charge', 'Fuel Surcharges', 'Other Charges', 'Discount',
            'Imp/Exp Duties & Taxes', 'Taxes to Applicable Charges', 'BCU Total',
            'Ex. Rate To LCU', 'LCU Weight Charge', 'LCU Fuel Surcharges',
            'LCU Other Charges', 'LCU Discount', 'LCU Imp/Exp Duties & Taxes',
            'LCU Taxes to Applicable Charges', 'LCU Total'
        ]
        
        for col in numeric_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)
        
        return df_clean
    
    def _load_to_database(self, df: pd.DataFrame) -> int:
        """Load cleaned data to database"""
        print("   💾 Loading to database...")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        loaded_count = 0
        
        for _, row in df.iterrows():
            try:
                # Map DataFrame columns to database columns
                invoice_data = {
                    'source_country': row.get('Source Country'),
                    'billing_period': row.get('Billing Period'),
                    'service_type': row.get('Service Type'),
                    'billing_term': row.get('Billing Term'),
                    'billing_type': row.get('Billing Type'),
                    'transaction_status': row.get('Transaction Status'),
                    'ib434_line_id': row.get('IB434 Line ID'),
                    'omit_code': row.get('Omit Code'),
                    'offset_code': row.get('Offset Code'),
                    'transaction_status_description': row.get('Transaction Status Description'),
                    
                    'shipment_date': row.get('Shipment Date'),
                    'load_date': row.get('Load Date'),
                    'air_waybill': str(int(row.get('Air waybill', 0))) if pd.notna(row.get('Air waybill')) else '',
                    'unique_record_id': row.get('Unique Record ID'),
                    'shipper_account': str(row.get('Shipper Account', '')),
                    'bill_to_account': str(row.get('Bill To Account', '')),
                    'bill_to_account_name': row.get('Bill To Account Name'),
                    'shipper_reference': row.get('Shipper Reference'),
                    'local_product_code': row.get('Local Product Code'),
                    'origin_code': row.get('Orgn'),
                    'dest_code': row.get('Dest'),
                    
                    'billing_currency': row.get('Billing Currency'),
                    'invoice_number': row.get('Invoice Number'),
                    'invoice_date': row.get('Invoice Date'),
                    'original_invoice': row.get('Original Invoice'),
                    'original_invoice_date': row.get('Original Invoice Dt.'),
                    'modify_code': row.get('Modify Code'),
                    'modify_description': row.get('Modify Description'),
                    
                    'pieces': int(row.get('Pieces', 0)),
                    'customer_weight': float(row.get('Customer Weight', 0)),
                    'dhl_weight': float(row.get('DHL Weight', 0)),
                    'customer_vol_weight': float(row.get('Customer Vol. Weight', 0)),
                    'dhl_vol_weight': float(row.get('DHL Vol. Weight', 0)),
                    'weight_code': row.get('Weight Code'),
                    'billed_weight_kg': float(row.get('Billed Weight (Kilos)', 0)),
                    'billed_weight_lbs': float(row.get('Billed Weight (Pounds)', 0)),
                    
                    'bcu_weight_charge': float(row.get('Weight Charge', 0)),
                    'bcu_fuel_surcharges': float(row.get('Fuel Surcharges', 0)),
                    'bcu_other_charges': float(row.get('Other Charges', 0)),
                    'bcu_discount': float(row.get('Discount', 0)),
                    'bcu_duties_taxes': float(row.get('Imp/Exp Duties & Taxes', 0)),
                    'bcu_taxes_applicable': float(row.get('Taxes to Applicable Charges', 0)),
                    'bcu_total': float(row.get('BCU Total', 0)),
                    
                    'exchange_rate': float(row.get('Ex. Rate To LCU', 1)),
                    'local_currency': row.get('Local Currency'),
                    'lcu_weight_charge': float(row.get('LCU Weight Charge', 0)),
                    'lcu_fuel_surcharges': float(row.get('LCU Fuel Surcharges', 0)),
                    'lcu_other_charges': float(row.get('LCU Other Charges', 0)),
                    'lcu_discount': float(row.get('LCU Discount', 0)),
                    'lcu_duties_taxes': float(row.get('LCU Imp/Exp Duties & Taxes', 0)),
                    'lcu_taxes_applicable': float(row.get('LCU Taxes to Applicable Charges', 0)),
                    'lcu_total': float(row.get('LCU Total', 0)),
                    
                    'consignor_name': row.get('Consignor Name'),
                    'consignor_contact_name': row.get('Consignor Contact Name'),
                    'consignor_address_1': row.get('Consignor Address 1'),
                    'consignor_address_2': row.get('Consignor Address 2'),
                    'consignor_city': row.get('Consignor City'),
                    'consignor_province_state': row.get('Consignor Province / State'),
                    'consignor_country': row.get('Consignor Country'),
                    'consignor_postal_code': str(row.get('Consignor Postal Code', '')),
                    
                    'consignee_name': row.get('Consignee Name'),
                    'consignee_contact_name': row.get('Consignee Contact Name'),
                    'consignee_address_1': row.get('Consignee Address 1'),
                    'consignee_address_2': row.get('Consignee Address 2'),
                    'consignee_city': row.get('Consignee City'),
                    'consignee_province_state': row.get('Consignee Province / State'),
                    'consignee_country': row.get('Consignee Country'),
                    'consignee_postal_code': str(row.get('Consignee Postal Code', ''))
                }
                
                # Insert into database
                columns = ', '.join(invoice_data.keys())
                placeholders = ', '.join(['?' for _ in invoice_data.values()])
                
                cursor.execute(f'''
                    INSERT OR REPLACE INTO dhl_express_china_invoices ({columns})
                    VALUES ({placeholders})
                ''', list(invoice_data.values()))
                
                loaded_count += 1
                
            except Exception as e:
                print(f"      ⚠️  Error loading record {loaded_count + 1}: {e}")
                continue
        
        conn.commit()
        conn.close()
        
        return loaded_count
    
    def _record_upload_start(self, filename: str, uploaded_by: str) -> int:
        """Record the start of an upload process"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO dhl_express_china_uploads 
            (filename, original_filename, uploaded_by, status)
            VALUES (?, ?, ?, ?)
        ''', (os.path.basename(filename), filename, uploaded_by, 'processing'))
        
        upload_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return upload_id
    
    def _record_upload_complete(self, upload_id: int, loaded: int, total: int, 
                              errors: int, notes: str = None):
        """Record the completion of an upload process"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        status = 'completed' if errors == 0 else 'completed_with_errors'
        
        cursor.execute('''
            UPDATE dhl_express_china_uploads 
            SET processed_date = ?, status = ?, records_processed = ?, 
                records_loaded = ?, errors_count = ?, notes = ?
            WHERE id = ?
        ''', (datetime.now(), status, total, loaded, errors, notes, upload_id))
        
        conn.commit()
        conn.close()

def test_loader():
    """Test the Chinese invoice loader"""
    loader = DHLExpressChinaInvoiceLoader()
    result = loader.load_invoices_from_excel('uploads/DHL Bill.xlsx')
    print(f"\n📋 Test Result: {result}")

if __name__ == '__main__':
    test_loader()
