"""FedEx Audit Engine - initial scaffold."""
from __future__ import annotations
from typing import Dict, Any
import sqlite3, json, re
from datetime import datetime

# Database configuration
FEDEX_DB = 'fedex_audit.db'

def _to_float(v):
    try:
        if v is None: return None
        s = str(v).strip().replace(',', '')
        if not s: return None
        m = re.findall(r'-?\d+(?:\.\d+)?', s)
        return float(m[0]) if m else None
    except Exception:
        return None

def _to_int(v):
    try:
        if v is None: return None
        s = str(v).strip()
        if not s: return None
        m = re.findall(r'\d+', s)
        return int(m[0]) if m else None
    except Exception:
        return None

def _parse_date(v):
    if v is None: return None
    s = str(v).strip()
    if not s: return None
    if re.match(r'^\d{8}$', s):
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return s

def _parse_datetime(v):
    if v is None: return None
    s = str(v).strip()
    m = re.match(r'^(\d{8})\s(\d{2})(\d{2})$', s)
    if m:
        d, hh, mm = m.groups()
        return f"{d[0:4]}-{d[4:6]}-{d[6:8]} {hh}:{mm}:00"
    return s

def _safe(v):
    try:
        return None if v is None else str(v)
    except Exception:
        return None

class FedExAuditEngine:
    def __init__(self, db_path: str = FEDEX_DB):
        self.db_path = db_path

    def _conn(self):
        return sqlite3.connect(self.db_path)

    def load_invoice_xls(self, file_path: str) -> Dict[str, Any]:
        import pandas as pd
        try:
            xl = pd.ExcelFile(file_path)
            sheet_name = 'Sheet0' if 'Sheet0' in xl.sheet_names else xl.sheet_names[0]
            df = xl.parse(sheet_name)
        except Exception as e:
            return {'success': False, 'error': f'Failed to open invoice file: {e}'}
        df.columns = [c.strip() for c in df.columns]
        required = ['inv_no','awb_nbr','service_type','SvcAbbrev','ship_date','Rated Amt']
        missing = [c for c in required if c not in df.columns]
        if missing:
            return {'success': False, 'error': f'Missing required columns: {missing}'}
        
        inserted = 0
        duplicates = 0
        errors = 0
        
        with self._conn() as conn:
            cur = conn.cursor()
            self._ensure_tables(conn)
            for _, row in df.iterrows():
                try:
                    invoice_no = str(row.get('inv_no','')).strip()
                    awb = str(row.get('awb_nbr','')).strip()
                    if not invoice_no or not awb:
                        continue
                    
                    # Check if this combination already exists
                    cur.execute('''
                        SELECT COUNT(*) FROM fedex_invoices 
                        WHERE invoice_no = ? AND awb_number = ?
                    ''', (invoice_no, awb))
                    
                    if cur.fetchone()[0] > 0:
                        duplicates += 1
                        continue  # Skip this record as it already exists
                    
                    service_type = str(row.get('service_type','')).strip()
                    service_abbrev = str(row.get('SvcAbbrev','')).strip()
                    direction = str(row.get('in_out_bound_desc','')).strip()
                    pieces = _to_int(row.get('no_pieces'))
                    actual_wt = _to_float(row.get('ActualWgtV'))
                    chg_wt = _to_float(row.get('ChrgableWeight'))
                    dim_wt = _to_float(row.get('dim_wgt'))
                    origin_country = str(row.get('shpr_cntry','')).strip()
                    dest_country = str(row.get('cnsgn_cntry','')).strip()
                    origin_loc = str(row.get('orig_locn','')).strip()
                    exch_rate = _to_float(row.get('exchange_rate'))
                    rated_amt = _to_float(row.get('Rated Amt'))
                    discount_amt = _to_float(row.get('Discount Amt'))
                    fuel = _to_float(row.get('Fuel_Surcharge'))
                    other = _to_float(row.get('Other Surcharge'))
                    vat_amt = _to_float(row.get('China Vat Amt'))
                    total_awb = _to_float(row.get('AWB Bill amount'))
                    ship_date = _parse_date(row.get('ship_date'))
                    invoice_date = _parse_date(row.get('inv_date'))
                    delivery = _parse_datetime(row.get('Del_DateTime'))
                    raw_json = json.dumps({k: _safe(row.get(k)) for k in df.columns})
                    
                    cur.execute('''
                        INSERT INTO fedex_invoices (
                            invoice_no, invoice_date, awb_number, service_type, service_abbrev,
                            direction, pieces, actual_weight_kg, chargeable_weight_kg, dim_weight_kg,
                            origin_country, dest_country, origin_loc, ship_date, delivery_datetime,
                            exchange_rate, rated_amount_cny, discount_amount_cny, fuel_surcharge_cny,
                            other_surcharge_cny, vat_amount_cny, total_awb_amount_cny, raw_json
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ''', (
                        invoice_no, invoice_date, awb, service_type, service_abbrev,
                        direction, pieces, actual_wt, chg_wt, dim_wt,
                        origin_country, dest_country, origin_loc, ship_date, delivery,
                        exch_rate, rated_amt, discount_amt, fuel,
                        other, vat_amt, total_awb, raw_json
                    ))
                    inserted += 1
                except Exception as e:
                    errors += 1
                    continue
            conn.commit()
        return {
            'success': True, 
            'records_inserted': inserted,
            'duplicates_skipped': duplicates,
            'errors': errors,
            'total_processed': inserted + duplicates + errors
        }

    def get_invoice_summary(self) -> Dict[str, Any]:
        with self._conn() as conn:
            cur = conn.cursor()
            # Ensure tables exist so first visit before any upload doesn't error
            try:
                self._ensure_tables(conn)
            except Exception:
                pass
            cur.execute('SELECT COUNT(DISTINCT invoice_no), COUNT(*), SUM(total_awb_amount_cny) FROM fedex_invoices')
            row = cur.fetchone()
            invs, lines, amt = row if row else (0,0,0)
            cur.execute('SELECT service_type, COUNT(*), SUM(total_awb_amount_cny) FROM fedex_invoices GROUP BY service_type ORDER BY SUM(total_awb_amount_cny) DESC LIMIT 10')
            by_service = [{'service_type': r[0], 'count': r[1], 'amount': r[2]} for r in cur.fetchall()]
            return {'invoices': invs or 0, 'lines': lines or 0, 'total_amount_cny': round(amt or 0,2), 'top_services': by_service}

    def _ensure_tables(self, conn):
        cur = conn.cursor()
        
        # Create the main table with proper unique constraint
        cur.execute('''CREATE TABLE IF NOT EXISTS fedex_invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_no TEXT NOT NULL, 
            invoice_date TEXT, 
            awb_number TEXT NOT NULL, 
            service_type TEXT, 
            service_abbrev TEXT,
            direction TEXT, 
            pieces INTEGER, 
            actual_weight_kg REAL, 
            chargeable_weight_kg REAL, 
            dim_weight_kg REAL,
            origin_country TEXT, 
            dest_country TEXT, 
            origin_loc TEXT, 
            ship_date TEXT, 
            delivery_datetime TEXT,
            exchange_rate REAL, 
            rated_amount_cny REAL, 
            discount_amount_cny REAL, 
            fuel_surcharge_cny REAL,
            other_surcharge_cny REAL, 
            vat_amount_cny REAL, 
            total_awb_amount_cny REAL, 
            raw_json TEXT,
            created_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(invoice_no, awb_number)
        )''')
        
        # Create index for better performance on duplicate checks
        cur.execute('''CREATE INDEX IF NOT EXISTS idx_fedex_invoice_awb 
                      ON fedex_invoices(invoice_no, awb_number)''')
        
        conn.commit()

    def get_audit_status_summary(self) -> Dict[str, Any]:
        """Get summary statistics of audit results"""
        with self._conn() as conn:
            cursor = conn.cursor()
            
            # Ensure audit results table exists
            self._ensure_audit_tables(conn)
            
            # Get total counts
            cursor.execute('SELECT COUNT(DISTINCT invoice_no) FROM fedex_invoices')
            total_invoices = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT COUNT(*) FROM fedex_invoices')
            total_awbs = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT COUNT(*) FROM fedex_audit_results')
            audited_awbs = cursor.fetchone()[0] or 0
            
            # Get status counts
            cursor.execute('SELECT audit_status, COUNT(*) FROM fedex_audit_results GROUP BY audit_status')
            status_counts = dict(cursor.fetchall())
            
            # Get amount totals
            cursor.execute('SELECT SUM(invoice_amount), SUM(expected_amount), SUM(variance) FROM fedex_audit_results')
            amounts = cursor.fetchone()
            
            return {
                'total_invoices': total_invoices,
                'total_awbs': total_awbs,
                'audited_awbs': audited_awbs,
                'pass_count': status_counts.get('PASS', 0),
                'review_count': status_counts.get('REVIEW', 0),
                'fail_count': status_counts.get('FAIL', 0),
                'error_count': status_counts.get('ERROR', 0),
                'total_amount': amounts[0] or 0,
                'total_expected': amounts[1] or 0,
                'total_variance': amounts[2] or 0,
                'audit_completion_rate': (audited_awbs / total_awbs * 100) if total_awbs > 0 else 0
            }

    def get_audit_results(self, limit: int = None, offset: int = 0, status_filter: str = None) -> dict:
        """Get audit results with optional filtering and pagination"""
        with self._conn() as conn:
            cursor = conn.cursor()
            
            # Build WHERE clause
            where_clause = ""
            params = []
            
            if status_filter and status_filter != 'all':
                where_clause = "WHERE ar.audit_status = ?"
                params.append(status_filter)
            
            # Get total count
            count_query = f'''
                SELECT COUNT(*) FROM fedex_audit_results ar
                {where_clause}
            '''
            cursor.execute(count_query, params)
            total_count = cursor.fetchone()[0]
            
            # Get results with invoice data
            query = f'''
                SELECT ar.*, fi.origin_country, fi.dest_country, fi.actual_weight_kg, fi.chargeable_weight_kg
                FROM fedex_audit_results ar
                LEFT JOIN fedex_invoices fi ON ar.invoice_no = fi.invoice_no AND ar.awb_number = fi.awb_number
                {where_clause}
                ORDER BY ar.audit_timestamp DESC
            '''
            
            if limit:
                query += f" LIMIT {limit} OFFSET {offset}"
                
            cursor.execute(query, params)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'id': row[0],
                    'invoice_no': row[1],
                    'awb_number': row[2],
                    'audit_timestamp': row[3],
                    'invoiced_amount': row[4],  # invoice_amount column
                    'expected_amount': row[5],
                    'variance': row[6],
                    'variance_percentage': row[7],
                    'status': row[8],  # audit_status column
                    'zone_applied': row[9],
                    'rate_applied': row[10],
                    'fuel_surcharge_expected': row[11],
                    'vat_expected': row[12],
                    'audit_details': row[13],
                    'origin_country': row[14],
                    'dest_country': row[15],
                    'actual_weight': row[16],
                    'chargeable_weight': row[17],
                    'route': f"{row[14]} -> {row[15]}" if row[14] and row[15] else ""
                })
            
            return {
                'success': True,
                'results': results,
                'total_count': total_count,
                'returned_count': len(results)
            }

    def audit_batch(self, invoice_awb_list: list) -> dict:
        """Audit a batch of specific invoice/AWB combinations"""
        results = []
        success_count = 0
        error_count = 0
        
        for item in invoice_awb_list:
            if isinstance(item, dict):
                invoice_no = item.get('invoice_no')
                awb_number = item.get('awb_number')
            else:
                # Assume item is a tuple (invoice_no, awb_number)
                invoice_no, awb_number = item
                
            try:
                result = self._audit_single_awb(invoice_no, awb_number)
                results.append(result)
                
                if result['status'] in ['PASS', 'REVIEW', 'FAIL']:
                    success_count += 1
                else:
                    error_count += 1
                    
            except Exception as e:
                error_result = {
                    'invoice_no': invoice_no,
                    'awb_number': awb_number,
                    'status': 'ERROR',
                    'message': f'Audit failed: {str(e)}'
                }
                results.append(error_result)
                error_count += 1
        
        return {
            'success': True,
            'message': f'Batch audit completed. {success_count} successful, {error_count} errors.',
            'success_count': success_count,
            'error_count': error_count,
            'total_processed': len(invoice_awb_list),
            'results': results
        }

    def get_unaudited_invoices(self) -> list:
        """Get list of invoices that haven't been audited yet"""
        with self._conn() as conn:
            cursor = conn.cursor()
            
            # Find invoices that don't have corresponding audit results
            cursor.execute('''
                SELECT DISTINCT fi.invoice_no, fi.awb_number, fi.origin_country, fi.dest_country, 
                       fi.service_type, fi.total_awb_amount_cny
                FROM fedex_invoices fi
                LEFT JOIN fedex_audit_results far ON fi.invoice_no = far.invoice_no 
                    AND fi.awb_number = far.awb_number
                WHERE far.id IS NULL
                ORDER BY fi.invoice_no, fi.awb_number
                LIMIT 1000
            ''')
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'invoice_no': row[0],
                    'awb_number': row[1],
                    'origin_country': row[2],
                    'dest_country': row[3],
                    'service_type': row[4],
                    'amount': row[5]
                })
            
            return results

    def audit_all_unaudited_invoices(self) -> dict:
        """Audit all invoices that haven't been audited yet"""
        with self._conn() as conn:
            cursor = conn.cursor()
            
            # Get unaudited invoices
            cursor.execute('''
                SELECT fi.invoice_no, fi.awb_number
                FROM fedex_invoices fi
                LEFT JOIN fedex_audit_results ar ON fi.invoice_no = ar.invoice_no 
                    AND fi.awb_number = ar.awb_number
                WHERE ar.id IS NULL
            ''')
            
            unaudited = cursor.fetchall()
            
        if not unaudited:
            return {
                'success': True,
                'message': 'No unaudited invoices found.',
                'success_count': 0,
                'error_count': 0,
                'total_processed': 0,
                'results': []
            }
        
        # Audit the unaudited invoices
        return self.audit_batch(unaudited)

    def _audit_single_awb(self, invoice_no: str, awb_number: str) -> dict:
        """Audit a single AWB with comprehensive rate calculation and zone mapping"""
        with self._conn() as conn:
            cursor = conn.cursor()
            
            # Get invoice data
            cursor.execute('''
                SELECT total_awb_amount_cny, origin_country, dest_country, service_type, 
                       chargeable_weight_kg, actual_weight_kg, exchange_rate, fuel_surcharge_cny, vat_amount_cny
                FROM fedex_invoices 
                WHERE invoice_no = ? AND awb_number = ?
            ''', (invoice_no, awb_number))
            
            row = cursor.fetchone()
            if not row:
                return {
                    'invoice_no': invoice_no,
                    'awb_number': awb_number,
                    'status': 'ERROR',
                    'message': 'Invoice/AWB not found'
                }
            
            invoiced_amount, origin, dest, service, chargeable_weight, actual_weight, exchange_rate, fuel_invoiced, vat_invoiced = row
            
            # Build detailed audit trail
            audit_trail = {
                'step1_data_extraction': {
                    'origin_country': origin,
                    'dest_country': dest,
                    'service_type': service,
                    'actual_weight': actual_weight,
                    'chargeable_weight': chargeable_weight,
                    'invoiced_amount': invoiced_amount,
                    'exchange_rate': exchange_rate
                }
            }
            
            # Step 1: Determine zone mapping
            zone = self._get_zone_mapping(origin, dest, cursor)
            if not zone:
                audit_trail['step2_zone_mapping'] = {
                    'error': f'No zone mapping found for {origin} -> {dest}',
                    'zone_applied': None
                }
                expected_amount = invoiced_amount
                variance = 0
                status = 'ERROR'
                audit_details = json.dumps(audit_trail)
            else:
                audit_trail['step2_zone_mapping'] = {
                    'origin_country': origin,
                    'dest_country': dest,
                    'zone_applied': zone,
                    'mapping_found': True
                }
                
                # Step 2: Calculate expected base rate
                base_rate_result = self._calculate_base_rate(service, zone, chargeable_weight, cursor)
                audit_trail['step3_base_rate_calculation'] = base_rate_result
                
                # Step 3: Calculate FedEx surcharges (excluding fuel)
                # Assuming declared value is 10% of invoiced amount if not available
                declared_value = invoiced_amount * 0.1
                surcharge_result = self._calculate_fedex_surcharges(
                    origin, dest, chargeable_weight, declared_value, service, cursor
                )
                audit_trail['step4_surcharge_calculation'] = surcharge_result
                
                # Step 4: Calculate fuel surcharge (applied to base + surcharges)
                base_plus_surcharges = base_rate_result['base_cost_usd'] + surcharge_result['total_surcharge_usd']
                fuel_result = self._calculate_fuel_surcharge(base_plus_surcharges, cursor)
                audit_trail['step5_fuel_surcharge'] = fuel_result
                
                # Step 5: Convert to CNY
                exchange_rate_used = exchange_rate or 7.3
                base_cost_cny = base_rate_result['base_cost_usd'] * exchange_rate_used
                surcharge_cost_cny = surcharge_result['total_surcharge_usd'] * exchange_rate_used
                fuel_cost_cny = fuel_result['fuel_cost_usd'] * exchange_rate_used
                
                audit_trail['step6_currency_conversion'] = {
                    'exchange_rate_used': exchange_rate_used,
                    'base_cost_usd': base_rate_result['base_cost_usd'],
                    'base_cost_cny': base_cost_cny,
                    'surcharge_cost_usd': surcharge_result['total_surcharge_usd'],
                    'surcharge_cost_cny': surcharge_cost_cny,
                    'fuel_cost_usd': fuel_result['fuel_cost_usd'],
                    'fuel_cost_cny': fuel_cost_cny
                }
                
                # Step 6: Calculate VAT (13% is typical for China)
                subtotal_cny = base_cost_cny + surcharge_cost_cny + fuel_cost_cny
                vat_expected = subtotal_cny * 0.13  # 13% VAT rate
                expected_amount = subtotal_cny + vat_expected
                
                audit_trail['step7_vat_calculation'] = {
                    'subtotal_cny': subtotal_cny,
                    'vat_rate': '13%',
                    'vat_expected': vat_expected,
                    'total_expected': expected_amount
                }
                
                # Step 7: Compare with invoiced amount and determine status
                variance = invoiced_amount - expected_amount
                variance_percentage = (variance / expected_amount * 100) if expected_amount > 0 else 0
                
                audit_trail['step8_variance_analysis'] = {
                    'invoiced_amount': invoiced_amount,
                    'expected_amount': expected_amount,
                    'variance_amount': variance,
                    'variance_percentage': variance_percentage
                }
                
                # Determine audit status based on variance
                if abs(variance_percentage) <= 5:  # Within 5%
                    status = 'PASS'
                elif abs(variance_percentage) <= 15:  # Within 15%
                    status = 'REVIEW'
                else:
                    status = 'FAIL'
                
                audit_trail['step8_conclusion'] = {
                    'audit_status': status,
                    'reason': f'Variance of {variance_percentage:.2f}% is {"within acceptable range" if status == "PASS" else "requires review" if status == "REVIEW" else "exceeds acceptable threshold"}'
                }
                
                audit_details = json.dumps(audit_trail)
            
            # Save audit result with detailed breakdown
            cursor.execute('''
                INSERT OR REPLACE INTO fedex_audit_results 
                (invoice_no, awb_number, audit_timestamp, invoice_amount, expected_amount, 
                 variance, variance_percentage, audit_status, zone_applied,
                 rate_applied, fuel_surcharge_expected, vat_expected, audit_details)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                invoice_no, awb_number, 
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                invoiced_amount, expected_amount,
                variance, variance_percentage, status, zone,
                base_rate_result.get('rate_applied') if 'base_rate_result' in locals() else None,
                fuel_result.get('fuel_cost_cny') if 'fuel_result' in locals() else None,
                vat_expected if 'vat_expected' in locals() else None,
                audit_details
            ))
            
            conn.commit()
            
            return {
                'invoice_no': invoice_no,
                'awb_number': awb_number,
                'status': status,
                'invoiced_amount': invoiced_amount,
                'expected_amount': expected_amount,
                'variance': variance,
                'audit_trail': audit_trail,
                'message': 'Comprehensive audit completed successfully'
            }

    def _get_zone_mapping(self, origin_country: str, dest_country: str, cursor) -> str:
        """Get zone mapping for origin-destination pair using the zone matrix"""
        # First, find the destination region for the destination country
        cursor.execute('''
            SELECT zone_letter FROM fedex_zone_matrix zm
            JOIN fedex_country_zones cz ON (
                (zm.destination_region = 'Africa' AND cz.country_code IN ('AO', 'BF', 'BI', 'BJ', 'BW', 'CD', 'CF', 'CG', 'CI', 'CM', 'CV', 'DJ', 'DZ', 'ER', 'ET', 'GA', 'GH', 'GM', 'GN', 'GQ', 'GW', 'KE', 'LR', 'LS', 'LY', 'MA', 'MG', 'ML', 'MR', 'MU', 'MW', 'MZ', 'NA', 'NE', 'NG', 'RE', 'RW', 'SC', 'SD', 'SL', 'SN', 'SO', 'SZ', 'TD', 'TG', 'TN', 'TZ', 'UG', 'ZA', 'ZM', 'ZW')) OR
                (zm.destination_region = 'Asia One' AND cz.country_code IN ('HK', 'MO', 'MY', 'SG', 'TH', 'TW')) OR
                (zm.destination_region = 'Asia Two' AND cz.country_code IN ('AU', 'ID', 'NZ', 'PH', 'VN')) OR
                (zm.destination_region = 'China' AND cz.country_code = 'CN') OR
                (zm.destination_region = 'NPAC' AND cz.country_code IN ('JP', 'KR')) OR
                (zm.destination_region = 'Canada' AND cz.country_code = 'CA') OR
                (zm.destination_region = 'Mexico' AND cz.country_code = 'MX') OR
                (zm.destination_region = 'US, AK, HI, PR' AND cz.country_code IN ('US', 'AK', 'HI', 'PR'))
            )
            WHERE zm.origin_country = ? AND cz.country_code = ? AND zm.active = 1
            LIMIT 1
        ''', (origin_country, dest_country))
        
        result = cursor.fetchone()
        return result[0] if result else None

    def _calculate_base_rate(self, service_type: str, zone: str, weight_kg: float, cursor) -> dict:
        """Calculate base shipping rate based on service, zone, and weight"""
        if not weight_kg or weight_kg <= 0:
            return {
                'error': 'Invalid weight',
                'base_cost_usd': 0,
                'rate_applied': 0,
                'weight_bracket': None
            }
            
        # First try to find exact weight match for packages (IP, IE, PAK types)
        cursor.execute('''
            SELECT rate_usd, rate_type, weight_from, weight_to FROM fedex_rate_cards
            WHERE service_type = ? AND zone_code = ? AND weight_from = ? AND weight_to = ?
            AND rate_type IN ('IP', 'IE', 'PAK', 'OL')
            ORDER BY rate_type
            LIMIT 1
        ''', (service_type, zone, weight_kg, weight_kg))
        
        rate_row = cursor.fetchone()
        
        if not rate_row:
            # Try to find weight range for heavyweight packages (IPKG, IEKG)
            cursor.execute('''
                SELECT rate_usd, rate_type, weight_from, weight_to FROM fedex_rate_cards
                WHERE service_type = ? AND zone_code = ? 
                AND weight_from <= ? AND weight_to >= ?
                AND rate_type LIKE '%PKG'
                ORDER BY weight_from
                LIMIT 1
            ''', (service_type, zone, weight_kg, weight_kg))
            
            rate_row = cursor.fetchone()
            
            if rate_row:
                # For heavyweight, multiply rate per kg by actual weight
                rate_usd, rate_type, weight_from, weight_to = rate_row
                total_cost = rate_usd * weight_kg
                
                return {
                    'service_type': service_type,
                    'zone': zone,
                    'chargeable_weight': weight_kg,
                    'base_cost_usd': round(total_cost, 2),
                    'rate_applied': rate_usd,
                    'weight_bracket': f"{weight_from}-{weight_to}kg",
                    'rate_type': rate_type
                }
            
        if not rate_row:
            return {
                'error': f'No rate found for service {service_type}, zone {zone}, weight {weight_kg}kg',
                'base_cost_usd': 0,
                'rate_applied': 0,
                'weight_bracket': None
            }
            
        rate_usd, rate_type, weight_from, weight_to = rate_row
        
        return {
            'service_type': service_type,
            'zone': zone,
            'chargeable_weight': weight_kg,
            'weight_bracket': f"{weight_from}-{weight_to}kg",
            'base_rate_usd': rate_usd,
            'base_cost_usd': rate_usd,
            'rate_applied': rate_usd,
            'rate_type': rate_type,
            'calculation_method': f'Fixed rate {rate_usd} USD for {weight_kg}kg package'
        }

    def _calculate_fuel_surcharge(self, base_cost_usd: float, cursor) -> dict:
        """Calculate FedEx fuel surcharge based on US FSC index"""
        # Get current fuel surcharge rate from FedEx surcharges table
        cursor.execute('''
            SELECT rate_value FROM fedex_surcharges
            WHERE surcharge_code = 'FUEL' AND active = 1
            LIMIT 1
        ''')
        
        fuel_rate_row = cursor.fetchone()
        fuel_rate = fuel_rate_row[0] / 100 if fuel_rate_row else 0.155  # Convert percentage to decimal, default 15.5%
        
        # For FedEx, fuel surcharge is applied to base rate + applicable transportation surcharges
        fuel_cost = base_cost_usd * fuel_rate
        
        return {
            'base_cost_usd': base_cost_usd,
            'fuel_surcharge_rate': fuel_rate,
            'fuel_cost_usd': fuel_cost,
            'calculation_method': f'{fuel_rate * 100:.1f}% of base cost (FedEx US FSC index)',
            'note': 'Applied to net package rate plus applicable transportation-related surcharges'
        }

    def _calculate_fedex_surcharges(self, origin: str, dest: str, weight_kg: float, 
                                   declared_value: float, service_type: str, cursor) -> dict:
        """Calculate FedEx-specific surcharges"""
        surcharges = []
        total_surcharge = 0.0
        
        # Get all active FedEx surcharges
        cursor.execute('''
            SELECT surcharge_code, surcharge_name, rate_type, rate_value, 
                   minimum_charge, maximum_charge, applies_to_service
            FROM fedex_surcharges 
            WHERE active = 1 AND surcharge_code != 'FUEL'
        ''')
        
        surcharge_rules = cursor.fetchall()
        
        for rule in surcharge_rules:
            code, name, rate_type, rate_value, min_charge, max_charge, applies_to = rule
            
            # Skip if doesn't apply to this service type
            if applies_to != 'ALL' and service_type not in applies_to:
                continue
                
            surcharge_amount = 0.0
            calculation_note = ""
            
            if rate_type == 'FIXED':
                surcharge_amount = rate_value
                calculation_note = f"Fixed rate: ${rate_value}"
                
            elif rate_type == 'WEIGHT_OR_FIXED':
                weight_based = weight_kg * rate_value
                surcharge_amount = max(weight_based, min_charge or 0)
                calculation_note = f"Greater of ${min_charge} or ${rate_value}/kg ({weight_kg}kg) = ${surcharge_amount}"
                
            elif rate_type == 'VALUE_OR_WEIGHT':
                if declared_value > 0:
                    value_based = (declared_value / 100) * rate_value  # rate_value is per $100
                    weight_based = weight_kg * min_charge if min_charge else 0
                    surcharge_amount = max(value_based, weight_based)
                    calculation_note = f"Greater of ${value_based:.2f} (value-based) or ${weight_based:.2f} (weight-based)"
                else:
                    surcharge_amount = 0
                    calculation_note = "No declared value"
                    
            elif rate_type == 'PERCENTAGE':
                # This would be applied to base rate, but we'll calculate it separately
                surcharge_amount = 0
                calculation_note = f"{rate_value}% (calculated separately)"
                
            elif rate_type == 'VARIABLE':
                # These are determined by FedEx based on current conditions
                surcharge_amount = 0
                calculation_note = "Variable rate - determined by FedEx"
            
            if surcharge_amount > 0:
                # Apply maximum charge limit if specified
                if max_charge and surcharge_amount > max_charge:
                    surcharge_amount = max_charge
                    calculation_note += f" (capped at ${max_charge})"
                
                surcharges.append({
                    'code': code,
                    'name': name,
                    'amount': surcharge_amount,
                    'calculation': calculation_note
                })
                total_surcharge += surcharge_amount
        
        return {
            'individual_surcharges': surcharges,
            'total_surcharge_usd': total_surcharge,
            'surcharge_count': len(surcharges),
            'calculation_method': 'FedEx surcharge rules applied'
        }

    def _ensure_audit_tables(self, conn):
        """Ensure audit results table exists"""
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS fedex_audit_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_no TEXT NOT NULL,
            awb_number TEXT NOT NULL,
            audit_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            invoice_amount REAL,
            expected_amount REAL,
            variance REAL,
            variance_percentage REAL,
            audit_status TEXT,
            zone_applied TEXT,
            rate_applied REAL,
            fuel_surcharge_expected REAL,
            vat_expected REAL,
            audit_details TEXT,
            UNIQUE(invoice_no, awb_number)
        )''')
        conn.commit()


    # helper functions defined above class
