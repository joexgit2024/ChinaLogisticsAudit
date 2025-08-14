"""FedEx Audit Engine - initial scaffold."""
from __future__ import annotations
from typing import Dict, Any
import sqlite3, json, re

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


    # helper functions defined above class
