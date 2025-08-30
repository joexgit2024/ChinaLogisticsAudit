import os
import sqlite3
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional

class DGFInvoiceProcessor:
    """Process DGF invoice Excel files and load into DB (AIR/FCL/LCL)."""

    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path

    # Header candidates for each logical field (supports English/Chinese variants)
    HEADERS = {
    'lane_id': ['lane id/fqr#', 'lane id', 'fqr#', 'fqr id', 'lane id fqr#'],
    'quote_id': ['报价单号', 'quote id', 'quoteid', 'quote no', 'quote number', 'quote ref', 'quote ref#', 'quote reference', 'quote reference no'],
    'actual_arrival_date': ['实际到港日期', 'actual arrival date', 'arrival date', 'ata', 'actual arrival', 'eta'],
    'hbl_number': ['分单号', 'hbl number', 'hbl no', 'hbl', 'hawb', 'hawb no', 'hawb number', 'awb', 'awb no', 'awb number'],
        'pieces': ['件数', 'pieces', 'pkgs', 'packages'],
        'gross_weight': ['毛重', 'gross weight', 'gw'],
        'chargeable_weight': ['计费重', 'chargeable weight', 'cw', 'chargeable wt'],
        'terms': ['条款', 'terms', 'incoterms'],
        'origin_country': ['发货国', 'origin country', 'origin ctry'],
    'origin_port': ['发货港', 'origin port', 'pol', 'port of loading', 'origin airport', 'departure airport', 'aol', 'airport of loading'],
        'freight': ['港到港', 'freight', 'port to port', 'p2p freight', 'ocean freight', 'air freight'],
    'origin_currency': ['到港币种', 'origin currency', 'origin curr', 'freight currency'],
    'origin_fx_rate': ['到港汇率', 'origin fx rate', 'origin exchange rate', 'freight fx rate', 'fx rate (origin)'],
        'destination_charges': ['目的港费用', 'destination charges', 'dest charges', 'destination charge'],
        'destination_currency': ['目的港币种', 'destination currency', 'dest currency'],
    'destination_fx_rate': ['目的港汇率', '目的港汇率j', 'destination fx rate', 'destination exchange rate', 'dest fx rate', 'destination fx rate (j)'],
    'total_cny': ['总计', 'total (cny)', 'total cny', 'total amount cny', 'grand total (cny)', 'grand total cny', 'total'],
    }

    @staticmethod
    def _norm_key(k: Any) -> str:
        try:
            return str(k).strip().lower()
        except Exception:
            return ''

    def _norm_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {self._norm_key(k): v for k, v in row.items()}

    def pick(self, nrow: Dict[str, Any], candidates: List[str], default=None):
        # 1) exact key match
        for c in candidates:
            v = nrow.get(self._norm_key(c))
            if v is not None and v != '':
                return v
        # 2) loose contains match on keys
        keys = list(nrow.keys())
        for c in candidates:
            needle = self._norm_key(c)
            for k in keys:
                if needle and needle in k:
                    v = nrow.get(k)
                    if v is not None and v != '':
                        return v
        return default

    def pick_float(self, nrow: Dict[str, Any], candidates: List[str]) -> Optional[float]:
        v = self.pick(nrow, candidates, None)
        try:
            if v is None or v == '':
                return None
            if isinstance(v, str):
                v = v.replace(',', '').strip()
            return float(v)
        except Exception:
            return None

    def _candidate_set(self) -> set:
        return {self._norm_key(x) for vals in self.HEADERS.values() for x in vals}

    def _ensure_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        """If DataFrame columns don't resemble expected headers, try to lift a header row from the first few rows."""
        try:
            cand = self._candidate_set()
            cols_norm = {self._norm_key(c) for c in df.columns}
            # If we already match at least 3 known header names, keep as-is
            if len(cand.intersection(cols_norm)) >= 3:
                return df

            # Try first 10 rows to locate a header-like row
            max_rows = min(10, len(df))
            for header_row_idx in range(max_rows):
                row_vals = [self._norm_key(v) for v in list(df.iloc[header_row_idx].values)]
                row_set = set(row_vals)
                if len(cand.intersection(row_set)) >= 3:
                    # Promote this row to header
                    df2 = df.copy()
                    df2.columns = df2.iloc[header_row_idx]
                    df2 = df2.iloc[header_row_idx + 1:]
                    df2 = df2.reset_index(drop=True)
                    return df2
        except Exception:
            pass
        return df

    def _process_invoice_file(self, table: str, file_path: str, uploaded_by: str, replace_existing: bool = True) -> Dict[str, Any]:
        result = {"success": 0, "errors": 0, "messages": []}
        if not os.path.exists(file_path):
            return {"success": 0, "errors": 1, "messages": [f"File not found: {file_path}"]}

        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        try:
            xls = pd.ExcelFile(file_path)
            # Iterate sheets and rows
            for sheet in xls.sheet_names:
                df = xls.parse(sheet)
                df = self._ensure_headers(df)
                records = df.to_dict(orient='records')
                for i, row in enumerate(records):
                    try:
                        nrow = self._norm_row(row)
                        lane_id = self.pick(nrow, self.HEADERS['lane_id'])
                        quote_id = self.pick(nrow, self.HEADERS['quote_id'])
                        arrival = self.pick(nrow, self.HEADERS['actual_arrival_date'])
                        if isinstance(arrival, pd.Timestamp):
                            arrival = arrival.strftime('%Y-%m-%d')
                        elif isinstance(arrival, str) and arrival.strip():
                            try:
                                parsed = pd.to_datetime(arrival, errors='coerce')
                                arrival = parsed.strftime('%Y-%m-%d') if not pd.isna(parsed) else None
                            except Exception:
                                arrival = None
                        hbl = self.pick(nrow, self.HEADERS['hbl_number'])
                        pieces = self.pick_float(nrow, self.HEADERS['pieces'])
                        gross = self.pick_float(nrow, self.HEADERS['gross_weight'])
                        chargeable = self.pick_float(nrow, self.HEADERS['chargeable_weight'])
                        terms = self.pick(nrow, self.HEADERS['terms'])
                        o_country = self.pick(nrow, self.HEADERS['origin_country'])
                        o_port = self.pick(nrow, self.HEADERS['origin_port'])
                        freight = self.pick_float(nrow, self.HEADERS['freight'])
                        o_curr = self.pick(nrow, self.HEADERS['origin_currency'])
                        o_fx = self.pick_float(nrow, self.HEADERS['origin_fx_rate'])
                        d_charges = self.pick_float(nrow, self.HEADERS['destination_charges'])
                        d_curr = self.pick(nrow, self.HEADERS['destination_currency'])
                        d_fx = self.pick_float(nrow, self.HEADERS['destination_fx_rate'])
                        total_cny = self.pick_float(nrow, self.HEADERS['total_cny'])

                        if not (lane_id or hbl or quote_id):
                            # Skip empty rows
                            continue

                        # Ensure lane_id exists; try to synthesize from HBL if needed
                        if not lane_id and hbl:
                            lane_id = f"HBL:{hbl}"
                            result.setdefault('messages', []).append(
                                f"Sheet '{sheet}' row {i+1}: Lane ID missing; using fallback '{lane_id}'."
                            )

                        insert_mode = 'INSERT OR REPLACE' if replace_existing else 'INSERT OR IGNORE'
                        cur.execute(f'''{insert_mode} INTO {table} (
                                lane_id, quote_id, actual_arrival_date, hbl_number, pieces,
                                gross_weight, chargeable_weight, terms, origin_country, origin_port,
                                freight, origin_currency, origin_fx_rate, destination_charges,
                                destination_currency, destination_fx_rate, total_cny,
                                file_name, sheet_name, row_number, uploaded_by
                            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        ''', (
                            lane_id, quote_id, arrival, hbl, pieces, gross, chargeable, terms,
                            o_country, o_port, freight, o_curr, o_fx, d_charges,
                            d_curr, d_fx, total_cny, os.path.basename(file_path), sheet, i+1, uploaded_by
                        ))
                        result['success'] += 1
                    except Exception as e:
                        result['errors'] += 1
                        result['messages'].append(f"Sheet '{sheet}' row {i+1}: {type(e).__name__}: {e}")
            conn.commit()
        finally:
            conn.close()
        return result

    def process_air_invoice_file(self, file_path: str, uploaded_by: str, replace_existing: bool = True) -> Dict[str, Any]:
        return self._process_invoice_file('dgf_air_invoices', file_path, uploaded_by, replace_existing)

    def process_fcl_invoice_file(self, file_path: str, uploaded_by: str, replace_existing: bool = True) -> Dict[str, Any]:
        return self._process_invoice_file('dgf_fcl_invoices', file_path, uploaded_by, replace_existing)

    def process_lcl_invoice_file(self, file_path: str, uploaded_by: str, replace_existing: bool = True) -> Dict[str, Any]:
        return self._process_invoice_file('dgf_lcl_invoices', file_path, uploaded_by, replace_existing)
