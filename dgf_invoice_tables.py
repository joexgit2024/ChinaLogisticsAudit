import os
import sqlite3
from typing import Optional
import logging

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.getcwd(), 'dhl_audit.db')

class DGFInvoiceTables:
    """Create and manage DGF invoice tables (minimal, invoice-fields only)."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or DB_PATH
        self.create_invoice_tables()

    def create_invoice_tables(self, force_recreate: bool = False):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        # Detect existing schema
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dgf_air_invoices'")
        exists = cur.fetchone() is not None
        needs_rebuild = False
        if exists:
            try:
                cur.execute("PRAGMA table_info('dgf_air_invoices')")
                cols = cur.fetchall()  # cid, name, type, notnull, dflt_value, pk
                colnames = {c[1] for c in cols}
                notnull_map = {c[1]: c[3] for c in cols}
                # If lane_id missing or quote_id is NOT NULL, we rebuild to new schema
                if 'lane_id' not in colnames or notnull_map.get('quote_id', 0) == 1:
                    needs_rebuild = True
            except Exception:
                needs_rebuild = True
        if force_recreate or needs_rebuild:
            cur.execute('DROP TABLE IF EXISTS dgf_air_invoices')
            logger.info('Dropped dgf_air_invoices (rebuild schema)')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS dgf_air_invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lane_id TEXT NOT NULL,
                quote_id TEXT,
                actual_arrival_date DATE,
                hbl_number TEXT,
                pieces INTEGER,
                gross_weight REAL,
                chargeable_weight REAL,
                terms TEXT,
                origin_country TEXT,
                origin_port TEXT,
                freight REAL,
                origin_currency TEXT,
                origin_fx_rate REAL,
                destination_charges REAL,
                destination_currency TEXT,
                destination_fx_rate REAL,
                total_cny REAL,
                file_name TEXT,
                sheet_name TEXT,
                row_number INTEGER,
                uploaded_by TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'ACTIVE',
                UNIQUE(lane_id)
            )
        ''')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_air_inv_lane ON dgf_air_invoices(lane_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_air_inv_quote ON dgf_air_invoices(quote_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_air_inv_date ON dgf_air_invoices(actual_arrival_date)')
        conn.commit()
        # Create SEA FCL and LCL invoice tables (same minimal schema)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS dgf_fcl_invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lane_id TEXT NOT NULL,
                quote_id TEXT,
                actual_arrival_date DATE,
                hbl_number TEXT,
                pieces INTEGER,
                gross_weight REAL,
                chargeable_weight REAL,
                terms TEXT,
                origin_country TEXT,
                origin_port TEXT,
                freight REAL,
                origin_currency TEXT,
                origin_fx_rate REAL,
                destination_charges REAL,
                destination_currency TEXT,
                destination_fx_rate REAL,
                total_cny REAL,
                file_name TEXT,
                sheet_name TEXT,
                row_number INTEGER,
                uploaded_by TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'ACTIVE',
                UNIQUE(lane_id)
            )
        ''')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_fcl_inv_lane ON dgf_fcl_invoices(lane_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_fcl_inv_quote ON dgf_fcl_invoices(quote_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_fcl_inv_date ON dgf_fcl_invoices(actual_arrival_date)')

        cur.execute('''
            CREATE TABLE IF NOT EXISTS dgf_lcl_invoices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                lane_id TEXT NOT NULL,
                quote_id TEXT,
                actual_arrival_date DATE,
                hbl_number TEXT,
                pieces INTEGER,
                gross_weight REAL,
                chargeable_weight REAL,
                terms TEXT,
                origin_country TEXT,
                origin_port TEXT,
                freight REAL,
                origin_currency TEXT,
                origin_fx_rate REAL,
                destination_charges REAL,
                destination_currency TEXT,
                destination_fx_rate REAL,
                total_cny REAL,
                file_name TEXT,
                sheet_name TEXT,
                row_number INTEGER,
                uploaded_by TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'ACTIVE',
                UNIQUE(lane_id)
            )
        ''')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_lcl_inv_lane ON dgf_lcl_invoices(lane_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_lcl_inv_quote ON dgf_lcl_invoices(quote_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_lcl_inv_date ON dgf_lcl_invoices(actual_arrival_date)')

        conn.commit()
        conn.close()
        logger.info('DGF invoice tables created successfully')
