import sqlite3
from datetime import datetime

def update_dgf_table_structure():
    conn = sqlite3.connect('dhl_audit.db')
    cursor = conn.cursor()
    
    print("Updating DGF invoice table structure...")
    
    # Add missing columns to dgf_invoices table
    missing_columns = [
        ('lane_id_fqr', 'TEXT'),  # Lane ID/FQR#
        ('ata_date', 'DATE'),     # ATA date  
        ('pkg_no', 'INTEGER'),    # PKG No
        ('m3_volume', 'DECIMAL(10,2)'),  # M3
        ('inco_term', 'TEXT'),    # Inco-term
        ('pickup_charge', 'DECIMAL(10,2)'),      # Pickup
        ('dtp_handling_charge', 'DECIMAL(10,2)'), # DTP-Handling
        ('customs_charge', 'DECIMAL(10,2)'),      # Customs
        ('dtp_others_charge', 'DECIMAL(10,2)'),   # DTP-Others
        ('ptp_charge', 'DECIMAL(10,2)'),          # PTP
        ('imo_charge', 'DECIMAL(10,2)'),          # IMO
        ('subtotal_dta_dtp', 'DECIMAL(10,2)'),    # Sub-total(DTA/DTP)
        ('exchange_rate_dta_dtp', 'DECIMAL(10,6)'), # Exchange rate(DTA/DTP)
        ('subtotal_cny_dta_dtp', 'DECIMAL(10,2)'),  # Sub-total CNY(DTA/DTP)
        ('currency_dc', 'TEXT'),                    # Currency(DC)
        ('doc_turnover', 'DECIMAL(10,2)'),          # Doc Turnover
        ('others_charge', 'DECIMAL(10,2)'),         # Others
        ('subtotal_dc', 'DECIMAL(10,2)'),           # Sub-total
        ('fx_rate_dc', 'DECIMAL(10,6)'),            # FX Rate(DC)
        ('subtotal_cny_dc', 'DECIMAL(10,2)'),       # Sub-total CNY
        ('trax_status', 'TEXT'),                    # Trax status
        ('tax_invoice_number', 'TEXT'),             # 税票 (Tax invoice number)
    ]
    
    for column_name, column_type in missing_columns:
        try:
            cursor.execute(f'ALTER TABLE dgf_invoices ADD COLUMN {column_name} {column_type}')
            print(f"Added column: {column_name} ({column_type})")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                print(f"Column {column_name} already exists, skipping...")
            else:
                print(f"Error adding column {column_name}: {e}")
    
    # Update existing column mappings where needed
    print("\nColumn mappings:")
    print("lane_id_fqr -> Lane ID/FQR#")
    print("ata_date -> ATA date") 
    print("hbl_number -> HBL")
    print("pkg_no -> PKG No")
    print("m3_volume -> M3")
    print("container_info -> Container")
    print("inco_term -> Inco-term")
    print("origin_country -> Origin Country")
    print("origin_port -> Origin Port")
    print("origin_currency -> Currency(DTA/DTP)")
    print("pickup_charge -> Pickup")
    print("dtp_handling_charge -> DTP-Handling")
    print("customs_charge -> Customs")
    print("dtp_others_charge -> DTP-Others")
    print("ptp_charge -> PTP")
    print("imo_charge -> IMO")
    print("subtotal_dta_dtp -> Sub-total(DTA/DTP)")
    print("exchange_rate_dta_dtp -> Exchange rate(DTA/DTP)")
    print("subtotal_cny_dta_dtp -> Sub-total CNY(DTA/DTP)")
    print("currency_dc -> Currency(DC)")
    print("doc_turnover -> Doc Turnover")
    print("others_charge -> Others")
    print("subtotal_dc -> Sub-total")
    print("fx_rate_dc -> FX Rate(DC)")
    print("subtotal_cny_dc -> Sub-total CNY")
    print("total_cny -> Total CNY")
    print("invoice_number -> invoice No.")
    print("trax_status -> Trax status")
    print("tax_invoice_number -> 税票")
    
    conn.commit()
    conn.close()
    print("\nDGF table structure updated successfully!")

if __name__ == "__main__":
    update_dgf_table_structure()
