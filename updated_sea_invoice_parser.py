def save_sea_invoices_updated(self, file_path: str):
    """Save sea freight invoices from Excel file - Updated for DGF-CN10 billing.xlsx format."""
    try:
        # Try to read the Excel file - check if it has multiple sheets
        xl = pd.ExcelFile(file_path)
        sheet_names = xl.sheet_names
        print(f"Available sheets: {sheet_names}")
        
        # Use the first sheet or look for a specific sheet name
        df = pd.read_excel(file_path, sheet_name=0)
        
        print(f"Columns in the file: {list(df.columns)}")
        print(f"First few rows:")
        print(df.head())
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        saved_count = 0
        
        for index, row in df.iterrows():
            try:
                # Skip rows without invoice number
                invoice_no = str(row.get('invoice No.', '')).strip()
                if pd.isna(invoice_no) or invoice_no == '' or invoice_no == 'nan':
                    continue
                
                # Extract values based on the new DGF-CN10 format
                lane_id_fqr = str(row.get('Lane ID/FQR#', '')).strip()
                ata_date = row.get('ATA date')
                hbl = str(row.get('HBL', '')).strip() 
                pkg_no = int(row.get('PKG No', 0)) if pd.notna(row.get('PKG No')) else 0
                m3 = float(row.get('M3', 0)) if pd.notna(row.get('M3')) else 0
                container = str(row.get('Container', '')).strip()
                inco_term = str(row.get('Inco-term', '')).strip()
                origin_country = str(row.get('Origin Country', '')).strip()
                origin_port = str(row.get('Origin Port', '')).strip()
                currency_dta_dtp = str(row.get('Currency(DTA/DTP)', 'USD')).strip()
                
                # Charges
                pickup = float(row.get('Pickup', 0)) if pd.notna(row.get('Pickup')) else 0
                dtp_handling = float(row.get('DTP-Handling', 0)) if pd.notna(row.get('DTP-Handling')) else 0
                customs = float(row.get('Customs', 0)) if pd.notna(row.get('Customs')) else 0
                dtp_others = float(row.get('DTP-Others', 0)) if pd.notna(row.get('DTP-Others')) else 0
                ptp = float(row.get('PTP', 0)) if pd.notna(row.get('PTP')) else 0
                imo = float(row.get('IMO', 0)) if pd.notna(row.get('IMO')) else 0
                
                # Subtotals and exchange rates
                subtotal_dta_dtp = float(row.get('Sub-total(DTA/DTP)', 0)) if pd.notna(row.get('Sub-total(DTA/DTP)')) else 0
                exchange_rate_dta_dtp = float(row.get('Exchange rate(DTA/DTP)', 1)) if pd.notna(row.get('Exchange rate(DTA/DTP)')) else 1
                subtotal_cny_dta_dtp = float(row.get('Sub-total CNY(DTA/DTP)', 0)) if pd.notna(row.get('Sub-total CNY(DTA/DTP)')) else 0
                
                # Destination charges
                currency_dc = str(row.get('Currency(DC)', 'CNY')).strip()
                doc_turnover = float(row.get('Doc Turnover', 0)) if pd.notna(row.get('Doc Turnover')) else 0
                others = float(row.get('Others', 0)) if pd.notna(row.get('Others')) else 0
                subtotal_dc = float(row.get('Sub-total', 0)) if pd.notna(row.get('Sub-total')) else 0
                fx_rate_dc = float(row.get('FX Rate(DC)', 1)) if pd.notna(row.get('FX Rate(DC)')) else 1
                subtotal_cny_dc = float(row.get('Sub-total CNY', 0)) if pd.notna(row.get('Sub-total CNY')) else 0
                total_cny = float(row.get('Total CNY', 0)) if pd.notna(row.get('Total CNY')) else 0
                
                # Status and tax info
                trax_status = str(row.get('Trax status', '')).strip()
                tax_invoice = str(row.get('税票', '')).strip()
                
                cursor.execute('''
                    INSERT OR REPLACE INTO dgf_invoices 
                    (invoice_number, lane_id_fqr, ata_date, hbl_number, pkg_no, m3_volume,
                     container_info, inco_term, origin_country, origin_port, 
                     origin_currency, pickup_charge, dtp_handling_charge, customs_charge,
                     dtp_others_charge, ptp_charge, imo_charge, subtotal_dta_dtp,
                     exchange_rate_dta_dtp, subtotal_cny_dta_dtp, currency_dc,
                     doc_turnover, others_charge, subtotal_dc, fx_rate_dc,
                     subtotal_cny_dc, total_cny, trax_status, tax_invoice_number,
                     mode, file_path, processed_at, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    invoice_no, lane_id_fqr, ata_date, hbl, pkg_no, m3,
                    container, inco_term, origin_country, origin_port,
                    currency_dta_dtp, pickup, dtp_handling, customs,
                    dtp_others, ptp, imo, subtotal_dta_dtp,
                    exchange_rate_dta_dtp, subtotal_cny_dta_dtp, currency_dc,
                    doc_turnover, others, subtotal_dc, fx_rate_dc,
                    subtotal_cny_dc, total_cny, trax_status, tax_invoice,
                    'SEA', file_path, datetime.now(), 'PROCESSED'
                ))
                
                saved_count += 1
                print(f"Saved invoice: {invoice_no}")
                
            except Exception as e:
                print(f"Error processing row {index}: {e}")
                continue
        
        conn.commit()
        conn.close()
        print(f"Successfully saved {saved_count} sea invoices from {file_path}")
        
    except Exception as e:
        print(f"Error saving sea invoices: {e}")
        raise
