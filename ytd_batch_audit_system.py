#!/usr/bin/env python3
"""
YTD Batch Audit System
Comprehensive audit system for YTD invoice processing

This module is now a thin wrapper around the modular ytd_audit package.
"""

# Import from modular package
from ytd_audit import YTDBatchAuditSystem as ModularYTDBatchAuditSystem


class YTDBatchAuditSystem(ModularYTDBatchAuditSystem):
    """YTD Batch Audit System for processing invoices - Legacy wrapper"""
    
    def __init__(self, db_path="dhl_audit.db"):
        """Initialize the batch audit system"""
        super().__init__(db_path)
        
    def get_db_connection(self):
        """Legacy method for backward compatibility
        
        Returns a database connection directly for code that hasn't been updated
        to use the DatabaseManager interface.
        """
        return self.db.get_connection()


# For backward compatibility, create instance for direct use
ytd_audit_system = YTDBatchAuditSystem()

# Re-export core methods for backward compatibility
run_full_ytd_audit = ytd_audit_system.run_full_ytd_audit
run_batch_audit = ytd_audit_system.run_batch_audit
audit_single_invoice = ytd_audit_system.audit_single_invoice
audit_single_invoice_comprehensive = ytd_audit_system.audit_single_invoice_comprehensive
get_audit_summary = ytd_audit_system.get_audit_summary
get_existing_audit_results = ytd_audit_system.get_existing_audit_results
delete_batch = ytd_audit_system.delete_batch


if __name__ == "__main__":
    # Test the system
    system = YTDBatchAuditSystem()
    print("YTD Batch Audit System initialized successfully")
    
    def ensure_tables_exist(self):
        """Ensure required tables exist"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        # Create batch audit runs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ytd_batch_audit_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_name TEXT NOT NULL,
                status TEXT DEFAULT 'running',
                total_invoices INTEGER DEFAULT 0,
                invoices_passed INTEGER DEFAULT 0,
                invoices_warned INTEGER DEFAULT 0,
                invoices_failed INTEGER DEFAULT 0,
                invoices_error INTEGER DEFAULT 0,
                processing_time_ms INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP
            )
        ''')
        
        # Create audit results table with batch_run_id
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ytd_audit_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_run_id INTEGER,
                invoice_no TEXT NOT NULL,
                audit_status TEXT NOT NULL,
                transportation_mode TEXT,
                total_invoice_amount REAL DEFAULT 0,
                total_expected_amount REAL DEFAULT 0,
                total_variance REAL DEFAULT 0,
                variance_percent REAL DEFAULT 0,
                rate_cards_checked INTEGER DEFAULT 0,
                matching_lanes INTEGER DEFAULT 0,
                best_match_rate_card TEXT,
                audit_details TEXT,
                processing_time_ms INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (batch_run_id) REFERENCES ytd_batch_audit_runs(id)
            )
        ''')
        
        conn.commit()
        conn.close()
    
    def get_audit_summary(self):
        """Get audit summary statistics"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Get total results with better error handling
            cursor.execute("SELECT COUNT(*) FROM ytd_audit_results")
            result = cursor.fetchone()
            total_results = int(result[0]) if result and result[0] is not None else 0
            
            # Get total batch runs with better error handling
            cursor.execute("SELECT COUNT(*) FROM ytd_batch_audit_runs")
            result = cursor.fetchone()
            total_batch_runs = int(result[0]) if result and result[0] is not None else 0
            
            # Get recent batches and convert to dictionaries
            cursor.execute("""
                SELECT id, run_name, status, total_invoices, invoices_passed, 
                       invoices_warned, invoices_failed, invoices_error, created_at
                FROM ytd_batch_audit_runs 
                ORDER BY created_at DESC 
                LIMIT 10
            """)
            recent_batches_raw = cursor.fetchall()
            
            # Convert tuples to dictionaries with better null handling
            recent_batches = []
            for batch in recent_batches_raw:
                recent_batches.append({
                    'id': int(batch[0]) if batch[0] is not None else 0,
                    'run_name': str(batch[1]) if batch[1] is not None else 'Unknown',
                    'status': str(batch[2]) if batch[2] is not None else 'unknown',
                    'total_invoices': int(batch[3]) if batch[3] is not None else 0,
                    'invoices_passed': int(batch[4]) if batch[4] is not None else 0,
                    'invoices_warned': int(batch[5]) if batch[5] is not None else 0,
                    'invoices_failed': int(batch[6]) if batch[6] is not None else 0,
                    'invoices_error': int(batch[7]) if batch[7] is not None else 0,
                    'created_at': str(batch[8]) if batch[8] is not None else ''
                })
            
            # Get status counts with better error handling and status mapping
            cursor.execute("""
                SELECT audit_status, COUNT(*) 
                FROM ytd_audit_results 
                GROUP BY audit_status
            """)
            status_counts_raw = cursor.fetchall()
            status_counts = {}
            
            # Map database statuses to display statuses
            status_mapping = {
                'approved': 'APPROVED',
                'review_required': 'REVIEW_REQUIRED', 
                'rejected': 'REJECTED',
                'error': 'ERROR',
                'pass': 'APPROVED',  # Legacy status mapping
                'warning': 'REVIEW_REQUIRED',  # Legacy status mapping
                'fail': 'REJECTED'  # Legacy status mapping
            }
            
            for status, count in status_counts_raw:
                if status and count is not None:
                    # Map to display status, defaulting to original if not found
                    display_status = status_mapping.get(str(status).lower(), str(status).upper())
                    
                    # Aggregate counts for mapped statuses
                    if display_status in status_counts:
                        status_counts[display_status] += int(count)
                    else:
                        status_counts[display_status] = int(count)
            
            # Get latest batch statistics for dashboard
            latest_batch_stats = {}
            if recent_batches:
                latest_batch = recent_batches[0]  # First batch is the most recent
                latest_batch_stats = {
                    'latest_total': latest_batch.get('total_invoices', 0),
                    'latest_passed': latest_batch.get('invoices_passed', 0),
                    'latest_warned': latest_batch.get('invoices_warned', 0),
                    'latest_failed': latest_batch.get('invoices_failed', 0),
                    'latest_error': latest_batch.get('invoices_error', 0),
                    'latest_batch_name': latest_batch.get('run_name', 'No recent batch'),
                    'latest_created_at': latest_batch.get('created_at', '')
                }
            
            return {
                'total_results': total_results,
                'total_batch_runs': total_batch_runs,
                'recent_batches': recent_batches,
                'status_counts': status_counts,
                'latest_batch_stats': latest_batch_stats
            }
            
        except Exception as e:
            print(f"Error getting audit summary: {e}")
            import traceback
            print(traceback.format_exc())
            return {
                'total_results': 0,
                'total_batch_runs': 0,
                'recent_batches': [],
                'status_counts': {}
            }
        finally:
            conn.close()
    
    def run_full_ytd_audit(self, force_reaudit=False, batch_name=None, detailed_analysis=True):
        """Run comprehensive audit on all YTD invoices"""
        if not batch_name:
            batch_name = f"YTD Full Audit - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Create batch run record
            cursor.execute("""
                INSERT INTO ytd_batch_audit_runs (run_name, status, created_at)
                VALUES (?, 'running', ?)
            """, (batch_name, datetime.now()))
            
            batch_run_id = cursor.lastrowid
            conn.commit()
            
            # Get all invoices from YTD tables
            invoices = self.get_all_ytd_invoices()
            total_invoices = len(invoices)
            
            if total_invoices == 0:
                # Update batch as completed but with no invoices
                cursor.execute("""
                    UPDATE ytd_batch_audit_runs 
                    SET status = 'completed', total_invoices = 0, end_time = ?
                    WHERE id = ?
                """, (datetime.now(), batch_run_id))
                conn.commit()
                
                return {
                    'batch_run_id': batch_run_id,
                    'total_invoices': 0,
                    'status': 'completed',
                    'message': 'No invoices found in YTD tables'
                }
            
            # If force_reaudit is True, delete existing results for these invoices
            if force_reaudit:
                invoice_list = "', '".join([inv[0] for inv in invoices])
                cursor.execute(f"""
                    DELETE FROM ytd_audit_results 
                    WHERE invoice_no IN ('{invoice_list}')
                """)
                conn.commit()
            
            # Process invoices
            passed = warned = failed = error = 0
            start_time = datetime.now()
            
            for i, invoice in enumerate(invoices):
                try:
                    result = self.audit_single_invoice_comprehensive(
                        invoice, batch_run_id, detailed_analysis
                    )
                    
                    # Use lowercase status that matches what the audit engines return
                    if result['status'] == 'approved':
                        passed += 1
                    elif result['status'] == 'review_required':
                        warned += 1
                    elif result['status'] == 'rejected':
                        failed += 1
                    else:
                        error += 1
                        
                    # Commit every 50 invoices to avoid large transactions
                    if (i + 1) % 50 == 0:
                        conn.commit()
                        
                except Exception as e:
                    print(f"Error auditing invoice {invoice[0]}: {e}")
                    error += 1
                    # Insert error result
                    try:
                        cursor.execute("""
                            INSERT INTO ytd_audit_results (
                                batch_run_id, invoice_no, audit_status, transportation_mode,
                                total_invoice_amount, total_expected_amount, total_variance,
                                variance_percent, rate_cards_checked, matching_lanes,
                                audit_details, created_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            batch_run_id, invoice[0], 'error', 'unknown',
                            0.0, 0.0, 0.0, 0.0, 0, 0,
                            json.dumps({'error': str(e)}), datetime.now()
                        ))
                    except:
                        pass  # Don't fail on insert error
            
            # Calculate processing time
            end_time = datetime.now()
            processing_time = int((end_time - start_time).total_seconds() * 1000)
            
            # Update batch run with final results
            cursor.execute("""
                UPDATE ytd_batch_audit_runs 
                SET status = 'completed',
                    total_invoices = ?,
                    invoices_passed = ?,
                    invoices_warned = ?,
                    invoices_failed = ?,
                    invoices_error = ?,
                    processing_time_ms = ?,
                    end_time = ?
                WHERE id = ?
            """, (total_invoices, passed, warned, failed, error, 
                  processing_time, end_time, batch_run_id))
            
            conn.commit()
            
            return {
                'batch_run_id': batch_run_id,
                'total_invoices': total_invoices,
                'passed': passed,
                'warned': warned,
                'failed': failed,
                'error': error,
                'processing_time_ms': processing_time,
                'status': 'completed'
            }
            
        except Exception as e:
            print(f"Error in full YTD audit: {e}")
            # Mark batch as failed
            try:
                cursor.execute("""
                    UPDATE ytd_batch_audit_runs 
                    SET status = 'failed', end_time = ?
                    WHERE id = ?
                """, (datetime.now(), batch_run_id))
                conn.commit()
            except:
                pass
            raise
        finally:
            conn.close()
    
    def get_all_ytd_invoices(self):
        """Get all invoices from YTD tables for auditing"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Check which YTD tables exist and get invoices
            table_names = ['dhl_ytd_invoices', 'ytd_invoices', 'invoices']
            all_invoices = []
            
            for table_name in table_names:
                try:
                    cursor.execute(f"""
                        SELECT name FROM sqlite_master 
                        WHERE type='table' AND name=?
                    """, (table_name,))
                    
                    if cursor.fetchone():
                        # Table exists, get invoices with available columns
                        if table_name == 'dhl_ytd_invoices':
                            # Check what columns are available
                            cursor.execute(f"PRAGMA table_info({table_name})")
                            columns = [row[1] for row in cursor.fetchall()]
                            
                            # Build query based on available columns
                            select_cols = ['invoice_no']
                            if 'transportation_mode' in columns:
                                select_cols.append('transportation_mode')
                            else:
                                select_cols.append("'unknown' as transportation_mode")
                                
                            # Map to the correct total amount column - USE USD COLUMNS
                            if 'total_charges_with_duty_tax_usd' in columns:
                                select_cols.append('COALESCE(total_charges_with_duty_tax_usd, 0) as total_amount')
                            elif 'total_charges_without_duty_tax_usd' in columns:
                                select_cols.append('COALESCE(total_charges_without_duty_tax_usd, 0) as total_amount')
                            elif 'total_charges_with_duty_tax' in columns:
                                select_cols.append('COALESCE(total_charges_with_duty_tax, 0) as total_amount')
                            elif 'total_charges_without_duty_tax' in columns:
                                select_cols.append('COALESCE(total_charges_without_duty_tax, 0) as total_amount')
                            elif 'total_amount' in columns:
                                select_cols.append('COALESCE(total_amount, 0) as total_amount')
                            elif 'invoice_amount' in columns:
                                select_cols.append('COALESCE(invoice_amount, 0) as total_amount')
                            else:
                                select_cols.append('0 as total_amount')
                            
                            if 'origin' in columns:
                                select_cols.append('origin')
                            else:
                                select_cols.append("'Unknown' as origin")
                                
                            if 'destination' in columns:
                                select_cols.append('destination')
                            else:
                                select_cols.append("'Unknown' as destination")
                                
                            # Map to the correct weight column
                            if 'shipment_weight_kg' in columns:
                                select_cols.append('COALESCE(shipment_weight_kg, 0) as weight')
                            elif 'total_shipment_chargeable_weight_kg' in columns:
                                select_cols.append('COALESCE(total_shipment_chargeable_weight_kg, 0) as weight')
                            elif 'weight' in columns:
                                select_cols.append('COALESCE(weight, 0) as weight')
                            else:
                                select_cols.append('0 as weight')
                                
                            if 'service_type' in columns:
                                select_cols.append('service_type')
                            else:
                                select_cols.append("'Standard' as service_type")
                            
                            query = f"SELECT {', '.join(select_cols)} FROM {table_name}"
                            cursor.execute(query)
                            invoices = cursor.fetchall()
                            all_invoices.extend(invoices)
                            print(f"Found {len(invoices)} invoices in {table_name}")
                            break  # Use first available table
                            
                except Exception as e:
                    print(f"Error querying table {table_name}: {e}")
                    continue
            
            return all_invoices
            
        except Exception as e:
            print(f"Error getting YTD invoices: {e}")
            return []
        finally:
            conn.close()
    
    def audit_single_invoice_comprehensive(self, invoice_data, batch_run_id=None, detailed_analysis=True):
        """Comprehensive audit of a single invoice with proper rate card matching"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            invoice_no = invoice_data[0]
            transportation_mode = invoice_data[1] if len(invoice_data) > 1 else 'unknown'
            invoice_amount = float(invoice_data[2]) if len(invoice_data) > 2 and invoice_data[2] else 0.0
            origin = invoice_data[3] if len(invoice_data) > 3 else 'Unknown'
            destination = invoice_data[4] if len(invoice_data) > 4 else 'Unknown'
            weight = float(invoice_data[5]) if len(invoice_data) > 5 and invoice_data[5] else 0.0
            service_type = invoice_data[6] if len(invoice_data) > 6 else 'Standard'
            
            # Use ocean freight audit engine for sea/ocean shipments
            if transportation_mode.lower() in ['sea', 'ocean', 'lcl', 'fcl', 'maritime']:
                audit_result = self.ocean_audit_engine.audit_ocean_freight_invoice(invoice_no)
                
                if audit_result.get('audit_status') == 'error':
                    # Fallback to old calculation if ocean audit fails
                    return self._fallback_audit_calculation(invoice_data, batch_run_id, detailed_analysis)
                
                # Get invoice details for volume
                invoice_details_from_audit = audit_result.get('invoice_data', {})
                volume_m3 = invoice_details_from_audit.get('volume_m3', 0)
                
                # Extract results from ocean audit engine (updated fields)
                status = audit_result.get('audit_status', 'error')
                
                # Get expected amount from invoice_data section
                invoice_info = audit_result.get('invoice_data', {})
                # Use USD amount from ocean audit engine instead of original currency
                invoice_amount = invoice_info.get('total_actual_usd', invoice_amount)
                expected_amount = invoice_info.get('total_expected_usd', 0)
                variance = invoice_info.get('total_variance_usd', 0)
                variance_percent = invoice_info.get('variance_percentage', 0)
                
                # Get rate card info
                rate_card_info = audit_result.get('rate_card_info', {})
                rate_cards_checked = rate_card_info.get('rate_cards_found', 0)
                matching_lanes = 1 if rate_cards_checked > 0 else 0
                
                selected_card = rate_card_info.get('selected_rate_card', {})
                card_id = selected_card.get('rate_card_id', 'N/A')
                lane_name = selected_card.get('lane_name', 'N/A')
                match_score = selected_card.get('match_score', 0)
                best_match_rate_card = (f"Card {card_id}: {lane_name} "
                                        f"(Score: {match_score:.2f})")
                
                # Map audit engine status to batch system status first
                # Ocean audit engine returns: approved, review_required, rejected, error
                if status == 'approved':
                    overall_status = 'APPROVED'
                elif status == 'review_required':
                    overall_status = 'REVIEW_REQUIRED'
                elif status == 'rejected':
                    overall_status = 'REJECTED'
                else:  # error or unknown
                    overall_status = 'ERROR'
                
                # Transform charge breakdown from ocean audit engine for comprehensive comparison
                charge_breakdown_from_engine = audit_result.get('charge_breakdown', {})
                variances_list = []
                total_expected_breakdown = 0
                total_actual_breakdown = 0
                
                # Process each charge type from the engine's breakdown
                for charge_type, charge_data in charge_breakdown_from_engine.items():
                    if isinstance(charge_data, dict):
                        expected_val = charge_data.get('rate_card_amount_usd', 0)
                        actual_val = charge_data.get('invoice_amount_usd', 0)
                        variance_val = charge_data.get('variance_usd', 0)
                        variance_pct = charge_data.get('percentage_variance', 0)
                        
                        # Add to detailed variances list
                        variances_list.append({
                            'charge_type': charge_type.replace('_', ' ').title(),
                            'expected': expected_val,
                            'actual': actual_val,
                            'variance': variance_val,
                            'variance_pct': variance_pct
                        })
                        
                        total_expected_breakdown += expected_val
                        total_actual_breakdown += actual_val
                
                # If no detailed breakdown available, create a single total comparison
                if not variances_list:
                    variances_list = [{
                        'charge_type': 'Total Invoice Amount',
                        'expected': expected_amount,
                        'actual': invoice_amount,
                        'variance': variance,
                        'variance_pct': variance_percent
                    }]
                    total_expected_breakdown = expected_amount
                    total_actual_breakdown = invoice_amount

                # Create audit details with ocean freight specifics
                audit_details = {
                    'invoice_amount': invoice_amount,
                    'expected_amount': expected_amount,
                    'variance': variance,
                    'variance_percent': variance_percent,
                    'transportation_mode': transportation_mode,
                    'origin': origin,
                    'destination': destination,
                    'weight': weight,
                    'service_type': service_type,
                    'rate_cards_checked': rate_cards_checked,
                    'matching_lanes': matching_lanes,
                    'best_match_rate_card': best_match_rate_card,
                    'audit_timestamp': datetime.now().isoformat(),
                    'audit_engine': 'ocean_freight',
                    'processing_time_ms': audit_result.get('processing_time_ms', 0),
                    'thresholds': {
                        'pass_threshold': 5.0,
                        'warning_threshold': 15.0
                    },
                    # Add template-expected structure
                    'invoice_details': {
                        'origin': origin,
                        'destination': destination,
                        'origin_country': 'N/A',
                        'destination_country': 'N/A',
                        'weight_kg': weight,
                        'chargeable_weight_kg': weight,
                        'actual_weight_kg': weight,
                        'volume_m3': volume_m3,
                        'currency': 'USD'
                    },
                    'audit_results': [{
                        'rate_card_id': selected_card.get('rate_card_id', 'N/A'),
                        'lane_description': f"{origin} -> {destination}",
                        'service': service_type,
                        'audit_status': overall_status,
                        'expected_amount': expected_amount,
                        'variance': variance,
                        'variance_percent': variance_percent,
                        'calculation_details': (
                            rate_card_info.get('calculation_method',
                                               'Ocean freight calculation')),
                        'charge_breakdown': charge_breakdown_from_engine,
                        'total_expected': total_expected_breakdown,
                        'total_actual': total_actual_breakdown,
                        'total_variance': total_actual_breakdown - total_expected_breakdown,
                        'variances': variances_list,
                        'status_reason': audit_result.get('reason', f"Variance: {variance_percent:.2f}%")
                    }]
                }
                
            # Use air freight audit engine for air freight shipments
            elif transportation_mode.lower() in ['air', 'airfreight']:
                audit_result = self.air_audit_engine.audit_air_freight_invoice(invoice_no)
                
                if audit_result.get('audit_status') == 'error':
                    # Fallback to old calculation if air audit fails
                    return self._fallback_audit_calculation(invoice_data, batch_run_id, detailed_analysis)
                
                # Extract results from air audit engine
                status = audit_result.get('audit_status', 'error')
                
                # Get expected amount from invoice_data section
                invoice_info = audit_result.get('invoice_data', {})
                # Use USD amount from air audit engine
                invoice_amount = invoice_info.get('total_actual_usd', invoice_amount)
                expected_amount = invoice_info.get('total_expected_usd', 0)
                variance = invoice_info.get('total_variance_usd', 0)
                variance_percent = invoice_info.get('variance_percentage', 0)
                
                # Get rate card info
                rate_card_info = audit_result.get('rate_card_info', {})
                rate_cards_checked = rate_card_info.get('rate_cards_found', 0)
                matching_lanes = 1 if rate_cards_checked > 0 else 0
                
                selected_card = rate_card_info.get('selected_rate_card', {})
                card_id = selected_card.get('rate_card_id', 'N/A')
                lane_name = selected_card.get('lane_name', 'N/A')
                match_score = selected_card.get('match_score', 0)
                best_match_rate_card = (f"Card {card_id}: {lane_name} "
                                        f"(Score: {match_score:.2f})")
                
                # Map audit engine status to batch system status
                # Air audit engine returns: approved, review_required, rejected, error
                if status == 'approved':
                    overall_status = 'APPROVED'
                elif status == 'review_required':
                    overall_status = 'REVIEW_REQUIRED'
                elif status == 'rejected':
                    overall_status = 'REJECTED'
                else:  # error or unknown
                    overall_status = 'ERROR'
                
                # Transform charge breakdown from air audit engine
                charge_breakdown_from_engine = audit_result.get('charge_breakdown', {})
                variances_list = []
                total_expected_breakdown = 0
                total_actual_breakdown = 0
                
                # Process each charge type from the engine's breakdown
                for charge_type, charge_data in charge_breakdown_from_engine.items():
                    if isinstance(charge_data, dict):
                        expected_val = charge_data.get('rate_card_amount_usd', 0)
                        actual_val = charge_data.get('invoice_amount_usd', 0)
                        variance_val = charge_data.get('variance_usd', 0)
                        variance_pct = charge_data.get('percentage_variance', 0)
                        
                        # Add to detailed variances list
                        variances_list.append({
                            'charge_type': charge_type.replace('_', ' ').title(),
                            'expected': expected_val,
                            'actual': actual_val,
                            'variance': variance_val,
                            'variance_pct': variance_pct
                        })
                        
                        total_expected_breakdown += expected_val
                        total_actual_breakdown += actual_val
                
                # If no detailed breakdown available, create a single total comparison
                if not variances_list:
                    variances_list = [{
                        'charge_type': 'Total Invoice Amount',
                        'expected': expected_amount,
                        'actual': invoice_amount,
                        'variance': variance,
                        'variance_pct': variance_percent
                    }]
                    total_expected_breakdown = expected_amount
                    total_actual_breakdown = invoice_amount

                # Create audit details with air freight specifics
                audit_details = {
                    'invoice_amount': invoice_amount,
                    'expected_amount': expected_amount,
                    'variance': variance,
                    'variance_percent': variance_percent,
                    'transportation_mode': transportation_mode,
                    'origin': origin,
                    'destination': destination,
                    'weight': weight,
                    'service_type': service_type,
                    'rate_cards_checked': rate_cards_checked,
                    'matching_lanes': matching_lanes,
                    'best_match_rate_card': best_match_rate_card,
                    'audit_timestamp': datetime.now().isoformat(),
                    'audit_engine': 'air_freight',
                    'processing_time_ms': audit_result.get('processing_time_ms', 0),
                    'thresholds': {
                        'pass_threshold': 5.0,
                        'warning_threshold': 15.0
                    },
                    # Add template-expected structure
                    'invoice_details': {
                        'origin': origin,
                        'destination': destination,
                        'origin_country': 'N/A',
                        'destination_country': 'N/A',
                        'weight_kg': weight,
                        'chargeable_weight_kg': weight,
                        'actual_weight_kg': weight,
                        'volume_m3': 0,  # Not typically used for air freight
                        'currency': 'USD'
                    },
                    'audit_results': [{
                        'rate_card_id': selected_card.get('rate_card_id', 'N/A'),
                        'lane_description': f"{origin} -> {destination}",
                        'service': service_type,
                        'audit_status': overall_status,
                        'expected_amount': expected_amount,
                        'variance': variance,
                        'variance_percent': variance_percent,
                        'calculation_details': (
                            rate_card_info.get('calculation_method',
                                               'Air freight calculation')),
                        'charge_breakdown': charge_breakdown_from_engine,
                        'total_expected': total_expected_breakdown,
                        'total_actual': total_actual_breakdown,
                        'total_variance': total_actual_breakdown - total_expected_breakdown,
                        'variances': variances_list,
                        'status_reason': audit_result.get('reason', f"Variance: {variance_percent:.2f}%")
                    }]
                }
                
            else:
                # Use legacy calculation for other transportation modes
                return self._fallback_audit_calculation(invoice_data, batch_run_id, detailed_analysis)
            
            if detailed_analysis:
                audit_details['detailed_analysis'] = {
                    'variance_analysis': self.analyze_variance(variance_percent, transportation_mode),
                    'rate_comparison': self.compare_rates(transportation_mode, service_type),
                    'recommendations': self.generate_recommendations(status, variance_percent)
                }
            
            # Insert audit result
            if batch_run_id:
                cursor.execute("""
                    INSERT INTO ytd_audit_results (
                        batch_run_id, invoice_no, audit_status, transportation_mode,
                        total_invoice_amount, total_expected_amount, total_variance,
                        variance_percent, rate_cards_checked, matching_lanes,
                        best_match_rate_card, audit_details, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    batch_run_id, invoice_no, status, transportation_mode,
                    invoice_amount, expected_amount, variance, variance_percent,
                    rate_cards_checked, matching_lanes, best_match_rate_card,
                    json.dumps(audit_details), datetime.now()
                ))
                conn.commit()
            
            return {
                'status': status,
                'invoice_no': invoice_no,
                'variance_percent': variance_percent,
                'summary': {
                    'overall_status': overall_status,
                    'expected_amount': expected_amount,
                    'invoice_amount': invoice_amount,
                    'total_variance': variance,
                    'variance_percent': variance_percent,
                    'rate_cards_checked': rate_cards_checked,
                    'matching_lanes': matching_lanes,
                    'best_match_rate_card': best_match_rate_card
                },
                'details': audit_details
            }
            
        except Exception as e:
            print(f"Error in comprehensive audit for invoice {invoice_data[0] if invoice_data else 'unknown'}: {e}")
            return {
                'status': 'error',
                'invoice_no': invoice_data[0] if invoice_data else 'unknown',
                'message': str(e),
                'summary': {
                    'overall_status': 'ERROR',
                    'total_variance': 0,
                    'variance_percent': 0
                }
            }
        
        finally:
            conn.close()
    
    def _fallback_audit_calculation(self, invoice_data, batch_run_id=None, detailed_analysis=True):
        """Fallback audit calculation using the old margin-based method"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            invoice_no = invoice_data[0]
            transportation_mode = invoice_data[1] if len(invoice_data) > 1 else 'unknown'
            invoice_amount = float(invoice_data[2]) if len(invoice_data) > 2 and invoice_data[2] else 0.0
            origin = invoice_data[3] if len(invoice_data) > 3 else 'Unknown'
            destination = invoice_data[4] if len(invoice_data) > 4 else 'Unknown'
            weight = float(invoice_data[5]) if len(invoice_data) > 5 and invoice_data[5] else 0.0
            service_type = invoice_data[6] if len(invoice_data) > 6 else 'Standard'
            
            # Use legacy calculation method
            expected_amount = self.calculate_expected_amount(
                transportation_mode, invoice_amount, weight, service_type, origin, destination
            )
            
            variance = invoice_amount - expected_amount
            
            # Calculate variance percentage with proper error handling
            if invoice_amount > 0:
                variance_percent = (variance / invoice_amount) * 100.0
            else:
                variance_percent = 0.0
            
            # Determine audit status based on variance thresholds
            abs_variance_percent = abs(variance_percent)
            if abs_variance_percent <= 2:
                status = 'pass'
                overall_status = 'PASS'
            elif abs_variance_percent <= 5:
                status = 'warning'
                overall_status = 'WARNING'
            else:
                status = 'fail'
                overall_status = 'FAIL'
            
            # Create audit details
            audit_details = {
                'invoice_amount': invoice_amount,
                'expected_amount': expected_amount,
                'variance': variance,
                'variance_percent': variance_percent,
                'transportation_mode': transportation_mode,
                'origin': origin,
                'destination': destination,
                'weight': weight,
                'service_type': service_type,
                'rate_cards_checked': 0,
                'matching_lanes': 0,
                'audit_timestamp': datetime.now().isoformat(),
                'audit_engine': 'legacy_margin',
                'thresholds': {
                    'pass_threshold': 2.0,
                    'warning_threshold': 5.0
                },
                # Add template-expected structure for fallback calculations too
                'invoice_details': {
                    'origin': origin,
                    'destination': destination,
                    'origin_country': 'N/A',
                    'destination_country': 'N/A',
                    'weight_kg': weight,
                    'chargeable_weight_kg': weight,
                    'actual_weight_kg': weight,
                    'volume_m3': 0,  # Not available in fallback
                    'currency': 'USD'
                },
                'audit_results': [{
                    'rate_card_id': 'FALLBACK',
                    'lane_description': f"{origin} -> {destination}",
                    'service': service_type,
                    'audit_status': overall_status,
                    'expected_amount': expected_amount,
                    'variance': variance,
                    'variance_percent': variance_percent,
                    'calculation_details': f"Legacy margin calculation for {transportation_mode}",
                    'charge_breakdown': {
                        'total_invoice_amount': invoice_amount,
                        'total_expected_amount': expected_amount,
                        'calculation_method': 'margin_based'
                    },
                    # Add template-expected fields
                    'total_expected': expected_amount,
                    'total_actual': invoice_amount,
                    'total_variance': variance,
                    'variances': [
                        {
                            'charge_type': 'Total Invoice Amount',
                            'expected': expected_amount,
                            'actual': invoice_amount,
                            'variance': variance,
                            'variance_pct': variance_percent
                        }
                    ],
                    'status_reason': f"Variance: {variance_percent:.2f}%"
                }]
            }
            
            if detailed_analysis:
                audit_details['detailed_analysis'] = {
                    'variance_analysis': self.analyze_variance(variance_percent, transportation_mode),
                    'rate_comparison': self.compare_rates(transportation_mode, service_type),
                    'recommendations': self.generate_recommendations(status, variance_percent)
                }
            
            # Insert audit result
            if batch_run_id:
                cursor.execute("""
                    INSERT INTO ytd_audit_results (
                        batch_run_id, invoice_no, audit_status, transportation_mode,
                        total_invoice_amount, total_expected_amount, total_variance,
                        variance_percent, rate_cards_checked, matching_lanes,
                        best_match_rate_card, audit_details, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    batch_run_id, invoice_no, status, transportation_mode,
                    invoice_amount, expected_amount, variance, variance_percent,
                    0, 0, 'Legacy calculation (no rate card)',
                    json.dumps(audit_details), datetime.now()
                ))
                conn.commit()
            
            return {
                'status': status,
                'invoice_no': invoice_no,
                'variance_percent': variance_percent,
                'summary': {
                    'overall_status': overall_status,
                    'total_variance': variance,
                    'variance_percent': variance_percent
                },
                'details': audit_details
            }
            
        except Exception as e:
            print(f"Error in fallback audit for invoice {invoice_data[0] if invoice_data else 'unknown'}: {e}")
            return {
                'status': 'error',
                'invoice_no': invoice_data[0] if invoice_data else 'unknown',
                'message': str(e),
                'summary': {
                    'overall_status': 'ERROR',
                    'total_variance': 0,
                    'variance_percent': 0
                }
            }
        
        finally:
            conn.close()
    
    def calculate_expected_amount(self, transportation_mode, invoice_amount, weight, service_type, origin, destination):
        """Calculate expected amount based on transportation mode and other factors"""
        # Base calculation with different margins for different modes
        if transportation_mode.lower() in ['air', 'airfreight']:
            if service_type.lower() in ['express', 'priority']:
                margin = 0.15  # 15% margin for express air
            else:
                margin = 0.12  # 12% margin for standard air
        elif transportation_mode.lower() in ['ocean', 'sea', 'lcl', 'fcl']:
            if service_type.lower() in ['express', 'priority']:
                margin = 0.18  # 18% margin for express ocean
            else:
                margin = 0.15  # 15% margin for standard ocean
        elif transportation_mode.lower() in ['ground', 'truck', 'road']:
            margin = 0.10  # 10% margin for ground transport
        else:
            margin = 0.12  # Default 12% margin
        
        # Adjust for weight (higher margins for smaller shipments)
        if weight > 0:
            if weight < 50:
                margin += 0.03  # Add 3% for small shipments
            elif weight > 1000:
                margin -= 0.02  # Reduce 2% for large shipments
        
        return invoice_amount / (1 + margin)
    
    def analyze_variance(self, variance_percent, transportation_mode):
        """Analyze variance and provide detailed explanation"""
        abs_variance = abs(variance_percent)
        
        analysis = {
            'variance_magnitude': 'low' if abs_variance <= 2 else 'medium' if abs_variance <= 5 else 'high',
            'variance_direction': 'overcharge' if variance_percent > 0 else 'undercharge',
            'expected_range': f"±2% for {transportation_mode} shipments"
        }
        
        if abs_variance > 10:
            analysis['concern_level'] = 'high'
            analysis['investigation_required'] = True
        elif abs_variance > 5:
            analysis['concern_level'] = 'medium'
            analysis['investigation_required'] = False
        else:
            analysis['concern_level'] = 'low'
            analysis['investigation_required'] = False
            
        return analysis
    
    def compare_rates(self, transportation_mode, service_type):
        """Compare rates against market standards"""
        return {
            'market_position': 'within_range',  # Placeholder
            'competitor_comparison': 'competitive',  # Placeholder
            'rate_trend': 'stable'  # Placeholder
        }
    
    def generate_recommendations(self, status, variance_percent):
        """Generate recommendations based on audit results"""
        recommendations = []
        
        if status == 'fail':
            if variance_percent > 10:
                recommendations.append("Investigate significant overcharge - potential billing error")
            elif variance_percent < -10:
                recommendations.append("Review undercharge - may indicate rate card issues")
            else:
                recommendations.append("Review pricing structure for this lane/service")
        elif status == 'warning':
            recommendations.append("Monitor this invoice type for trends")
        else:
            recommendations.append("Invoice within acceptable variance range")
            
        return recommendations
    
    def run_batch_audit(self, force_reaudit=False, batch_name=None):
        """Run batch audit on all invoices"""
        if not batch_name:
            batch_name = f"Batch Audit - {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Create batch run record
            cursor.execute("""
                INSERT INTO ytd_batch_audit_runs (run_name, status, created_at)
                VALUES (?, 'running', ?)
            """, (batch_name, datetime.now()))
            
            batch_run_id = cursor.lastrowid
            conn.commit()
            
            # Get invoices to audit
            invoices = self.get_invoices_to_audit()
            total_invoices = len(invoices)
            
            # Process invoices (simplified for now)
            passed = warned = failed = error = 0
            
            for invoice in invoices[:10]:  # Limit for testing
                try:
                    result = self.audit_single_invoice(invoice[0], batch_run_id)
                    if result['status'] == 'pass':
                        passed += 1
                    elif result['status'] == 'warning':
                        warned += 1
                    elif result['status'] == 'fail':
                        failed += 1
                    else:
                        error += 1
                except Exception as e:
                    print(f"Error auditing invoice {invoice[0]}: {e}")
                    error += 1
            
            # Update batch run
            cursor.execute("""
                UPDATE ytd_batch_audit_runs 
                SET status = 'completed',
                    total_invoices = ?,
                    invoices_passed = ?,
                    invoices_warned = ?,
                    invoices_failed = ?,
                    invoices_error = ?,
                    end_time = ?
                WHERE id = ?
            """, (total_invoices, passed, warned, failed, error, datetime.now(), batch_run_id))
            
            conn.commit()
            
            return {
                'batch_run_id': batch_run_id,
                'total_invoices': total_invoices,
                'status': 'completed'
            }
            
        except Exception as e:
            print(f"Error in batch audit: {e}")
            # Mark batch as failed
            cursor.execute("""
                UPDATE ytd_batch_audit_runs 
                SET status = 'failed', end_time = ?
                WHERE id = ?
            """, (datetime.now(), batch_run_id))
            conn.commit()
            raise
        finally:
            conn.close()
    
    def audit_single_invoice(self, invoice_no, batch_run_id=None):
        """Audit a single invoice"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Get invoice details - check multiple possible table names
            invoice = None
            table_names = ['dhl_ytd_invoices', 'invoices', 'ytd_invoices']
            
            for table_name in table_names:
                try:
                    cursor.execute(f"""
                        SELECT name FROM sqlite_master 
                        WHERE type='table' AND name=?
                    """, (table_name,))
                    
                    if cursor.fetchone():
                        if table_name == 'dhl_ytd_invoices':
                            cursor.execute(f"""
                                SELECT invoice_no, transportation_mode, COALESCE(total_amount, 0),
                                       origin, destination, weight, service_type
                                FROM {table_name}
                                WHERE invoice_no = ?
                            """, (invoice_no,))
                        else:
                            cursor.execute(f"""
                                SELECT invoice_number, mode, COALESCE(total_charges, invoice_amount, 0),
                                       origin_city, destination_city, weight, service_type
                                FROM {table_name}
                                WHERE invoice_number = ?
                            """, (invoice_no,))
                        
                        invoice = cursor.fetchone()
                        if invoice:
                            break
                except Exception as e:
                    print(f"Error querying table {table_name}: {e}")
                    continue
            
            if not invoice:
                # Insert error result
                if batch_run_id:
                    cursor.execute("""
                        INSERT INTO ytd_audit_results (
                            batch_run_id, invoice_no, audit_status, transportation_mode,
                            total_invoice_amount, total_expected_amount, total_variance,
                            variance_percent, rate_cards_checked, matching_lanes,
                            audit_details, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        batch_run_id, invoice_no, 'error', 'unknown',
                        0.0, 0.0, 0.0, 0.0, 0, 0, 
                        json.dumps({'error': 'Invoice not found'}), datetime.now()
                    ))
                    conn.commit()
                
                return {
                    'status': 'error',
                    'invoice_no': invoice_no,
                    'message': f'Invoice {invoice_no} not found in any table',
                    'summary': {
                        'overall_status': 'ERROR',
                        'total_variance': 0,
                        'variance_percent': 0
                    }
                }
            
            # Enhanced audit logic
            invoice_amount = float(invoice[2]) if invoice[2] is not None else 0.0
            transportation_mode = invoice[1] or 'unknown'
            
            # Try to find matching rate cards (simplified logic for now)
            rate_cards_checked = 1
            matching_lanes = 0
            
            # For demo purposes, create some variance
            if transportation_mode.lower() in ['air', 'airfreight']:
                expected_amount = invoice_amount * 0.92  # 8% markup expected
            elif transportation_mode.lower() in ['ocean', 'sea', 'lcl', 'fcl']:
                expected_amount = invoice_amount * 0.88  # 12% markup expected
            else:
                expected_amount = invoice_amount * 0.90  # 10% markup expected
            
            variance = invoice_amount - expected_amount
            
            # Fix division by zero error with better type checking
            if invoice_amount is not None and invoice_amount > 0:
                variance_percent = float(variance) / float(invoice_amount) * 100.0
            else:
                variance_percent = 0.0
            
            # Determine status based on variance
            abs_variance_percent = abs(variance_percent)
            if abs_variance_percent < 3:
                status = 'pass'
                overall_status = 'PASS'
            elif abs_variance_percent < 10:
                status = 'warning'  
                overall_status = 'WARNING'
            else:
                status = 'fail'
                overall_status = 'FAIL'
            
            # Create audit details
            audit_details = {
                'invoice_amount': invoice_amount,
                'expected_amount': expected_amount,
                'variance': variance,
                'variance_percent': variance_percent,
                'transportation_mode': transportation_mode,
                'rate_cards_checked': rate_cards_checked,
                'matching_lanes': matching_lanes,
                'audit_timestamp': datetime.now().isoformat()
            }
            
            # Insert audit result if batch_run_id provided
            if batch_run_id:
                cursor.execute("""
                    INSERT INTO ytd_audit_results (
                        batch_run_id, invoice_no, audit_status, transportation_mode,
                        total_invoice_amount, total_expected_amount, total_variance,
                        variance_percent, rate_cards_checked, matching_lanes,
                        audit_details, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    batch_run_id, invoice_no, status, transportation_mode,
                    invoice_amount, expected_amount, variance, variance_percent,
                    rate_cards_checked, matching_lanes, 
                    json.dumps(audit_details), datetime.now()
                ))
                conn.commit()
            
            # Return detailed result for frontend
            return {
                'status': status,
                'invoice_no': invoice_no,
                'variance_percent': variance_percent,
                'summary': {
                    'overall_status': overall_status,
                    'total_variance': variance,
                    'variance_percent': variance_percent,
                    'best_match': {
                        'total_variance': variance,
                        'variance_percent': variance_percent
                    }
                },
                'air_audits': [] if transportation_mode.lower() not in ['air', 'airfreight'] else [
                    {
                        'rate_card': 'Demo Air Rate Card',
                        'variance': variance,
                        'variance_percent': variance_percent
                    }
                ],
                'ocean_audits': [] if transportation_mode.lower() not in ['ocean', 'sea', 'lcl', 'fcl'] else [
                    {
                        'rate_card': 'Demo Ocean Rate Card', 
                        'variance': variance,
                        'variance_percent': variance_percent
                    }
                ],
                'details': audit_details
            }
            
        except Exception as e:
            print(f"Error auditing invoice {invoice_no}: {e}")
            
            # Insert error result if batch_run_id provided
            if batch_run_id:
                try:
                    cursor.execute("""
                        INSERT INTO ytd_audit_results (
                            batch_run_id, invoice_no, audit_status, transportation_mode,
                            total_invoice_amount, total_expected_amount, total_variance,
                            variance_percent, rate_cards_checked, matching_lanes,
                            audit_details, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        batch_run_id, invoice_no, 'error', 'unknown',
                        0.0, 0.0, 0.0, 0.0, 0, 0,
                        json.dumps({'error': str(e)}), datetime.now()
                    ))
                    conn.commit()
                except:
                    pass  # Don't fail on insert error
            
            return {
                'status': 'error',
                'invoice_no': invoice_no,
                'message': str(e),
                'summary': {
                    'overall_status': 'ERROR',
                    'total_variance': 0,
                    'variance_percent': 0
                }
            }
        finally:
            conn.close()
    
    def get_existing_audit_results(self, invoice_numbers, batch_run_id=None):
        """Get existing audit results for specified invoice numbers"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            results = []
            
            for invoice_no in invoice_numbers:
                try:
                    # Look for existing audit results for this invoice
                    cursor.execute("""
                        SELECT invoice_no, audit_status, transportation_mode, 
                               total_invoice_amount, total_expected_amount, total_variance,
                               variance_percent, rate_cards_checked, matching_lanes,
                               best_match_rate_card, audit_details, processing_time_ms, created_at
                        FROM ytd_audit_results 
                        WHERE invoice_no = ?
                        ORDER BY created_at DESC 
                        LIMIT 1
                    """, (invoice_no,))
                    
                    existing_result = cursor.fetchone()
                    
                    if existing_result:
                        # Found existing result - optionally copy to new batch
                        if batch_run_id:
                            cursor.execute("""
                                INSERT INTO ytd_audit_results (
                                    batch_run_id, invoice_no, audit_status, transportation_mode,
                                    total_invoice_amount, total_expected_amount, total_variance,
                                    variance_percent, rate_cards_checked, matching_lanes,
                                    best_match_rate_card, audit_details, processing_time_ms, created_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                batch_run_id, existing_result[0], existing_result[1], existing_result[2],
                                existing_result[3], existing_result[4], existing_result[5], existing_result[6],
                                existing_result[7], existing_result[8], existing_result[9], existing_result[10],
                                existing_result[11], datetime.now()
                            ))
                        
                        # Convert to result format
                        result = {
                            'status': existing_result[1],  # audit_status
                            'invoice_no': existing_result[0],
                            'transportation_mode': existing_result[2],
                            'total_invoice_amount': existing_result[3] or 0,
                            'total_expected_amount': existing_result[4] or 0,
                            'total_variance': existing_result[5] or 0,
                            'variance_percent': existing_result[6] or 0,
                            'rate_cards_checked': existing_result[7] or 0,
                            'matching_lanes': existing_result[8] or 0,
                            'message': 'Existing audit result retrieved',
                            'summary': {
                                'overall_status': existing_result[1].upper(),
                                'total_variance': existing_result[5] or 0,
                                'variance_percent': existing_result[6] or 0
                            }
                        }
                    else:
                        # No existing result found
                        if batch_run_id:
                            cursor.execute("""
                                INSERT INTO ytd_audit_results (
                                    batch_run_id, invoice_no, audit_status, transportation_mode,
                                    total_invoice_amount, total_expected_amount, total_variance,
                                    variance_percent, rate_cards_checked, matching_lanes,
                                    audit_details, created_at
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                batch_run_id, invoice_no, 'error', 'unknown',
                                0.0, 0.0, 0.0, 0.0, 0, 0, 
                                json.dumps({'error': 'No existing audit result found'}), datetime.now()
                            ))
                        
                        result = {
                            'status': 'error',
                            'invoice_no': invoice_no,
                            'message': f'No existing audit result found for invoice {invoice_no}',
                            'summary': {
                                'overall_status': 'ERROR',
                                'total_variance': 0,
                                'variance_percent': 0
                            }
                        }
                    
                    results.append(result)
                    
                except Exception as e:
                    print(f"Error retrieving audit result for invoice {invoice_no}: {e}")
                    result = {
                        'status': 'error',
                        'invoice_no': invoice_no,
                        'message': f'Error retrieving audit result: {str(e)}',
                        'summary': {
                            'overall_status': 'ERROR',
                            'total_variance': 0,
                            'variance_percent': 0
                        }
                    }
                    results.append(result)
            
            if batch_run_id:
                conn.commit()
                
            return results
            
        except Exception as e:
            print(f"Error getting existing audit results: {e}")
            return []
        finally:
            conn.close()
    
    def get_invoices_to_audit(self):
        """Get list of invoices that need auditing"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Try different table names that might exist
            table_names = ['dhl_ytd_invoices', 'invoices', 'ytd_invoices']
            
            for table_name in table_names:
                try:
                    cursor.execute(f"""
                        SELECT name FROM sqlite_master 
                        WHERE type='table' AND name=?
                    """, (table_name,))
                    
                    if cursor.fetchone():
                        # Table exists, try to get invoices
                        if table_name == 'dhl_ytd_invoices':
                            cursor.execute(f"""
                                SELECT invoice_no, transportation_mode, COALESCE(total_amount, 0)
                                FROM {table_name}
                                ORDER BY invoice_no
                                LIMIT 1000
                            """)
                        else:
                            cursor.execute(f"""
                                SELECT invoice_number, mode, COALESCE(total_charges, invoice_amount, 0)
                                FROM {table_name}
                                ORDER BY invoice_number
                                LIMIT 1000
                            """)
                        return cursor.fetchall()
                except Exception as e:
                    print(f"Error querying table {table_name}: {e}")
                    continue
            
            print("No suitable invoice table found")
            return []
            
        except Exception as e:
            print(f"Error getting invoices to audit: {e}")
            return []
        finally:
            conn.close()
    
    def update_batch_statistics(self, batch_run_id):
        """Update statistics for a specific batch run based on its audit results"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Count audit results by status for this batch
            cursor.execute("""
                SELECT audit_status, COUNT(*) 
                FROM ytd_audit_results 
                WHERE batch_run_id = ?
                GROUP BY audit_status
            """, (batch_run_id,))
            
            status_counts = {}
            for status, count in cursor.fetchall():
                status_counts[status] = count
            
            # Map statuses to batch run columns (handle both uppercase and lowercase)
            passed = (status_counts.get('approved', 0) + status_counts.get('APPROVED', 0) + 
                     status_counts.get('pass', 0) + status_counts.get('PASS', 0))
            warned = (status_counts.get('review_required', 0) + status_counts.get('REVIEW_REQUIRED', 0) + 
                     status_counts.get('warning', 0) + status_counts.get('WARNING', 0))
            failed = (status_counts.get('rejected', 0) + status_counts.get('REJECTED', 0) + 
                     status_counts.get('fail', 0) + status_counts.get('FAIL', 0))
            error = (status_counts.get('error', 0) + status_counts.get('ERROR', 0))
            total = passed + warned + failed + error
            
            # Update batch run statistics
            cursor.execute("""
                UPDATE ytd_batch_audit_runs 
                SET total_invoices = ?,
                    invoices_passed = ?,
                    invoices_warned = ?,
                    invoices_failed = ?,
                    invoices_error = ?
                WHERE id = ?
            """, (total, passed, warned, failed, error, batch_run_id))
            
            conn.commit()
            
            print(f"Updated batch {batch_run_id} statistics: "
                  f"Total={total}, Passed={passed}, Warned={warned}, Failed={failed}, Error={error}")
            
            return {
                'total_invoices': total,
                'invoices_passed': passed,
                'invoices_warned': warned,
                'invoices_failed': failed,
                'invoices_error': error
            }
            
        except Exception as e:
            print(f"Error updating batch statistics for batch {batch_run_id}: {e}")
            return None
        finally:
            conn.close()
    
    def recalculate_all_batch_statistics(self):
        """Recalculate statistics for all existing batch runs"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        try:
            # Get all batch run IDs
            cursor.execute("SELECT id FROM ytd_batch_audit_runs ORDER BY id")
            batch_ids = [row[0] for row in cursor.fetchall()]
            
            updated_count = 0
            for batch_id in batch_ids:
                result = self.update_batch_statistics(batch_id)
                if result:
                    updated_count += 1
            
            print(f"Successfully updated statistics for {updated_count} out of {len(batch_ids)} batch runs")
            return updated_count
            
        except Exception as e:
            print(f"Error recalculating all batch statistics: {e}")
            return 0
        finally:
            conn.close()


if __name__ == "__main__":
    # Test the system
    system = YTDBatchAuditSystem()
    print("YTD Batch Audit System initialized successfully")
