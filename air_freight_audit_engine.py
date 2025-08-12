"""
Air Freight Audit Engine
Enhanced auditing for air freight invoices using rate card matching
"""

import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple


class AirFreightAuditEngine:
    """
    Air Freight Audit Engine for processing air freight invoices 
    against rate cards with housebill port code matching
    """
    
    def __init__(self, db_path: str = "dhl_audit.db"):
        self.db_path = db_path
    
    def audit_invoice(self, invoice_no: str) -> Dict:
        """
        Audit invoice method for compatibility with modular system
        
        Args:
            invoice_no: Invoice number to audit
            
        Returns:
            Dict containing audit results
        """
        return self.audit_air_freight_invoice(invoice_no)
    
    def audit_air_freight_invoice(self, invoice_no: str) -> Dict:
        """
        Audit an air freight invoice against rate cards
        
        Args:
            invoice_no: Invoice number to audit
            
        Returns:
            Dict containing audit results
        """
        try:
            start_time = datetime.now()
            
            # Get invoice data
            invoice_data = self._get_invoice_data(invoice_no)
            if not invoice_data:
                return {
                    'audit_status': 'error',
                    'reason': f'Invoice {invoice_no} not found',
                    'processing_time_ms': 0
                }
            
            # Find matching rate cards
            rate_cards = self._find_matching_rate_cards(invoice_data)
            if not rate_cards:
                return {
                    'audit_status': 'error',
                    'reason': 'No matching rate cards found',
                    'processing_time_ms': int((datetime.now() - start_time).total_seconds() * 1000),
                    'invoice_data': invoice_data
                }
            
            # Select best rate card and calculate charges
            # Calculate charges for both Standard and Expedite services if available
            service_results = []
            
            # Group rate cards by service type
            standard_rates = [r for r in rate_cards if r.get('service', '').lower() == 'standard']
            expedite_rates = [r for r in rate_cards if r.get('service', '').lower() == 'expedite']
            
            # Calculate for Standard service
            if standard_rates:
                standard_card = standard_rates[0]  # Take first Standard match
                standard_result = self._calculate_charges(invoice_data, standard_card)
                standard_result['service_type'] = 'Standard'
                standard_result['rate_card'] = standard_card
                service_results.append(standard_result)
            
            # Calculate for Expedite service
            if expedite_rates:
                expedite_card = expedite_rates[0]  # Take first Expedite match
                expedite_result = self._calculate_charges(invoice_data, expedite_card)
                expedite_result['service_type'] = 'Expedite'
                expedite_result['rate_card'] = expedite_card
                service_results.append(expedite_result)
            
            # If no specific service types found, use first available rate card
            if not service_results and rate_cards:
                fallback_card = rate_cards[0]
                fallback_result = self._calculate_charges(invoice_data, fallback_card)
                fallback_result['service_type'] = fallback_card.get('service', 'Unknown')
                fallback_result['rate_card'] = fallback_card
                service_results.append(fallback_result)
            
            # Choose the result with the least absolute variance
            if service_results:
                best_result = min(service_results, 
                                key=lambda x: abs(x['invoice_data']['auditable_variance_usd']))
                audit_result = best_result
                best_rate_card = best_result['rate_card']
                selected_service_type = best_result['service_type']
            else:
                return {
                    'audit_status': 'error',
                    'reason': 'No rate cards found for route',
                    'processing_time_ms': 0
                }
            
            # Determine audit status
            variance_percentage = audit_result['invoice_data'].get('variance_percentage', 0)
            auditable_variance = audit_result['invoice_data'].get('auditable_variance_usd', 0)
            
            # If expected value is higher than invoice value (negative variance), mark as PASS
            if auditable_variance <= 0:
                audit_status = 'approved'
            # Otherwise apply the standard percentage thresholds
            elif abs(variance_percentage) <= 5:
                audit_status = 'approved'
            elif abs(variance_percentage) <= 15:
                audit_status = 'review_required'
            else:
                audit_status = 'rejected'
            
            processing_time = int((datetime.now() - start_time).total_seconds() * 1000)
            
            return {
                'audit_status': audit_status,
                'reason': f'Variance: {variance_percentage:.2f}%',
                'processing_time_ms': processing_time,
                'invoice_data': audit_result['invoice_data'],
                'rate_card_info': {
                    'rate_cards_found': len(rate_cards),
                    'services_evaluated': len(service_results),
                    'selected_rate_card': {
                        'rate_card_id': best_rate_card.get('rate_card_id'),
                        'service_type': best_result['service_type'],
                        'lane_name': f"{best_rate_card.get('lane_origin', 'N/A')} -> {best_rate_card.get('lane_destination', 'N/A')}",
                        'match_score': 1.0  # Perfect match for exact port code matching
                    },
                    'calculation_method': 'Air freight weight-based calculation',
                    'service_comparison': [
                        {
                            'service_type': result['service_type'],
                            'variance_usd': result['invoice_data']['auditable_variance_usd'],
                            'variance_percentage': result['invoice_data']['variance_percentage']
                        } for result in service_results
                    ]
                },
                'charge_breakdown': audit_result.get('charge_breakdown', {})
            }
            
        except Exception as e:
            return {
                'audit_status': 'error',
                'reason': f'Error during audit: {str(e)}',
                'processing_time_ms': 0
            }
    
    def _get_invoice_data(self, invoice_no: str) -> Optional[Dict]:
        """Get invoice data from database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT invoice_no, transportation_mode, housebill_origin, 
                       housebill_destination, origin, destination, 
                       shipment_weight_kg, total_shipment_chargeable_weight_kg,
                       freight_charges_usd, fuel_surcharges_usd, 
                       security_surcharges_usd, origin_handling_charges_usd, 
                       destination_handling_charges_usd, pickup_charges_usd, 
                       delivery_charges_usd, other_charges_usd,
                       origin_customs_charges_usd, destination_customs_charges_usd,
                       duties_and_taxes_usd, 
                       total_charges_without_duty_tax_usd,
                       total_charges_with_duty_tax_usd,
                       invoice_currency, exchange_rate_usd
                FROM dhl_ytd_invoices 
                WHERE invoice_no = ? AND transportation_mode = 'Air'
            """, (invoice_no,))
            
            result = cursor.fetchone()
            if not result:
                return None
            
            columns = [desc[0] for desc in cursor.description]
            invoice_data = dict(zip(columns, result))
            
            return invoice_data
            
        finally:
            conn.close()
    
    def _find_matching_rate_cards(self, invoice_data: Dict) -> List[Dict]:
        """Find matching rate cards using housebill port codes"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            housebill_origin = invoice_data.get('housebill_origin', '')
            housebill_destination = invoice_data.get('housebill_destination', '')
            
            if not housebill_origin or not housebill_destination:
                return []
            
            # Apply port code mappings/exceptions
            # Map CNPVG (Shanghai Pudong) to CNSHA for the initial query
            if housebill_origin == 'CNPVG':
                housebill_origin = 'CNSHA'
            
            # Find exact matches using housebill port codes
            cursor.execute("""
                SELECT ar.*, arc.card_name, arc.validity_start, arc.validity_end
                FROM air_rate_entries ar
                JOIN air_rate_cards arc ON ar.rate_card_id = arc.id
                WHERE ar.origin_port_code = ? AND ar.destination_port_code = ?
                ORDER BY ar.lane_id
            """, (housebill_origin, housebill_destination))
            
            matches = []
            for row in cursor.fetchall():
                cursor.execute("PRAGMA table_info(air_rate_entries)")
                air_columns = [col[1] for col in cursor.fetchall()]
                air_columns.extend(['card_name', 'validity_start', 'validity_end'])
                
                rate_data = dict(zip(air_columns, row))
                matches.append(rate_data)
            
            # If no matches found and we have an alternate origin code, try again
            if not matches and housebill_origin == 'CNSHA':
                # Try with CNPVG as an alternate origin
                cursor.execute("""
                    SELECT ar.*, arc.card_name, arc.validity_start, arc.validity_end
                    FROM air_rate_entries ar
                    JOIN air_rate_cards arc ON ar.rate_card_id = arc.id
                    WHERE ar.origin_port_code = ? AND ar.destination_port_code = ?
                    ORDER BY ar.lane_id
                """, ('CNPVG', housebill_destination))
                
                for row in cursor.fetchall():
                    cursor.execute("PRAGMA table_info(air_rate_entries)")
                    air_columns = [col[1] for col in cursor.fetchall()]
                    air_columns.extend(['card_name', 'validity_start', 'validity_end'])
                    
                    rate_data = dict(zip(air_columns, row))
                    matches.append(rate_data)
            
            return matches
            
        finally:
            conn.close()
    
    def _calculate_charges(self, invoice_data: Dict, rate_card: Dict) -> Dict:
        """Calculate expected charges based on rate card with item-by-item comparison"""
        weight_kg = float(invoice_data.get('shipment_weight_kg') or 0)
        exchange_rate = float(invoice_data.get('exchange_rate_usd') or 1)
        currency = invoice_data.get('invoice_currency', 'USD')
        
        # ATA (Air Transport Association) freight calculation
        # Use ATA cost per kg based on weight brackets
        if weight_kg < 1000:
            ata_rate_per_kg = float(rate_card.get('ata_cost_lt1000kg') or 0)
        elif weight_kg < 2000:
            ata_rate_per_kg = float(rate_card.get('ata_cost_1000_1999kg') or 0)
        elif weight_kg < 3000:
            ata_rate_per_kg = float(rate_card.get('ata_cost_2000_3000kg') or 0)
        else:
            ata_rate_per_kg = float(rate_card.get('ata_cost_gt3000kg') or 0)
        
        # Calculate ATA freight: max(rate_per_kg × weight, ata_min_charge)
        ata_calculated = weight_kg * ata_rate_per_kg
        ata_min_charge = float(rate_card.get('ata_min_charge') or 0)
        freight_expected = max(ata_calculated, ata_min_charge)
        
        # Calculate expected fuel surcharge (if applicable)
        fuel_surcharge_expected = (weight_kg *
                                   float(rate_card.get('fuel_surcharge') or 0))
        
        # Origin handling calculation
        # Use PTD (Pre-Transport Delivery/Dispatch) for origin cost
        # PTD Cost = max(ptd_min_charge, ptd_freight_charge × weight)
        ptd_freight_charge = float(rate_card.get('ptd_freight_charge') or 0)
        ptd_min_charge = float(rate_card.get('ptd_min_charge') or 0)
        ptd_calculated = weight_kg * ptd_freight_charge
        origin_fees_expected = max(ptd_calculated, ptd_min_charge)
        
        # Destination handling calculation
        # For destination: max(min_charge, rate_per_kg × weight)
        destination_min_charge = float(
            rate_card.get('destination_min_charge') or 0)
        # If no per-kg rate is specified, use minimum charge
        destination_fees_expected = destination_min_charge
        
        # Calculate expected security charges
        security_expected = float(rate_card.get('security_surcharge') or 0)
        
        # Delivery charges calculation using PTD as well
        # For delivery: max(ptd_min_charge, ptd_freight_charge × weight)
        delivery_charge_expected = max(ptd_calculated, ptd_min_charge)
        
        # Get actual amounts and convert to USD
        def to_usd(amount):
            if currency != 'USD' and exchange_rate > 0:
                return float(amount or 0) * exchange_rate
            return float(amount or 0)
        
        # Actual charges from invoice (using correct USD column names)
        freight_actual_usd = float(invoice_data.get('freight_charges_usd') or 0)
        fuel_actual_usd = float(invoice_data.get('fuel_surcharges_usd') or 0)
        security_actual_usd = float(invoice_data.get('security_surcharges_usd') or 0)
        origin_actual_usd = float(
            invoice_data.get('origin_handling_charges_usd') or 0)
        destination_actual_usd = float(
            invoice_data.get('destination_handling_charges_usd') or 0)
        pickup_actual_usd = float(invoice_data.get('pickup_charges_usd') or 0)
        delivery_actual_usd = float(invoice_data.get('delivery_charges_usd') or 0)
        other_actual_usd = float(invoice_data.get('other_charges_usd') or 0)
        
        # Pass-through charges (should match exactly)
        duty_tax_actual_usd = float(invoice_data.get('duties_and_taxes_usd') or 0)
        
        # Get actual customs charges from invoice data
        origin_customs_actual_usd = float(invoice_data.get('origin_customs_charges_usd') or 0)
        destination_customs_actual_usd = float(invoice_data.get('destination_customs_charges_usd') or 0)
        customs_actual_usd = origin_customs_actual_usd + destination_customs_actual_usd
        
        # Calculate expected total (only auditable charges)
        total_expected = (freight_expected +
                          origin_fees_expected + destination_fees_expected +
                          security_expected + delivery_charge_expected)
        
        # Calculate actual total (all charges)
        total_actual_usd = float(
            invoice_data.get('total_charges_with_duty_tax_usd') or 0)
        
        # Build detailed charge breakdown
        charge_breakdown = {
            'freight_charges': {
                'invoice_amount_usd': freight_actual_usd,
                'rate_card_amount_usd': freight_expected,
                'variance_usd': freight_actual_usd - freight_expected,
                'percentage_variance': (
                    ((freight_actual_usd - freight_expected) / freight_expected * 100)
                    if freight_expected > 0 else 0),
                'audit_type': 'rate_card_comparison'
            },
            'fuel_surcharges': {
                'invoice_amount_usd': fuel_actual_usd,
                'rate_card_amount_usd': fuel_actual_usd,  # Pass-through
                'variance_usd': 0,
                'percentage_variance': 0,
                'audit_type': 'pass_through'
            },
            'security_surcharges': {
                'invoice_amount_usd': security_actual_usd,
                'rate_card_amount_usd': security_expected,
                'variance_usd': security_actual_usd - security_expected,
                'percentage_variance': (
                    ((security_actual_usd - security_expected) / security_expected * 100)
                    if security_expected > 0 else 0),
                'audit_type': 'rate_card_comparison'
            },
            'origin_handling': {
                'invoice_amount_usd': origin_actual_usd,
                'rate_card_amount_usd': origin_fees_expected,
                'variance_usd': origin_actual_usd - origin_fees_expected,
                'percentage_variance': (
                    ((origin_actual_usd - origin_fees_expected) / origin_fees_expected * 100)
                    if origin_fees_expected > 0 else 0),
                'audit_type': 'rate_card_comparison'
            },
            'destination_handling': {
                'invoice_amount_usd': destination_actual_usd,
                'rate_card_amount_usd': destination_fees_expected,
                'variance_usd': destination_actual_usd - destination_fees_expected,
                'percentage_variance': (
                    ((destination_actual_usd - destination_fees_expected) / destination_fees_expected * 100)
                    if destination_fees_expected > 0 else 0),
                'audit_type': 'rate_card_comparison'
            },
            'pickup_charges': {
                'invoice_amount_usd': pickup_actual_usd,
                'rate_card_amount_usd': 0,  # Not in rate card
                'variance_usd': pickup_actual_usd,
                'percentage_variance': 0,
                'audit_type': 'additional_charge'
            },
            'delivery_charges': {
                'invoice_amount_usd': delivery_actual_usd,
                'rate_card_amount_usd': delivery_charge_expected,
                'variance_usd': delivery_actual_usd - delivery_charge_expected,
                'percentage_variance': (
                    ((delivery_actual_usd - delivery_charge_expected) / delivery_charge_expected * 100)
                    if delivery_charge_expected > 0 else 0),
                'audit_type': 'rate_card_comparison'  # Changed from additional_charge to rate_card_comparison
            },
            'other_charges': {
                'invoice_amount_usd': other_actual_usd,
                'rate_card_amount_usd': 0,  # Not in rate card
                'variance_usd': other_actual_usd,
                'percentage_variance': 0,
                'audit_type': 'additional_charge'
            },
            'duty_tax_charges': {
                'invoice_amount_usd': duty_tax_actual_usd,
                'rate_card_amount_usd': duty_tax_actual_usd,  # Pass-through
                'variance_usd': 0,
                'percentage_variance': 0,
                'audit_type': 'pass_through'
            },
            'customs_charges': {
                'invoice_amount_usd': customs_actual_usd,
                'rate_card_amount_usd': customs_actual_usd,  # Pass-through
                'variance_usd': 0,
                'percentage_variance': 0,
                'audit_type': 'pass_through'
            }
        }
        
        # Calculate total variance (only for auditable charges)
        auditable_variance = sum([
            charge_breakdown['freight_charges']['variance_usd'],
            charge_breakdown['security_surcharges']['variance_usd'],
            charge_breakdown['origin_handling']['variance_usd'],
            charge_breakdown['destination_handling']['variance_usd'],
            charge_breakdown['delivery_charges']['variance_usd']  # Include delivery charges in auditable variance
        ])
        
        # Calculate expected total including pass-through charges
        # Note: delivery charges are now part of total_expected, so don't add them again
        total_expected_with_passthrough = (total_expected + 
                                          fuel_actual_usd +
                                          duty_tax_actual_usd + 
                                          customs_actual_usd +
                                          pickup_actual_usd +
                                          other_actual_usd)
        
        total_variance_usd = total_actual_usd - total_expected_with_passthrough
        variance_percentage = ((auditable_variance / total_expected * 100)
                              if total_expected > 0 else 0)
        
        return {
            'invoice_data': {
                'invoice_no': invoice_data.get('invoice_no'),
                'transportation_mode': invoice_data.get('transportation_mode'),
                'housebill_origin': invoice_data.get('housebill_origin'),
                'housebill_destination': invoice_data.get('housebill_destination'),
                'shipment_weight_kg': weight_kg,
                'total_actual_usd': total_actual_usd,
                'total_expected_usd': total_expected_with_passthrough,
                'total_variance_usd': total_variance_usd,
                'auditable_variance_usd': auditable_variance,
                'variance_percentage': variance_percentage,
                'weight_kg': weight_kg,
                'currency': 'USD',
                'exchange_rate': exchange_rate
            },
            'charge_breakdown': charge_breakdown
        }
