#!/usr/bin/env python3
"""
Ocean Freight Audit Engine
Comprehensive audit engine for ocean freight invoices using actual rate card matching
"""

import sqlite3
import json
from typing import Dict, List, Tuple, Optional
from difflib import SequenceMatcher
import re
from datetime import datetime

class OceanFreightAuditEngine:
    """Advanced ocean freight audit engine with fuzzy rate card matching"""
    
    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path
    
    def audit_invoice(self, invoice_no: str) -> Dict:
        """
        Audit invoice method for compatibility with modular system
        
        Args:
            invoice_no: Invoice number to audit
            
        Returns:
            Dict containing audit results
        """
        return self.audit_ocean_freight_invoice(invoice_no)
        
    def get_db_connection(self):
        """Get database connection"""
        return sqlite3.connect(self.db_path)
    
    def fuzzy_match_location(self, invoice_location: str, rate_card_location: str, threshold: float = 0.7) -> float:
        """
        Fuzzy match two location strings with sophisticated logic
        Returns similarity score (0.0 to 1.0)
        """
        if not invoice_location or not rate_card_location:
            return 0.0
            
        # Normalize strings
        inv_loc = invoice_location.lower().strip()
        rate_loc = rate_card_location.lower().strip()
        
        # Exact match gets highest score
        if inv_loc == rate_loc:
            return 1.0
            
        # Check if one contains the other
        if inv_loc in rate_loc or rate_loc in inv_loc:
            return 0.9
            
        # Extract city names (remove country codes, port codes)
        inv_city = self.extract_city_name(inv_loc)
        rate_city = self.extract_city_name(rate_loc)
        
        if inv_city and rate_city:
            # Check city match
            if inv_city == rate_city:
                return 0.85
                
            # Fuzzy match cities
            city_similarity = SequenceMatcher(None, inv_city, rate_city).ratio()
            if city_similarity > threshold:
                return city_similarity * 0.8
        
        # Overall fuzzy match
        overall_similarity = SequenceMatcher(None, inv_loc, rate_loc).ratio()
        return overall_similarity if overall_similarity > threshold else 0.0
    
    def extract_city_name(self, location: str) -> str:
        """Extract the main city name from a location string"""
        # Remove common port/airport codes and country names
        location = re.sub(r'\b[A-Z]{2,3}\b', '', location)  # Remove codes like SHA, SYD
        location = re.sub(r'\([^)]*\)', '', location)  # Remove parentheses content
        location = re.sub(r',.*$', '', location)  # Take only first part before comma
        return location.strip()
    
    def find_matching_rate_cards(self, origin: str, destination: str, service_type: str = None) -> List[Dict]:
        """
        Find rate cards that match origin and destination with fuzzy logic
        """
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        # Get all ocean rate cards
        cursor.execute("""
            SELECT id, lane_description, lane_origin, lane_destination, service, 
                   rate_validity, contract_validity, origin_port_code, destination_port_code,
                   cities_included_origin, cities_included_destination, created_at,
                   port_of_loading, port_of_discharge
            FROM ocean_rate_cards
            ORDER BY created_at DESC
        """)
        
        rate_cards = cursor.fetchall()
        conn.close()
        
        matches = []
        
        for card in rate_cards:
            (card_id, lane_description, lane_origin, lane_destination, service,
             rate_validity, contract_validity, origin_port_code,
             destination_port_code, cities_included_origin,
             cities_included_destination, created_at,
             port_of_loading, port_of_discharge) = card
            
            # First, check for exact port code matches (highest priority)
            origin_score = 0.0
            destination_score = 0.0
            
            # Check exact port code matches
            if port_of_loading and origin:
                if port_of_loading.upper() == origin.upper():
                    origin_score = 1.0  # Perfect match
                elif (port_of_loading.upper() in origin.upper() or
                      origin.upper() in port_of_loading.upper()):
                    origin_score = 0.95  # Close match
            
            if port_of_discharge and destination:
                if port_of_discharge.upper() == destination.upper():
                    destination_score = 1.0  # Perfect match
                elif (port_of_discharge.upper() in destination.upper() or
                      destination.upper() in port_of_discharge.upper()):
                    destination_score = 0.95  # Close match
            
            # If no port code match, fall back to fuzzy matching on lane names
            if origin_score < 0.5:
                origin_score = self.fuzzy_match_location(origin, lane_origin)
            
            if destination_score < 0.5:
                destination_score = self.fuzzy_match_location(
                    destination, lane_destination)
            
            # Also check cities_included fields for better matching
            if origin_score < 0.6 and cities_included_origin:
                for city in cities_included_origin.split(','):
                    city_score = self.fuzzy_match_location(
                        origin, city.strip())
                    if city_score > origin_score:
                        origin_score = city_score
            
            if destination_score < 0.6 and cities_included_destination:
                for city in cities_included_destination.split(','):
                    city_score = self.fuzzy_match_location(
                        destination, city.strip())
                    if city_score > destination_score:
                        destination_score = city_score
            
            # Debug output for rate card ID 96 (Shanghai-Sydney)
            # if card_id == 96:
            #     print(f"DEBUG: Rate card 96 - '{lane_origin}' -> '{lane_destination}'")
            #     print(f"DEBUG: Cities included origin: '{cities_included_origin}'")
            #     print(f"DEBUG: Cities included destination: '{cities_included_destination}'")
            #     print(f"DEBUG: Origin score: {origin_score:.3f} ({origin} vs {lane_origin})")
            #     print(f"DEBUG: Destination score: {destination_score:.3f} ({destination} vs {lane_destination})")
            
            # Combined score (both origin and destination must match reasonably well)
            if origin_score > 0.6 and destination_score > 0.6:
                combined_score = (origin_score + destination_score) / 2
                
                # Bonus for service type match
                service_bonus = 0.0
                if service_type and service:
                    if service_type.lower() == service.lower():
                        service_bonus = 0.1
                
                final_score = min(1.0, combined_score + service_bonus)
                
                matches.append({
                    'rate_card_id': card_id,
                    'lane_name': lane_description,
                    'lane_origin': lane_origin,
                    'lane_destination': lane_destination,
                    'service_type': service,
                    'origin_port_code': origin_port_code,
                    'destination_port_code': destination_port_code,
                    'port_of_loading': port_of_loading,
                    'port_of_discharge': port_of_discharge,
                    'match_score': final_score,
                    'origin_score': origin_score,
                    'destination_score': destination_score,
                    'rate_validity': rate_validity,
                    'contract_validity': contract_validity
                })
        
        # Sort by match score (highest first)
        matches.sort(key=lambda x: x['match_score'], reverse=True)
        return matches
    
    def get_rate_card_pricing(self, rate_card_id: int) -> Dict:
        """Get detailed pricing for a specific rate card (both FCL and LCL)"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        # Get LCL rates
        cursor.execute("""
            SELECT lcl_pickup_min_usd, lcl_pickup_usd_per_cbm,
                   lcl_origin_handling_min_usd, lcl_origin_handling_usd_per_cbm,
                   lcl_freight_min_usd, lcl_freight_usd_per_cbm,
                   lcl_pss_min_usd, lcl_pss_usd_per_cbm,
                   lcl_dest_handling_min_usd, lcl_dest_handling_usd_per_cbm,
                   lcl_delivery_min_usd, lcl_delivery_usd_per_cbm,
                   lcl_total_min_usd, lcl_total_usd_per_cbm,
                   lcl_dtd_transit_time
            FROM ocean_lcl_rates
            WHERE rate_card_id = ?
            ORDER BY id DESC
            LIMIT 1
        """, (rate_card_id,))
        
        lcl_rate = cursor.fetchone()
        
        # Get FCL rates
        cursor.execute("""
            SELECT pickup_20ft, pickup_40ft, pickup_40hc,
                   origin_handling_20ft, origin_handling_40ft, origin_handling_40hc,
                   freight_rate_20ft, freight_rate_40ft, freight_rate_40hc,
                   pss_20ft, pss_40ft, pss_40hc,
                   dest_handling_20ft, dest_handling_40ft, dest_handling_40hc,
                   delivery_20ft, delivery_40ft, delivery_40hc,
                   total_20ft, total_40ft, total_40hc
            FROM ocean_fcl_charges
            WHERE rate_card_id = ?
            ORDER BY id DESC
            LIMIT 1
        """, (rate_card_id,))
        
        fcl_rate = cursor.fetchone()
        conn.close()
        
        pricing = {}
        
        # Add LCL pricing if available
        if lcl_rate:
            pickup_min, pickup_per_cbm, origin_handling_min, origin_handling_per_cbm, \
            freight_min, freight_per_cbm, pss_min, pss_per_cbm, \
            dest_handling_min, dest_handling_per_cbm, delivery_min, delivery_per_cbm, \
            total_min, total_per_cbm, transit_time = lcl_rate
            
            pricing['lcl'] = {
                'pickup': {
                    'minimum_charge': pickup_min,
                    'rate_per_cbm': pickup_per_cbm,
                    'currency': 'USD'
                },
                'origin_handling': {
                    'minimum_charge': origin_handling_min,
                    'rate_per_cbm': origin_handling_per_cbm,
                    'currency': 'USD'
                },
                'freight': {
                    'minimum_charge': freight_min,
                    'rate_per_cbm': freight_per_cbm,
                    'currency': 'USD'
                },
                'destination_handling': {
                    'minimum_charge': dest_handling_min,
                    'rate_per_cbm': dest_handling_per_cbm,
                    'currency': 'USD'
                },
                'delivery': {
                    'minimum_charge': delivery_min,
                    'rate_per_cbm': delivery_per_cbm,
                    'currency': 'USD'
                },
                'total': {
                    'minimum_charge': total_min,
                    'rate_per_cbm': total_per_cbm,
                    'currency': 'USD'
                }
            }
            
            # Add PSS (Peak Season Surcharge) if available
            if pss_min is not None or pss_per_cbm is not None:
                pricing['lcl']['pss'] = {
                    'minimum_charge': pss_min,
                    'rate_per_cbm': pss_per_cbm,
                    'currency': 'USD'
                }
        
        # Add FCL pricing if available
        if fcl_rate:
            pickup_20ft, pickup_40ft, pickup_40hc, \
            origin_handling_20ft, origin_handling_40ft, origin_handling_40hc, \
            freight_rate_20ft, freight_rate_40ft, freight_rate_40hc, \
            pss_20ft, pss_40ft, pss_40hc, \
            dest_handling_20ft, dest_handling_40ft, dest_handling_40hc, \
            delivery_20ft, delivery_40ft, delivery_40hc, \
            total_20ft, total_40ft, total_40hc = fcl_rate
            
            pricing['fcl'] = {
                'container_rates': {
                    '20ft': {
                        'pickup': pickup_20ft or 0,
                        'origin_handling': origin_handling_20ft or 0,
                        'freight': freight_rate_20ft or 0,
                        'pss': pss_20ft or 0,
                        'destination_handling': dest_handling_20ft or 0,
                        'delivery': delivery_20ft or 0,
                        'total': total_20ft or 0
                    },
                    '40ft': {
                        'pickup': pickup_40ft or 0,
                        'origin_handling': origin_handling_40ft or 0,
                        'freight': freight_rate_40ft or 0,
                        'pss': pss_40ft or 0,
                        'destination_handling': dest_handling_40ft or 0,
                        'delivery': delivery_40ft or 0,
                        'total': total_40ft or 0
                    },
                    '40hc': {
                        'pickup': pickup_40hc or 0,
                        'origin_handling': origin_handling_40hc or 0,
                        'freight': freight_rate_40hc or 0,
                        'pss': pss_40hc or 0,
                        'destination_handling': dest_handling_40hc or 0,
                        'delivery': delivery_40hc or 0,
                        'total': total_40hc or 0
                    }
                }
            }
        
        return pricing
    
    def calculate_ocean_freight_cost(self, volume_cbm: float, weight_kg: float, 
                                   container_type: str, pricing: Dict) -> Dict:
        """
        Calculate expected ocean freight cost based on volume, weight and rate card pricing
        """
        total_cost = 0.0
        cost_breakdown = {}
        currency = 'USD'  # All pricing is in USD
        
        # Determine if FCL or LCL based on container type
        is_fcl = container_type and container_type.upper() == 'FCL'
        
        if is_fcl and 'fcl' in pricing:
            # Use FCL container rates
            fcl_rates = pricing['fcl']['container_rates']
            
            # Determine container size (default to 40ft for FCL)
            container_size = '40ft'  # Default
            if weight_kg > 25000:  # Heavy cargo, prefer 20ft
                container_size = '20ft'
            elif weight_kg > 30000:  # Very heavy, use 40HC
                container_size = '40hc'
            
            # Use the appropriate container rates
            if container_size in fcl_rates:
                container_rates = fcl_rates[container_size]
                
                # Calculate costs for each charge type
                for charge_type, rate in container_rates.items():
                    if charge_type == 'total':
                        continue  # Skip total, calculate individual components
                    
                    charge_cost = float(rate or 0)
                    if charge_cost > 0:
                        cost_breakdown[charge_type] = {
                            'cost': charge_cost,
                            'calculation_method': f"FCL {container_size} container rate: ${charge_cost:.2f}",
                            'container_size': container_size,
                            'rate_type': 'FCL'
                        }
                        total_cost += charge_cost
                
                # If no individual rates, use total rate
                if total_cost == 0 and container_rates.get('total', 0) > 0:
                    total_rate = float(container_rates['total'])
                    cost_breakdown['total'] = {
                        'cost': total_rate,
                        'calculation_method': f"FCL {container_size} total container rate: ${total_rate:.2f}",
                        'container_size': container_size,
                        'rate_type': 'FCL'
                    }
                    total_cost = total_rate
            
        elif 'lcl' in pricing:
            # Use LCL rates with volume-based calculation
            lcl_rates = pricing['lcl']
            
            # Calculate costs for each charge type
            for charge_type, rates in lcl_rates.items():
                if charge_type == 'total':
                    continue  # Skip total, we'll calculate it ourselves
                    
                charge_cost = 0.0
                calculation_method = ''
                
                minimum = rates.get('minimum_charge', 0) or 0
                rate_per_cbm = rates.get('rate_per_cbm', 0) or 0
                
                # Calculate based on volume but apply minimum if higher
                volume_cost = rate_per_cbm * volume_cbm
                charge_cost = max(minimum, volume_cost)
                
                if charge_cost == minimum:
                    calculation_method = f"LCL minimum charge ${minimum:.2f} (volume calc: ${volume_cost:.2f})"
                else:
                    calculation_method = f"LCL volume-based: ${rate_per_cbm:.2f} × {volume_cbm:.2f} CBM = ${volume_cost:.2f}"
                
                if charge_cost > 0:
                    cost_breakdown[charge_type] = {
                        'cost': charge_cost,
                        'calculation_method': calculation_method,
                        'minimum_charge': minimum,
                        'rate_per_cbm': rate_per_cbm,
                        'rate_type': 'LCL'
                    }
                    total_cost += charge_cost
        
        else:
            # Fallback: no pricing data available
            return {
                'total_cost': 0.0,
                'currency': currency,
                'cost_breakdown': {},
                'calculation_type': 'No pricing data available',
                'volume_cbm': volume_cbm,
                'weight_kg': weight_kg,
                'container_type': container_type
            }
        
        return {
            'total_cost': total_cost,
            'currency': currency,
            'cost_breakdown': cost_breakdown,
            'calculation_type': 'FCL' if is_fcl else 'LCL',
            'volume_cbm': volume_cbm,
            'weight_kg': weight_kg,
            'container_type': container_type
        }
    
    def extract_invoice_charges(self, invoice_no: str) -> Dict:
        """Extract detailed charges from YTD invoice (using USD columns)"""
        conn = self.get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT transportation_mode, fcl_lcl, origin, destination,
                   shipment_weight_kg, total_charges_without_duty_tax_usd,
                   total_charges_with_duty_tax_usd, invoice_currency,
                   pickup_charges_usd, origin_handling_charges_usd,
                   origin_demurrage_charges_usd, origin_storage_charges_usd,
                   origin_customs_charges_usd, freight_charges_usd,
                   fuel_surcharges_usd, security_surcharges_usd,
                   destination_customs_charges_usd,
                   destination_storage_charges_usd,
                   destination_demurrage_charges_usd,
                   destination_handling_charges_usd,
                   delivery_charges_usd, other_charges_usd,
                   duties_and_taxes_usd,
                   total_shipment_volume_m3, port_terminal_loading,
                   port_discharge, housebill_origin, housebill_destination
            FROM dhl_ytd_invoices
            WHERE invoice_no = ?
        """, (invoice_no,))
        
        invoice = cursor.fetchone()
        conn.close()
        
        if not invoice:
            return {}
        
        (transportation_mode, fcl_lcl, origin, destination,
         shipment_weight_kg, total_without_duty_tax_usd,
         total_with_duty_tax_usd, invoice_currency,
         pickup_charges_usd, origin_handling_charges_usd,
         origin_demurrage_charges_usd, origin_storage_charges_usd,
         origin_customs_charges_usd, freight_charges_usd,
         fuel_surcharges_usd, security_surcharges_usd,
         destination_customs_charges_usd, destination_storage_charges_usd,
         destination_demurrage_charges_usd,
         destination_handling_charges_usd,
         delivery_charges_usd, other_charges_usd, duties_and_taxes_usd,
         total_shipment_volume_m3, port_terminal_loading,
         port_discharge, housebill_origin, housebill_destination) = invoice
        
        return {
            'transportation_mode': transportation_mode,
            'service_type': fcl_lcl,  # FCL/LCL service type for ocean
            'origin_port': origin,
            'destination_port': destination,
            'housebill_origin_port_code': housebill_origin,
            'housebill_destination_port_code': housebill_destination,
            'port_terminal_loading': port_terminal_loading,
            'port_discharge': port_discharge,
            'shipment_weight_kg': shipment_weight_kg or 0,
            'total_charges_without_duty_tax_usd': (
                total_without_duty_tax_usd or 0),
            'total_charges_with_duty_tax_usd': (
                total_with_duty_tax_usd or 0),
            'currency': 'USD',  # We're using USD columns
            'volume_m3': total_shipment_volume_m3 or 0,
            'charge_breakdown_usd': {
                'pickup_charges': pickup_charges_usd or 0,
                'origin_handling_charges': origin_handling_charges_usd or 0,
                'origin_demurrage_charges': (
                    origin_demurrage_charges_usd or 0),
                'origin_storage_charges': origin_storage_charges_usd or 0,
                'origin_customs_charges': origin_customs_charges_usd or 0,
                'freight_charges': freight_charges_usd or 0,
                'fuel_surcharges': fuel_surcharges_usd or 0,
                'security_surcharges': security_surcharges_usd or 0,
                'destination_customs_charges': (
                    destination_customs_charges_usd or 0),
                'destination_storage_charges': (
                    destination_storage_charges_usd or 0),
                'destination_demurrage_charges': (
                    destination_demurrage_charges_usd or 0),
                'destination_handling_charges': (
                    destination_handling_charges_usd or 0),
                'delivery_charges': delivery_charges_usd or 0,
                'other_charges': other_charges_usd or 0,
                'duties_and_taxes': duties_and_taxes_usd or 0
            }
        }
    
    def audit_ocean_freight_invoice(self, invoice_no: str) -> Dict:
        """Comprehensive audit of an ocean freight invoice using USD amounts"""
        start_time = datetime.now()
        
        # Get invoice data
        invoice_data = self.extract_invoice_charges(invoice_no)
        if not invoice_data:
            return {
                'invoice_no': invoice_no,
                'audit_status': 'error',
                'reason': 'Invoice not found',
                'processing_time_ms': 0
            }
        
        # Check if this is an ocean freight invoice
        transportation_mode = invoice_data.get(
            'transportation_mode', '').lower()
        
        if transportation_mode not in ['sea', 'ocean', 'lcl', 'fcl',
                                       'maritime']:
            return {
                'invoice_no': invoice_no,
                'audit_status': 'skipped',
                'reason': (f"Transportation mode "
                           f"'{invoice_data['transportation_mode']}' is not "
                           f"ocean freight"),
                'processing_time_ms': 0
            }
        
        # Find matching rate cards using housebill port codes first,
        # fallback to city names if no housebill codes available
        origin_for_matching = (
            invoice_data.get('housebill_origin_port_code') or
            invoice_data['origin_port'])
        destination_for_matching = (
            invoice_data.get('housebill_destination_port_code') or
            invoice_data['destination_port'])
        
        matching_cards = self.find_matching_rate_cards(
            origin_for_matching,
            destination_for_matching,
            invoice_data['service_type']
        )
        
        if not matching_cards:
            return {
                'invoice_no': invoice_no,
                'audit_status': 'error',
                'reason': 'No matching rate cards found',
                'processing_time_ms': (
                    (datetime.now() - start_time).total_seconds() * 1000)
            }
        
        # Get rate card pricing for best match
        best_match = matching_cards[0]
        rate_card_pricing = self.get_rate_card_pricing(
            best_match['rate_card_id'])
        
        # Calculate expected costs
        volume_cbm = invoice_data.get('volume_m3', 0)
        weight_kg = invoice_data.get('shipment_weight_kg', 0)
        
        # Use actual volume if available, otherwise approximate from weight
        if volume_cbm <= 0:
            # Use typical density of 300 kg/CBM for general cargo
            volume_cbm = weight_kg / 300 if weight_kg > 0 else 1.0
        
        calculated_costs = self.calculate_ocean_freight_cost(
            volume_cbm, weight_kg, invoice_data['service_type'],
            rate_card_pricing
        )
        
        # Create comprehensive charge breakdown
        invoice_breakdown = invoice_data['charge_breakdown_usd']
        rate_card_breakdown = calculated_costs.get('cost_breakdown', {})
        
        # Map rate card charges to invoice charge types
        mapped_rate_card_breakdown = {}
        for charge_type, charge_info in rate_card_breakdown.items():
            charge_amount = charge_info.get('cost', 0)
            # Map common charge types
            if charge_type in ['pickup', 'pickup_charges']:
                mapped_rate_card_breakdown['pickup_charges'] = charge_amount
            elif charge_type in ['origin_handling', 'origin_handling_charges']:
                mapped_rate_card_breakdown['origin_handling_charges'] = (
                    charge_amount)
            elif charge_type in ['freight', 'freight_charges']:
                mapped_rate_card_breakdown['freight_charges'] = charge_amount
            elif charge_type in ['pss', 'fuel_surcharges']:
                mapped_rate_card_breakdown['fuel_surcharges'] = charge_amount
            elif charge_type in ['destination_handling',
                                 'destination_handling_charges']:
                mapped_rate_card_breakdown['destination_handling_charges'] = (
                    charge_amount)
            elif charge_type in ['delivery', 'delivery_charges']:
                mapped_rate_card_breakdown['delivery_charges'] = charge_amount
            elif charge_type == 'total':
                # If only total is available, distribute to freight
                if not mapped_rate_card_breakdown:
                    mapped_rate_card_breakdown['freight_charges'] = (
                        charge_amount)
            else:
                # Keep original name for unmapped charges
                mapped_rate_card_breakdown[charge_type] = charge_amount
        
        # Create a comprehensive comparison table
        all_charge_types = set(invoice_breakdown.keys()) | set(
            mapped_rate_card_breakdown.keys())
        
        # Define pass-through charges that should be matched 1:1
        passthrough_charges = {
            'duties_and_taxes',
            'fuel_surcharges',
            'security_surcharges',
            'origin_customs_charges',
            'destination_customs_charges'
        }
        
        charge_comparison = {}
        total_expected = 0
        total_actual = 0
        
        for charge_type in sorted(all_charge_types):
            invoice_amount = invoice_breakdown.get(charge_type, 0)
            rate_card_amount = mapped_rate_card_breakdown.get(charge_type, 0)
            
            # For pass-through charges, use invoice amount as expected
            # to avoid unfair variance calculations
            if charge_type in passthrough_charges and invoice_amount > 0:
                rate_card_amount = invoice_amount
                variance = 0  # No variance for pass-through charges
                variance_percentage = 0
            else:
                variance = invoice_amount - rate_card_amount
                variance_percentage = (
                    (variance / rate_card_amount * 100)
                    if rate_card_amount > 0 else
                    (100 if invoice_amount > 0 else 0)
                )
            
            charge_comparison[charge_type] = {
                'invoice_amount_usd': invoice_amount,
                'rate_card_amount_usd': rate_card_amount,
                'variance_usd': variance,
                'percentage_variance': variance_percentage,
                'is_passthrough': charge_type in passthrough_charges
            }
            
            total_expected += rate_card_amount
            total_actual += invoice_amount
        
        # Overall variance
        total_variance = total_actual - total_expected
        
        # Determine audit status
        variance_percentage = (
            (total_variance / total_expected * 100)
            if total_expected > 0 else 0
        )
        
        if abs(variance_percentage) <= 5:
            audit_status = 'approved'
        elif abs(variance_percentage) <= 15:
            audit_status = 'review_required'
        else:
            audit_status = 'rejected'
        
        return {
            'invoice_no': invoice_no,
            'audit_status': audit_status,
            'invoice_data': {
                'transportation_mode': invoice_data['transportation_mode'],
                'service_type': invoice_data['service_type'],
                'origin_port': invoice_data['origin_port'],
                'destination_port': invoice_data['destination_port'],
                'shipment_weight_kg': invoice_data['shipment_weight_kg'],
                'volume_m3': invoice_data['volume_m3'],
                'total_actual_usd': invoice_data[
                    'total_charges_without_duty_tax_usd'],
                'total_expected_usd': total_expected,
                'total_variance_usd': total_variance,
                'variance_percentage': variance_percentage
            },
            'rate_card_info': {
                'rate_cards_found': len(matching_cards),
                'selected_rate_card': {
                    'rate_card_id': best_match['rate_card_id'],
                    'lane_name': best_match.get('lane_name', 'N/A'),
                    'match_score': best_match.get('match_score', 0)
                },
                'calculation_method': calculated_costs.get(
                    'calculation_method', 'unknown')
            },
            'charge_breakdown': charge_comparison,
            'processing_time_ms': (
                (datetime.now() - start_time).total_seconds() * 1000)
        }

    def test_audit_specific_invoice(self, invoice_no: str) -> Dict:
        """Test the audit engine with a specific invoice"""
        print(f"\n=== Testing Ocean Freight Audit for Invoice {invoice_no} ===")
        
        result = self.audit_ocean_freight_invoice(invoice_no)
        
        print(f"Status: {result.get('status', 'unknown')}")
        print(f"Transportation Mode: {result.get('transportation_mode', 'N/A')}")
        print(f"Invoice Amount: ${result.get('total_invoice_amount', 0):.2f}")
        print(f"Expected Amount: ${result.get('total_expected_amount', 0):.2f}")
        print(f"Variance: ${result.get('total_variance', 0):.2f} ({result.get('variance_percent', 0):.1f}%)")
        print(f"Rate Cards Checked: {result.get('rate_cards_checked', 0)}")
        print(f"Matching Lanes: {result.get('matching_lanes', 0)}")
        print(f"Best Match: {result.get('best_match_rate_card', 'None')}")
        print(f"Processing Time: {result.get('processing_time_ms', 0):.1f}ms")
        
        if 'audit_details' in result:
            details = result['audit_details']
            print(f"\nCalculation Details:")
            if 'expected_calculation' in details:
                calc = details['expected_calculation']
                print(f"  Calculation Type: {calc.get('calculation_type', 'N/A')}")
                print(f"  Volume: {calc.get('volume_cbm', 0):.2f} CBM")
                print(f"  Container Type: {calc.get('container_type', 'N/A')}")
                print(f"  Cost Breakdown:")
                for charge_type, charge_data in calc.get('cost_breakdown', {}).items():
                    print(f"    {charge_type}: ${charge_data.get('cost', 0):.2f} - {charge_data.get('calculation_method', 'N/A')}")
        
        return result

if __name__ == "__main__":
    # Test the audit engine
    engine = OceanFreightAuditEngine()
    result = engine.test_audit_specific_invoice('D2016418')
