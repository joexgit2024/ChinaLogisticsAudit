"""
Enhanced Invoice Audit Engine
============================

This module provides comprehensive invoice auditing against rate cards.
Handles currency conversion, weight calculations, and flexible location matching.
"""

from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
import sqlite3
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AuditSeverity(Enum):
    """Audit issue severity levels"""
    ERROR = "error"          # Critical discrepancy
    WARNING = "warning"      # Significant variance
    INFO = "info"           # Minor variance or note
    APPROVED = "approved"    # Charges match rate card

@dataclass
class AuditIssue:
    """Single audit issue"""
    charge_type: str
    severity: AuditSeverity
    message: str
    invoice_amount: float = None
    expected_amount: float = None
    variance_amount: float = None
    variance_percent: float = None

@dataclass
class AuditResult:
    """Complete audit result for an invoice"""
    invoice_number: str
    audit_status: str  # approved, pending, review, error
    total_invoice_amount: float
    total_expected_amount: float
    total_variance: float
    variance_percent: float
    currency_conversion: Dict[str, Any]
    weight_analysis: Dict[str, Any]
    location_match: Dict[str, Any]
    issues: List[AuditIssue]
    rate_card_used: str = None
    
    @property
    def error_count(self) -> int:
        return len([i for i in self.issues if i.severity == AuditSeverity.ERROR])
    
    @property
    def warning_count(self) -> int:
        return len([i for i in self.issues if i.severity == AuditSeverity.WARNING])

class InvoiceAuditor:
    """Enhanced invoice audit engine"""
    
    # Currency exchange tolerance
    EXCHANGE_RATE_TOLERANCE = 0.05  # 5% tolerance
    
    # Charge variance thresholds
    VARIANCE_THRESHOLDS = {
        'approved': 2.0,    # <=2% variance - approved
        'pending': 10.0,    # <=10% variance - pending review
        'review': 25.0,     # <=25% variance - requires review
        'error': 100.0      # >25% variance - error
    }
    
    # Weight tolerance for chargeable weight calculation
    WEIGHT_TOLERANCE = 0.1  # 10% tolerance
    
    def __init__(self, db_path: str = 'dhl_audit.db'):
        self.db_path = db_path
    
    def audit_invoice(self, invoice_id: int) -> AuditResult:
        """
        Audit a single invoice against rate cards
        
        Args:
            invoice_id: Database ID of the invoice to audit
            
        Returns:
            AuditResult with complete audit analysis
        """
        # Get invoice data
        invoice = self._get_invoice_data(invoice_id)
        if not invoice:
            return self._create_error_result("Invoice not found", invoice_id)
        
        # Skip ocean freight if no ocean rate card
        if self._is_ocean_freight(invoice):
            return self._create_skip_result("Ocean freight - rate card not available", invoice['invoice_number'])
        
        # Find matching rate card
        rate_card_match = self._find_matching_rate_card(invoice)
        if not rate_card_match:
            return self._create_error_result("No matching rate card found", invoice['invoice_number'])
        
        # Perform audit
        return self._perform_detailed_audit(invoice, rate_card_match)
    
    def audit_all_invoices(self) -> List[AuditResult]:
        """Audit all invoices in the database"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        cursor = conn.execute("SELECT id FROM invoices ORDER BY id")
        invoice_ids = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        results = []
        for invoice_id in invoice_ids:
            try:
                result = self.audit_invoice(invoice_id)
                results.append(result)
                logger.info(f"Audited invoice ID {invoice_id}: {result.audit_status}")
            except Exception as e:
                logger.error(f"Error auditing invoice ID {invoice_id}: {e}")
                results.append(self._create_error_result(f"Audit error: {e}", f"ID_{invoice_id}"))
        
        return results
    
    def _get_invoice_data(self, invoice_id: int) -> Optional[Dict[str, Any]]:
        """Get invoice data from database"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        cursor = conn.execute("""
            SELECT * FROM invoices 
            WHERE id = ?
        """, (invoice_id,))
        
        invoice = cursor.fetchone()
        conn.close()
        
        return dict(invoice) if invoice else None
    
    def _is_ocean_freight(self, invoice: Dict[str, Any]) -> bool:
        """Check if invoice is for ocean freight"""
        shipping_mode = (invoice.get('shipping_mode') or '').upper()
        service_type = (invoice.get('service_type') or '').upper()
        
        # Ocean freight indicators
        ocean_indicators = [
            'OCEAN', 'SEA', 'FCL', 'LCL', 'CONTAINER',
            'VESSEL', 'SHIP', 'MARITIME'
        ]
        
        for indicator in ocean_indicators:
            if indicator in shipping_mode or indicator in service_type:
                return True
        
        # Check for container/vessel data
        if invoice.get('container_number') or invoice.get('vessel_name'):
            return True
        
        return False
    
    def _find_matching_rate_card(self, invoice: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Find matching rate card for the invoice"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        # Extract location info from invoice
        origin_country = invoice.get('shipper_country')
        dest_country = invoice.get('consignee_country')
        origin_city = invoice.get('shipper_city')
        dest_city = invoice.get('consignee_city')
        
        # Try exact location match first
        query = """
            SELECT * FROM air_rate_entries 
            WHERE (
                -- Exact country match
                (origin_country = ? AND destination_country = ?) OR
                -- City match
                (cities_included_origin LIKE ? AND cities_included_dest LIKE ?) OR
                -- Flexible location matching
                (origin_region LIKE ? AND destination_region LIKE ?)
            )
            ORDER BY 
                CASE 
                    WHEN origin_country = ? AND destination_country = ? THEN 1
                    WHEN cities_included_origin LIKE ? AND cities_included_dest LIKE ? THEN 2
                    ELSE 3
                END
            LIMIT 1
        """
        
        # Prepare search parameters
        origin_city_search = f'%{origin_city}%' if origin_city else '%'
        dest_city_search = f'%{dest_city}%' if dest_city else '%'
        origin_region_search = f'%{origin_country}%' if origin_country else '%'
        dest_region_search = f'%{dest_country}%' if dest_country else '%'
        
        cursor = conn.execute(query, (
            origin_country, dest_country,  # Exact country match
            origin_city_search, dest_city_search,  # City match
            origin_region_search, dest_region_search,  # Region match
            origin_country, dest_country,  # Order by exact country match
            origin_city_search, dest_city_search   # Order by city match
        ))
        
        rate_card = cursor.fetchone()
        conn.close()
        
        return dict(rate_card) if rate_card else None
    
    def _perform_detailed_audit(self, invoice: Dict[str, Any], rate_card: Dict[str, Any]) -> AuditResult:
        """Perform detailed audit of invoice against rate card"""
        issues = []
        
        # 1. Currency Analysis
        currency_analysis = self._analyze_currency_conversion(invoice)
        if currency_analysis.get('issues'):
            issues.extend(currency_analysis['issues'])
        
        # 2. Weight Analysis - use higher of ship_weight or bill_weight
        weight_analysis = self._analyze_weight_charges(invoice, rate_card)
        if weight_analysis.get('issues'):
            issues.extend(weight_analysis['issues'])
        
        # 3. Location Match Analysis
        location_analysis = self._analyze_location_match(invoice, rate_card)
        if location_analysis.get('issues'):
            issues.extend(location_analysis['issues'])
        
        # 4. Charge Breakdown Analysis
        charge_analysis = self._analyze_charge_breakdown(invoice, rate_card, weight_analysis['chargeable_weight'])
        if charge_analysis.get('issues'):
            issues.extend(charge_analysis['issues'])
        
        # Calculate totals and variance
        total_invoice = float(invoice.get('total_charges', 0))
        total_expected = charge_analysis.get('total_expected', total_invoice)
        total_variance = total_invoice - total_expected
        variance_percent = (abs(total_variance) / total_expected * 100) if total_expected > 0 else 0
        
        # Determine audit status
        audit_status = self._determine_audit_status(variance_percent, issues)
        
        return AuditResult(
            invoice_number=invoice['invoice_number'],
            audit_status=audit_status,
            total_invoice_amount=total_invoice,
            total_expected_amount=total_expected,
            total_variance=total_variance,
            variance_percent=variance_percent,
            currency_conversion=currency_analysis,
            weight_analysis=weight_analysis,
            location_match=location_analysis,
            issues=issues,
            rate_card_used=f"Lane {rate_card.get('lane_id', 'Unknown')}"
        )
    
    def _analyze_currency_conversion(self, invoice: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze currency conversion between invoice (AUD) and rate card (USD)"""
        invoice_currency = invoice.get('currency', 'AUD')
        rate_card_currency = 'USD'  # Rate cards are in USD
        exchange_rate = float(invoice.get('exchange_rate', 1.0))
        
        issues = []
        
        if invoice_currency == rate_card_currency:
            return {
                'conversion_needed': False,
                'exchange_rate': 1.0,
                'issues': issues
            }
        
        # Check if exchange rate is reasonable
        if exchange_rate <= 0 or exchange_rate > 10:
            issues.append(AuditIssue(
                charge_type='exchange_rate',
                severity=AuditSeverity.WARNING,
                message=f"Unusual exchange rate: {exchange_rate}",
                invoice_amount=exchange_rate
            ))
        
        # For AUD to USD conversion, rate should be around 0.6-0.8
        if invoice_currency == 'AUD' and rate_card_currency == 'USD':
            if exchange_rate < 0.5 or exchange_rate > 0.9:
                issues.append(AuditIssue(
                    charge_type='exchange_rate',
                    severity=AuditSeverity.INFO,
                    message=f"Exchange rate {exchange_rate} outside typical AUD/USD range (0.6-0.8)",
                    invoice_amount=exchange_rate
                ))
        
        return {
            'conversion_needed': True,
            'from_currency': invoice_currency,
            'to_currency': rate_card_currency,
            'exchange_rate': exchange_rate,
            'issues': issues
        }
    
    def _analyze_weight_charges(self, invoice: Dict[str, Any], rate_card: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze weight and determine chargeable weight (higher of ship_weight or bill_weight)"""
        ship_weight = float(invoice.get('ship_weight', 0) or 0)
        bill_weight = float(invoice.get('bill_weight', 0) or 0)
        actual_weight = float(invoice.get('weight', 0) or 0)
        
        # Use higher of ship_weight or bill_weight as chargeable weight
        chargeable_weight = max(ship_weight, bill_weight)
        
        # If no weight data, use actual weight
        if chargeable_weight == 0:
            chargeable_weight = actual_weight
        
        issues = []
        
        # Check for missing weight data
        if chargeable_weight == 0:
            issues.append(AuditIssue(
                charge_type='weight',
                severity=AuditSeverity.ERROR,
                message="No weight data available for rate calculation",
                invoice_amount=0
            ))
        
        # Check for significant difference between ship and bill weight
        if ship_weight > 0 and bill_weight > 0:
            weight_diff_percent = abs(ship_weight - bill_weight) / max(ship_weight, bill_weight) * 100
            if weight_diff_percent > 20:  # More than 20% difference
                issues.append(AuditIssue(
                    charge_type='weight_variance',
                    severity=AuditSeverity.INFO,
                    message=f"Significant difference between ship weight ({ship_weight} kg) and bill weight ({bill_weight} kg)",
                    variance_percent=weight_diff_percent
                ))
        
        return {
            'ship_weight': ship_weight,
            'bill_weight': bill_weight,
            'actual_weight': actual_weight,
            'chargeable_weight': chargeable_weight,
            'weight_used': 'bill_weight' if bill_weight >= ship_weight else 'ship_weight',
            'issues': issues
        }
    
    def _analyze_location_match(self, invoice: Dict[str, Any], rate_card: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze how well invoice location matches rate card"""
        invoice_origin = {
            'country': invoice.get('shipper_country'),
            'city': invoice.get('shipper_city'),
            'port': invoice.get('origin_port')
        }
        
        invoice_dest = {
            'country': invoice.get('consignee_country'),
            'city': invoice.get('consignee_city'),
            'port': invoice.get('destination_port')
        }
        
        rate_card_origin = {
            'country': rate_card.get('origin_country'),
            'region': rate_card.get('origin_region'),
            'cities': rate_card.get('cities_included_origin', ''),
            'port': rate_card.get('origin_port_code')
        }
        
        rate_card_dest = {
            'country': rate_card.get('destination_country'),
            'region': rate_card.get('destination_region'),
            'cities': rate_card.get('cities_included_dest', ''),
            'port': rate_card.get('destination_port_code')
        }
        
        # Determine match quality
        origin_match = self._calculate_location_match_score(invoice_origin, rate_card_origin)
        dest_match = self._calculate_location_match_score(invoice_dest, rate_card_dest)
        
        overall_match_score = (origin_match['score'] + dest_match['score']) / 2
        
        issues = []
        if overall_match_score < 0.7:  # Less than 70% match
            issues.append(AuditIssue(
                charge_type='location_match',
                severity=AuditSeverity.WARNING,
                message=f"Location match quality is low ({overall_match_score:.1%}). Rate card may not be optimal.",
                variance_percent=100 - (overall_match_score * 100)
            ))
        
        return {
            'invoice_route': f"{invoice_origin['country']}-{invoice_dest['country']}",
            'rate_card_route': f"{rate_card_origin['country']}-{rate_card_dest['country']}",
            'origin_match': origin_match,
            'destination_match': dest_match,
            'overall_match_score': overall_match_score,
            'issues': issues
        }
    
    def _calculate_location_match_score(self, invoice_location: Dict, rate_card_location: Dict) -> Dict[str, Any]:
        """Calculate how well an invoice location matches a rate card location"""
        score = 0.0
        match_type = "no_match"
        
        # Exact country match
        if invoice_location['country'] and invoice_location['country'] == rate_card_location['country']:
            score = 0.8
            match_type = "country_match"
            
            # Bonus for city match
            if (invoice_location['city'] and rate_card_location['cities'] and 
                invoice_location['city'].upper() in rate_card_location['cities'].upper()):
                score = 1.0
                match_type = "city_match"
            
            # Bonus for port match
            elif (invoice_location['port'] and rate_card_location['port'] and
                  invoice_location['port'] == rate_card_location['port']):
                score = 1.0
                match_type = "port_match"
        
        # Regional match (partial)
        elif (invoice_location['country'] and rate_card_location['region'] and
              invoice_location['country'] in rate_card_location['region']):
            score = 0.6
            match_type = "region_match"
        
        return {
            'score': score,
            'match_type': match_type,
            'invoice_location': invoice_location,
            'rate_card_location': rate_card_location
        }
    
    def _analyze_charge_breakdown(self, invoice: Dict[str, Any], rate_card: Dict[str, Any], chargeable_weight: float) -> Dict[str, Any]:
        """Analyze individual charges against rate card with detailed breakdown"""
        issues = []
        
        # Get exchange rate for conversion
        exchange_rate = float(invoice.get('exchange_rate', 1.0))
        
        # Get detailed charges from database
        detailed_charges = self._get_detailed_charges(invoice['id'])
        
        # Perform granular charge analysis
        charge_analysis = self._perform_granular_charge_analysis(
            detailed_charges, rate_card, chargeable_weight, exchange_rate
        )
        
        # Add issues from detailed analysis
        issues.extend(charge_analysis['issues'])
        
        # Calculate totals
        actual_total = float(invoice.get('total_charges', 0))
        expected_total = charge_analysis['total_expected']
        variance = actual_total - expected_total
        variance_percent = (abs(variance) / expected_total * 100) if expected_total > 0 else 0
        
        # Add overall variance issue if significant
        if variance_percent > self.VARIANCE_THRESHOLDS['approved']:
            severity = (AuditSeverity.ERROR if variance_percent > self.VARIANCE_THRESHOLDS['review'] 
                       else AuditSeverity.WARNING if variance_percent > self.VARIANCE_THRESHOLDS['pending']
                       else AuditSeverity.INFO)
            
            issues.append(AuditIssue(
                charge_type='total_charges',
                severity=severity,
                message=f"Overall charge variance: AUD {variance:+.2f} ({variance_percent:.1f}%)",
                invoice_amount=actual_total,
                expected_amount=expected_total,
                variance_amount=variance,
                variance_percent=variance_percent
            ))
        
        return {
            **charge_analysis,
            'actual_total': actual_total,
            'variance': variance,
            'variance_percent': variance_percent,
            'chargeable_weight': chargeable_weight,
            'issues': issues
        }
    
    def _get_detailed_charges(self, invoice_id: int) -> List[Dict[str, Any]]:
        """Get detailed charge breakdown from database"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        
        cursor = conn.execute("""
            SELECT charge_type, description, amount, rate, quantity, unit
            FROM charges 
            WHERE invoice_id = ?
            ORDER BY amount DESC
        """, (invoice_id,))
        
        charges = [dict(row) for row in cursor.fetchall()]
        conn.close()
        
        return charges
    
    def _perform_granular_charge_analysis(self, detailed_charges: List[Dict], rate_card: Dict, 
                                        chargeable_weight: float, exchange_rate: float) -> Dict[str, Any]:
        """Perform detailed charge-by-charge analysis"""
        issues = []
        charge_breakdown = {}
        
        # Define charge type mappings
        charge_mappings = {
            'AIR': 'air_freight',
            'FUE': 'fuel_surcharge', 
            'FUEL': 'fuel_surcharge',
            'THC': 'terminal_handling',
            'CHC': 'handling_charge',
            'DEL': 'delivery_charge',
            'PUC': 'pickup_charge',
            '360': 'customs_clearance',
            '750': 'vat_tax'
        }
        
        # Calculate expected charges from rate card
        expected_charges = self._calculate_expected_charges_detailed(rate_card, chargeable_weight, exchange_rate)
        
        # Analyze each charge type
        for charge in detailed_charges:
            charge_type = charge['charge_type']
            charge_desc = charge['description'] or ''
            actual_amount = float(charge['amount'] or 0)
            
            # Map to standard charge categories
            standard_type = charge_mappings.get(charge_type, charge_type.lower() if charge_type else 'other')
            
            # Get expected amount for this charge type
            expected_amount = expected_charges.get(standard_type, 0)
            
            # Calculate variance
            variance = actual_amount - expected_amount
            variance_percent = (abs(variance) / expected_amount * 100) if expected_amount > 0 else 0
            
            # Store breakdown
            charge_breakdown[standard_type] = {
                'charge_type': charge_type,
                'description': charge_desc,
                'actual_amount': actual_amount,
                'expected_amount': expected_amount,
                'variance': variance,
                'variance_percent': variance_percent
            }
            
            # Add specific charge issues
            issues.extend(self._analyze_specific_charge(
                standard_type, charge_type, charge_desc, actual_amount, 
                expected_amount, variance, variance_percent, chargeable_weight
            ))
        
        # Check for missing expected charges
        for exp_type, exp_amount in expected_charges.items():
            if exp_amount > 0 and exp_type not in charge_breakdown:
                issues.append(AuditIssue(
                    charge_type=exp_type,
                    severity=AuditSeverity.WARNING,
                    message=f"Expected {exp_type.replace('_', ' ').title()} charge missing (${exp_amount:.2f})",
                    expected_amount=exp_amount
                ))
        
        return {
            'charge_breakdown': charge_breakdown,
            'expected_charges': expected_charges,
            'total_expected': sum(expected_charges.values()),
            'issues': issues
        }
    
    def _calculate_expected_charges_detailed(self, rate_card: Dict[str, Any], weight: float, exchange_rate: float) -> Dict[str, float]:
        """Calculate detailed expected charges from rate card"""
        expected = {}
        
        if weight <= 0:
            return expected
        
        # Air freight charge based on weight bracket
        if weight < 1000:
            base_rate_usd = float(rate_card.get('base_rate_lt1000kg') or 0)
        elif weight < 2000:
            base_rate_usd = float(rate_card.get('base_rate_1000to2000kg') or 0)
        elif weight < 3000:
            base_rate_usd = float(rate_card.get('base_rate_2000to3000kg') or 0)
        else:
            base_rate_usd = float(rate_card.get('base_rate_gt3000kg') or 0)
        
        # Convert to AUD and calculate
        base_rate_aud = base_rate_usd / exchange_rate if exchange_rate > 0 else base_rate_usd
        expected['air_freight'] = weight * base_rate_aud
        
        # Fuel surcharge
        fuel_rate_usd = float(rate_card.get('fuel_surcharge') or 0)
        fuel_rate_aud = fuel_rate_usd / exchange_rate if exchange_rate > 0 else fuel_rate_usd
        expected['fuel_surcharge'] = weight * fuel_rate_aud
        
        # Origin and destination fees
        expected['pickup_charge'] = float(rate_card.get('origin_fees') or 0) / exchange_rate if exchange_rate > 0 else 0
        expected['delivery_charge'] = float(rate_card.get('destination_fees') or 0) / exchange_rate if exchange_rate > 0 else 0
        
        # Terminal handling (estimate based on weight and typical rates)
        if weight > 100:  # Only for larger shipments
            expected['terminal_handling'] = weight * 0.50  # Estimate $0.50/kg AUD
        
        # Handling charge (flat fee estimate)
        expected['handling_charge'] = 150.0  # Estimate AUD $150
        
        return expected
    
    def _analyze_specific_charge(self, standard_type: str, charge_type: str, description: str, 
                               actual: float, expected: float, variance: float, variance_percent: float,
                               weight: float) -> List[AuditIssue]:
        """Analyze specific charge types with business logic"""
        issues = []
        
        # Air freight analysis
        if standard_type == 'air_freight':
            if expected > 0 and variance_percent > 20:
                rate_per_kg = actual / weight if weight > 0 else 0
                expected_rate_per_kg = expected / weight if weight > 0 else 0
                issues.append(AuditIssue(
                    charge_type='air_freight',
                    severity=AuditSeverity.WARNING if variance_percent < 50 else AuditSeverity.ERROR,
                    message=f"Air freight rate variance: AUD {rate_per_kg:.2f}/kg vs expected {expected_rate_per_kg:.2f}/kg",
                    invoice_amount=actual,
                    expected_amount=expected,
                    variance_amount=variance,
                    variance_percent=variance_percent
                ))
        
        # Fuel surcharge analysis
        elif standard_type == 'fuel_surcharge':
            if expected > 0 and variance_percent > 30:
                issues.append(AuditIssue(
                    charge_type='fuel_surcharge',
                    severity=AuditSeverity.INFO,
                    message=f"Fuel surcharge variance: actual ${actual:.2f} vs expected ${expected:.2f}",
                    invoice_amount=actual,
                    expected_amount=expected,
                    variance_amount=variance,
                    variance_percent=variance_percent
                ))
        
        # Terminal handling analysis
        elif standard_type == 'terminal_handling':
            if actual > weight * 2.0:  # More than $2/kg seems high
                issues.append(AuditIssue(
                    charge_type='terminal_handling',
                    severity=AuditSeverity.WARNING,
                    message=f"Terminal handling charge seems high: ${actual:.2f} (${actual/weight:.2f}/kg)",
                    invoice_amount=actual
                ))
        
        # Tax charges (not in rate card, but validate reasonableness)
        elif standard_type == 'vat_tax':
            if actual > 0:
                # VAT should typically be 10% in Australia
                estimated_base = actual / 0.10
                issues.append(AuditIssue(
                    charge_type='vat_tax',
                    severity=AuditSeverity.INFO,
                    message=f"VAT charge: ${actual:.2f} (implies taxable base of ~${estimated_base:.2f})",
                    invoice_amount=actual
                ))
        
        # Customs charges (regulatory, hard to validate against rate card)
        elif standard_type == 'customs_clearance':
            if actual > 500:  # Flag if customs charges are very high
                issues.append(AuditIssue(
                    charge_type='customs_clearance',
                    severity=AuditSeverity.INFO,
                    message=f"High customs clearance charges: ${actual:.2f}",
                    invoice_amount=actual
                ))
        
        # General variance check for other charges
        elif expected > 0 and variance_percent > 25:
            issues.append(AuditIssue(
                charge_type=standard_type,
                severity=AuditSeverity.WARNING,
                message=f"{description} variance: ${variance:+.2f} ({variance_percent:.1f}%)",
                invoice_amount=actual,
                expected_amount=expected,
                variance_amount=variance,
                variance_percent=variance_percent
            ))
        
        return issues
    
    def _calculate_expected_freight(self, weight: float, rate_card: Dict[str, Any], exchange_rate: float) -> float:
        """Calculate expected freight charge based on weight brackets"""
        if weight <= 0:
            return 0.0
        
        # Get rate based on weight bracket (rates in USD, convert to AUD)
        # Handle None values by defaulting to 0
        if weight < 1000:
            rate_usd = float(rate_card.get('base_rate_lt1000kg') or 0)
        elif weight < 2000:
            rate_usd = float(rate_card.get('base_rate_1000to2000kg') or 0)
        elif weight < 3000:
            rate_usd = float(rate_card.get('base_rate_2000to3000kg') or 0)
        else:
            rate_usd = float(rate_card.get('base_rate_gt3000kg') or 0)
        
        # Convert USD rate to AUD and calculate freight
        rate_aud = rate_usd / exchange_rate if exchange_rate > 0 else rate_usd
        freight_charge = weight * rate_aud
        
        return freight_charge
    
    def _calculate_fuel_surcharge(self, weight: float, rate_card: Dict[str, Any], exchange_rate: float) -> float:
        """Calculate fuel surcharge"""
        fuel_rate_usd = float(rate_card.get('fuel_surcharge') or 0)
        fuel_rate_aud = fuel_rate_usd / exchange_rate if exchange_rate > 0 else fuel_rate_usd
        return weight * fuel_rate_aud
    
    def _determine_audit_status(self, variance_percent: float, issues: List[AuditIssue]) -> str:
        """Determine audit status based on variance and issues"""
        # Check for errors first
        if any(issue.severity == AuditSeverity.ERROR for issue in issues):
            return 'error'
        
        # Check variance thresholds
        if variance_percent <= self.VARIANCE_THRESHOLDS['approved']:
            return 'approved'
        elif variance_percent <= self.VARIANCE_THRESHOLDS['pending']:
            return 'pending'
        elif variance_percent <= self.VARIANCE_THRESHOLDS['review']:
            return 'review'
        else:
            return 'error'
    
    def _create_error_result(self, error_message: str, invoice_number: str) -> AuditResult:
        """Create an error audit result"""
        return AuditResult(
            invoice_number=invoice_number,
            audit_status='error',
            total_invoice_amount=0.0,
            total_expected_amount=0.0,
            total_variance=0.0,
            variance_percent=0.0,
            currency_conversion={},
            weight_analysis={},
            location_match={},
            issues=[AuditIssue('audit_error', AuditSeverity.ERROR, error_message)]
        )
    
    def _create_skip_result(self, skip_reason: str, invoice_number: str) -> AuditResult:
        """Create a skip audit result"""
        return AuditResult(
            invoice_number=invoice_number,
            audit_status='skipped',
            total_invoice_amount=0.0,
            total_expected_amount=0.0,
            total_variance=0.0,
            variance_percent=0.0,
            currency_conversion={},
            weight_analysis={},
            location_match={},
            issues=[AuditIssue('audit_skip', AuditSeverity.INFO, skip_reason)]
        )

def save_audit_results(audit_results: List[AuditResult], db_path: str = 'dhl_audit.db'):
    """Save audit results to database"""
    conn = sqlite3.connect(db_path)
    
    # Create audit results table if not exists
    conn.execute('''
        CREATE TABLE IF NOT EXISTS detailed_audit_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_number TEXT,
            audit_status TEXT,
            total_invoice_amount REAL,
            total_expected_amount REAL,
            total_variance REAL,
            variance_percent REAL,
            rate_card_used TEXT,
            audit_details TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Clear existing results
    conn.execute('DELETE FROM detailed_audit_results')
    
    # Insert new results
    for result in audit_results:
        audit_details = {
            'currency_conversion': result.currency_conversion,
            'weight_analysis': result.weight_analysis,
            'location_match': result.location_match,
            'issues': [
                {
                    'charge_type': issue.charge_type,
                    'severity': issue.severity.value,
                    'message': issue.message,
                    'invoice_amount': issue.invoice_amount,
                    'expected_amount': issue.expected_amount,
                    'variance_amount': issue.variance_amount,
                    'variance_percent': issue.variance_percent
                }
                for issue in result.issues
            ]
        }
        
        conn.execute('''
            INSERT INTO detailed_audit_results 
            (invoice_number, audit_status, total_invoice_amount, total_expected_amount,
             total_variance, variance_percent, rate_card_used, audit_details)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            result.invoice_number,
            result.audit_status,
            result.total_invoice_amount,
            result.total_expected_amount,
            result.total_variance,
            result.variance_percent,
            result.rate_card_used,
            str(audit_details)
        ))
    
    # Update main invoices table with audit status
    for result in audit_results:
        conn.execute('''
            UPDATE invoices 
            SET audit_status = ?, 
                audit_notes = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE invoice_number = ?
        ''', (
            result.audit_status,
            f"Variance: {result.variance_percent:.1f}%, Issues: {len(result.issues)}",
            result.invoice_number
        ))
    
    conn.commit()
    conn.close()
    
    logger.info(f"Saved audit results for {len(audit_results)} invoices")

# Convenience function for running complete audit
def run_comprehensive_audit():
    """Run comprehensive audit on all invoices"""
    auditor = InvoiceAuditor()
    results = auditor.audit_all_invoices()
    save_audit_results(results)
    
    # Print summary
    print(f"\n=== Comprehensive Invoice Audit Summary ===")
    print(f"Total invoices audited: {len(results)}")
    
    status_counts = {}
    for result in results:
        status_counts[result.audit_status] = status_counts.get(result.audit_status, 0) + 1
    
    for status, count in status_counts.items():
        print(f"{status.title()}: {count} invoices")
    
    # Show significant variances
    significant_variances = [r for r in results if r.variance_percent > 10 and r.audit_status != 'skipped']
    if significant_variances:
        print(f"\n=== Significant Variances (>10%) ===")
        for result in significant_variances[:5]:  # Top 5
            print(f"{result.invoice_number}: {result.variance_percent:.1f}% variance "
                  f"(AUD {result.total_variance:+.2f})")
    
    return results

if __name__ == "__main__":
    # Run audit when script is called directly
    results = run_comprehensive_audit()
