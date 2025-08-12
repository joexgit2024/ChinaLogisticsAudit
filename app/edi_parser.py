import re
from datetime import datetime
from typing import List, Dict, Any

class EDIParser:
    """Parser for X12 EDI format files containing DHL invoice data."""
    
    def __init__(self):
        self.segment_separator = '~'
        self.element_separator = '*'
        self.sub_element_separator = ':'
        # Support alternative delimiters
        self.alt_segment_separator = ','
        
    def parse_edi_content(self, content: str) -> List[Dict[str, Any]]:
        """Parse EDI content and extract invoice data."""
        invoices = []
        
        try:
            # Detect segment separator
            if content.count(',') > content.count('~'):
                self.segment_separator = ','
            
            # Split into segments
            segments = content.split(self.segment_separator)
            segments = [seg.strip() for seg in segments if seg.strip()]
            
            current_invoice = None
            current_charges = []
            
            # Extract header information from ISA and GS segments
            isa_info = {}
            gs_info = {}
            
            for segment in segments:
                elements = segment.split(self.element_separator)
                segment_id = elements[0] if elements else ''
                
                if segment_id == 'ISA' and len(elements) >= 16:
                    # Interchange Control Header
                    # ISA*00*          *00*          *ZZ*DGFAFR         *ZZ*ANDAAUAIR      *250630*0425*U*00401*000006764*0*P*>,
                    isa_info = {
                        'sender_id': elements[6].strip(),
                        'receiver_id': elements[8].strip(),
                        'date': elements[9],
                        'time': elements[10]
                    }
                    
                    # Extract client from receiver_id (ANDAAUAIR -> ANDA)
                    receiver = elements[8].strip()
                    if len(receiver) >= 4:
                        # First 4 characters typically represent client code
                        client_code = receiver[:4]
                        isa_info['client_code'] = client_code
                    break
                elif segment_id == 'GS' and len(elements) >= 8:
                    # Functional Group Header
                    gs_info = {
                        'functional_id': elements[1],
                        'sender_code': elements[2],
                        'receiver_code': elements[3],
                        'date': elements[4],
                        'time': elements[5]
                    }
                    break
            
            for segment in segments:
                elements = segment.split(self.element_separator)
                segment_id = elements[0] if elements else ''
                
                if segment_id == 'ST':
                    # Start of transaction set
                    if len(elements) >= 3:
                        transaction_type = elements[1]
                        if transaction_type in ['210', '310', '110']:  # Support freight invoice types
                            current_invoice = self._init_invoice()
                            current_charges = []
                            
                            # Apply header information to invoice
                            if isa_info:
                                current_invoice['client_code'] = isa_info.get('client_code', '')
                                current_invoice['carrier_code'] = isa_info.get('sender_id', '').strip()
                            
                            if gs_info:
                                current_invoice['account_number'] = gs_info.get('receiver_code', '').strip()
                                current_invoice['carrier_name'] = gs_info.get('sender_code', '').strip()
                
                elif segment_id == 'B3':
                    # Beginning segment for carriers invoice
                    if current_invoice and len(elements) >= 2:
                        # For 310 transactions, invoice number might be in different position
                        if len(elements) >= 4 and elements[2]:
                            current_invoice['invoice_number'] = elements[2]
                            current_invoice['pro_number'] = elements[3] if len(elements) > 3 else elements[2]
                        elif len(elements) >= 2:
                            current_invoice['invoice_number'] = elements[1]
                            current_invoice['pro_number'] = elements[2] if len(elements) > 2 else elements[1]
                        
                        # Extract additional B3 information
                        if len(elements) >= 6:
                            current_invoice['billed_to_type'] = elements[4] if elements[4] else ''
                            # Invoice date might be in position 6
                            if elements[5]:
                                current_invoice['invoice_date'] = elements[5]
                            elif len(elements) >= 7 and elements[6]:
                                current_invoice['invoice_date'] = elements[6]
                        
                        # Extract invoice amount if present - check multiple positions
                        if len(elements) >= 8 and elements[7]:
                            try:
                                current_invoice['invoice_amount'] = float(elements[7]) / 100.0  # Often in cents
                            except (ValueError, TypeError):
                                pass
                        elif len(elements) >= 7 and elements[6]:
                            try:
                                # Sometimes amount is in position 6
                                if elements[6].isdigit() and len(elements[6]) > 4:
                                    current_invoice['invoice_amount'] = float(elements[6]) / 100.0
                            except (ValueError, TypeError):
                                pass
                        
                        # Extract carrier code if present - check multiple positions
                        if len(elements) >= 12 and elements[11]:
                            current_invoice['carrier_code'] = elements[11]
                        elif len(elements) >= 9 and elements[8]:
                            current_invoice['carrier_code'] = elements[8]
                        
                        # Extract Incoterm if present (usually last element in B3)
                        if len(elements) >= 15 and elements[14]:
                            current_invoice['incoterm'] = elements[14]
                        elif len(elements) >= 14 and elements[13]:
                            current_invoice['incoterm'] = elements[13]
                        elif len(elements) >= 13 and elements[12]:
                            current_invoice['incoterm'] = elements[12]
                
                elif segment_id == 'B2A':
                    # Set purpose
                    if current_invoice and len(elements) >= 2:
                        current_invoice['reference_number'] = elements[1]
                
                elif segment_id == 'N1':
                    # Party identification
                    if current_invoice and len(elements) >= 3:
                        party_qualifier = elements[1]
                        party_name = elements[2]
                        party_id = elements[4] if len(elements) > 4 else ''
                        
                        # Store current party context for subsequent N3/N4 segments
                        current_invoice['_current_party'] = party_qualifier
                        
                        if party_qualifier == 'RI':  # Carrier/Responsible party
                            current_invoice['carrier_name'] = party_name
                        elif party_qualifier == 'BT':  # Bill-to party
                            current_invoice['bill_to_name'] = party_name
                            if len(elements) > 3 and elements[3]:
                                current_invoice['bill_to_code'] = elements[3]
                            # Extract account number from position 4 (AUOU0003)
                            if len(elements) > 4 and elements[4]:
                                current_invoice['account_number'] = elements[4]
                        elif party_qualifier == 'SH':  # Shipper
                            current_invoice['shipper_name'] = party_name
                        elif party_qualifier == 'CN':  # Consignee
                            current_invoice['consignee_name'] = party_name
                        elif party_qualifier == 'SF':  # Ship from
                            if not current_invoice.get('shipper_name'):
                                current_invoice['shipper_name'] = party_name
                        elif party_qualifier == 'ST':  # Ship to
                            if not current_invoice.get('consignee_name'):
                                current_invoice['consignee_name'] = party_name
                
                elif segment_id == 'N3':
                    # Address information
                    if current_invoice and len(elements) >= 2:
                        address_line1 = elements[1]
                        address_line2 = elements[2] if len(elements) > 2 else ''
                        full_address = f"{address_line1} {address_line2}".strip()
                        
                        # Assign to current party based on last N1 segment
                        current_party = current_invoice.get('_current_party')
                        if current_party == 'RI':  # Carrier
                            current_invoice['carrier_address'] = full_address
                        elif current_party == 'BT':  # Bill-to
                            current_invoice['bill_to_address'] = full_address
                        elif current_party == 'SH':  # Shipper
                            current_invoice['shipper_address'] = full_address
                        elif current_party == 'CN':  # Consignee
                            current_invoice['consignee_address'] = full_address
                        elif current_party == 'SF':  # Ship from
                            if not current_invoice.get('shipper_address'):
                                current_invoice['shipper_address'] = full_address
                        elif current_party == 'ST':  # Ship to
                            if not current_invoice.get('consignee_address'):
                                current_invoice['consignee_address'] = full_address
                
                elif segment_id == 'N4':
                    # Geographic location
                    if current_invoice and len(elements) >= 2:
                        city = elements[1]
                        state = elements[2] if len(elements) > 2 else ''
                        postal_code = elements[3] if len(elements) > 3 else ''
                        country = elements[4] if len(elements) > 4 else ''
                        
                        # Assign to current party based on last N1 segment
                        current_party = current_invoice.get('_current_party')
                        if current_party == 'RI':  # Carrier
                            current_invoice['carrier_city'] = city
                            current_invoice['carrier_state'] = state
                            current_invoice['carrier_postal_code'] = postal_code
                            current_invoice['carrier_country'] = country
                        elif current_party == 'BT':  # Bill-to
                            current_invoice['bill_to_city'] = city
                            current_invoice['bill_to_state'] = state
                            current_invoice['bill_to_postal_code'] = postal_code
                            current_invoice['bill_to_country'] = country
                        elif current_party == 'SH':  # Shipper
                            current_invoice['shipper_city'] = city
                            current_invoice['shipper_state'] = state
                            current_invoice['shipper_postal_code'] = postal_code
                            current_invoice['shipper_country'] = country
                        elif current_party == 'CN':  # Consignee
                            current_invoice['consignee_city'] = city
                            current_invoice['consignee_state'] = state
                            current_invoice['consignee_postal_code'] = postal_code
                            current_invoice['consignee_country'] = country
                        elif current_party == 'SF':  # Ship from
                            if not current_invoice.get('shipper_city'):
                                current_invoice['shipper_city'] = city
                                current_invoice['shipper_state'] = state
                                current_invoice['shipper_postal_code'] = postal_code
                                current_invoice['shipper_country'] = country
                        elif current_party == 'ST':  # Ship to
                            if not current_invoice.get('consignee_city'):
                                current_invoice['consignee_city'] = city
                                current_invoice['consignee_state'] = state
                                current_invoice['consignee_postal_code'] = postal_code
                                current_invoice['consignee_country'] = country
                        
                        # Also set origin/destination for routing display
                        if current_party in ['SH', 'SF'] and not current_invoice.get('origin_city'):
                            current_invoice['origin_city'] = f"{city}, {state}".strip(', ')
                        elif current_party in ['CN', 'ST'] and not current_invoice.get('destination_city'):
                            current_invoice['destination_city'] = f"{city}, {state}".strip(', ')
                
                elif segment_id == 'G62':
                    # Date/time
                    if current_invoice and len(elements) >= 3:
                        date_qualifier = elements[1]
                        date_value = elements[2]
                        
                        # Convert YYYYMMDD to readable format
                        try:
                            if len(date_value) == 8:
                                formatted_date = f"{date_value[:4]}-{date_value[4:6]}-{date_value[6:8]}"
                                
                                if date_qualifier == '10':  # Ship date
                                    current_invoice['service_date'] = formatted_date
                                elif date_qualifier == '17':  # Delivery date
                                    current_invoice['delivery_date'] = formatted_date
                        except:
                            pass
                
                elif segment_id == 'N1':
                    # Name segment
                    if current_invoice and len(elements) >= 3:
                        entity_identifier = elements[1]
                        name = elements[2]
                        
                        if entity_identifier == 'SH':  # Shipper
                            current_invoice['shipper_name'] = name
                        elif entity_identifier == 'CN':  # Consignee
                            current_invoice['consignee_name'] = name
                        elif entity_identifier == 'BT':  # Bill To
                            current_invoice['bill_to_name'] = name

                elif segment_id == 'N3':
                    # Address segment
                    if current_invoice and len(elements) >= 2:
                        address = elements[1]
                        if len(elements) > 2 and elements[2]:
                            address += f" {elements[2]}"
                        
                        # Assign to the last entity that was parsed
                        if current_invoice.get('shipper_name') and not current_invoice.get('shipper_address'):
                            current_invoice['shipper_address'] = address
                        elif current_invoice.get('consignee_name') and not current_invoice.get('consignee_address'):
                            current_invoice['consignee_address'] = address
                        elif current_invoice.get('bill_to_name') and not current_invoice.get('bill_to_address'):
                            current_invoice['bill_to_address'] = address

                elif segment_id == 'N4':
                    # Geographic location segment
                    if current_invoice and len(elements) >= 2:
                        city = elements[1] if elements[1] else ''
                        state = elements[2] if len(elements) > 2 and elements[2] else ''
                        postal_code = elements[3] if len(elements) > 3 and elements[3] else ''
                        country = elements[4] if len(elements) > 4 and elements[4] else ''
                        
                        # Assign to the last entity that was parsed
                        if current_invoice.get('shipper_name') and not current_invoice.get('shipper_city'):
                            current_invoice['shipper_city'] = city
                            current_invoice['shipper_state'] = state
                            current_invoice['shipper_postal_code'] = postal_code
                            current_invoice['shipper_country'] = country
                        elif current_invoice.get('consignee_name') and not current_invoice.get('consignee_city'):
                            current_invoice['consignee_city'] = city
                            current_invoice['consignee_state'] = state
                            current_invoice['consignee_postal_code'] = postal_code
                            current_invoice['consignee_country'] = country
                        elif current_invoice.get('bill_to_name') and not current_invoice.get('bill_to_city'):
                            current_invoice['bill_to_city'] = city
                            current_invoice['bill_to_state'] = state
                            current_invoice['bill_to_postal_code'] = postal_code
                            current_invoice['bill_to_country'] = country

                elif segment_id == 'V1':
                    # Vessel identification
                    if current_invoice and len(elements) >= 3:
                        current_invoice['vessel_name'] = elements[2]

                elif segment_id == 'M7':
                    # Container information
                    if current_invoice and len(elements) >= 2:
                        current_invoice['container_number'] = elements[1]

                elif segment_id == 'N9':
                    # Reference identification
                    if current_invoice and len(elements) >= 3:
                        ref_type = elements[1]
                        ref_value = elements[2]
                        
                        # Store specific reference types
                        if ref_type == 'BN':  # Booking Number
                            current_invoice['booking_number'] = ref_value
                        elif ref_type == 'BM':  # Bill of Lading
                            current_invoice['bill_of_lading'] = ref_value
                        elif ref_type == 'AW':  # Air Waybill/Tracking Number
                            current_invoice['tracking_number'] = ref_value
                        elif ref_type == 'Q8':  # Customer VAT Registration
                            current_invoice['customer_vat_registration'] = ref_value.replace(' ', '')
                        elif ref_type == 'QT':  # Carrier VAT Registration
                            current_invoice['carrier_vat_registration'] = ref_value.replace(' ', '')
                        elif ref_type == 'CR':  # Customer Reference
                            current_invoice['reference_number'] = ref_value
                        elif ref_type == 'SI':  # Shipment Identification
                            current_invoice['reference_number'] = ref_value
                        elif ref_type == '8U':  # Location Code
                            current_invoice['sap_plant'] = ref_value
                        elif ref_type == 'MB':  # Master Bill
                            current_invoice['bill_of_lading'] = ref_value
                        elif ref_type == 'AF':  # Service Code
                            current_invoice['service_type'] = ref_value
                        
                        # Store all reference numbers
                        current_invoice['reference_numbers'].append({
                            'reference_type': ref_type,
                            'reference_value': ref_value
                        })

                elif segment_id == 'N7':
                    # Equipment details
                    if current_invoice and len(elements) >= 4:
                        # N7 segment: N7*UETU*535655*14014.9*G****0*X**4B******K*2****45G0**FCL
                        # Service type is often in the last element
                        last_element = elements[-1] if elements else ''
                        if last_element in ['FCL', 'LCL']:
                            current_invoice['service_type'] = last_element
                        
                        # Equipment weight
                        if elements[3]:
                            try:
                                current_invoice['bill_weight'] = float(elements[3])
                            except (ValueError, TypeError):
                                pass
                        
                        # Set shipping mode to OCEAN if we see container equipment
                        if elements[1] and elements[1].startswith(('UET', 'CON')):
                            current_invoice['shipping_mode'] = 'OCEAN'

                elif segment_id == 'R4':
                    # Port or terminal
                    if current_invoice and len(elements) >= 5:
                        port_function = elements[1]
                        port_name = elements[4]
                        
                        if port_function == 'L':  # Loading port
                            current_invoice['origin_port'] = port_name
                        elif port_function == 'D':  # Discharge port
                            current_invoice['destination_port'] = port_name

                elif segment_id == 'P1':
                    # Pickup date - P1*SD*20250611*011
                    if current_invoice and len(elements) >= 3:
                        date_value = elements[2]
                        try:
                            if len(date_value) == 8:
                                formatted_date = f"{date_value[0:4]}-{date_value[4:6]}-{date_value[6:8]}"
                                current_invoice['pickup_date'] = formatted_date
                                current_invoice['ship_date'] = formatted_date
                        except:
                            pass

                elif segment_id == 'POD':
                    # Proof of delivery date - POD*20250625*0856*GANESH DNZ
                    if current_invoice and len(elements) >= 2:
                        date_value = elements[1]
                        try:
                            if len(date_value) == 8:
                                formatted_date = f"{date_value[0:4]}-{date_value[4:6]}-{date_value[6:8]}"
                                current_invoice['delivery_date'] = formatted_date
                        except:
                            pass

                elif segment_id == 'L3':
                    # Weight and measurement - L3*92*B***120975*******K
                    if current_invoice and len(elements) >= 2:
                        # Bill weight is in position 1
                        if elements[1]:
                            try:
                                current_invoice['bill_weight'] = float(elements[1])
                                # If no weight set yet, use this as main weight
                                if not current_invoice.get('weight'):
                                    current_invoice['weight'] = float(elements[1])
                            except (ValueError, TypeError):
                                pass

                elif segment_id == 'L4':
                    # Dimensions and measurements - L4*109*92*55*C*1
                    if current_invoice and len(elements) >= 2:
                        # Sometimes contains additional weight information
                        if len(elements) >= 3 and elements[2]:
                            try:
                                # Could be another weight measurement
                                weight_val = float(elements[2])
                                if weight_val > 0 and not current_invoice.get('bill_weight'):
                                    current_invoice['bill_weight'] = weight_val
                            except (ValueError, TypeError):
                                pass

                elif segment_id == 'L10':
                    # Weight information - L10*49*G*K or L10*92*A1*K
                    if current_invoice and len(elements) >= 2:
                        weight_val = elements[1]
                        weight_unit = elements[2] if len(elements) > 2 else ''
                        
                        try:
                            weight = float(weight_val)
                            if weight > 0:
                                # L10 with 'G' unit is usually gross weight (ship weight)
                                if weight_unit == 'G':
                                    current_invoice['ship_weight'] = weight
                                    # If no main weight set, use this
                                    if not current_invoice.get('weight'):
                                        current_invoice['weight'] = weight
                                # Other L10 segments might be additional weight info
                                elif weight_unit in ['A1', 'B'] and not current_invoice.get('bill_weight'):
                                    current_invoice['bill_weight'] = weight
                        except (ValueError, TypeError):
                            pass

                elif segment_id == 'DTM':
                    # Date/time reference
                    if current_invoice and len(elements) >= 3:
                        date_qualifier = elements[1]
                        date_value = elements[2]
                        
                        # Convert YYYYMMDD to readable format
                        try:
                            if len(date_value) == 8:
                                formatted_date = f"{date_value[0:4]}-{date_value[4:6]}-{date_value[6:8]}"
                                
                                if date_qualifier == '140':  # Pickup/Ship date
                                    current_invoice['pickup_date'] = formatted_date
                                    current_invoice['ship_date'] = formatted_date  # Also store as ship_date
                                elif date_qualifier == '139':  # Delivery date
                                    current_invoice['delivery_date'] = formatted_date
                                elif date_qualifier == '011':  # Ship date
                                    current_invoice['ship_date'] = formatted_date
                        except:
                            pass

                elif segment_id == 'L0':
                    # Line item - quantity and weight
                    # L0*1***49*G***1*PCS**K
                    # Position: [L0, line_num, ?, ?, weight_value, weight_unit, ?, quantity, quantity_unit, ?, unit]
                    if current_invoice and len(elements) >= 4:
                        line_number = elements[1]
                        
                        # Extract weight from position 4 (0-indexed) - this is the actual weight
                        weight = 0.0
                        if len(elements) > 4 and elements[4]:
                            try:
                                weight = float(elements[4])
                            except (ValueError, TypeError):
                                pass
                        
                        # Extract quantity from position 7 (0-indexed) - this is pieces/quantity
                        quantity = 0.0
                        if len(elements) > 7 and elements[7]:
                            try:
                                quantity = float(elements[7])
                            except (ValueError, TypeError):
                                pass
                        
                        # Unit type from position 8 (quantity unit)
                        unit_type = elements[8] if len(elements) > 8 else ''
                        
                        current_invoice['line_items'].append({
                            'line_number': line_number,
                            'quantity': quantity,
                            'weight': weight,
                            'unit_type': unit_type,
                            'description': ''
                        })
                        
                        # Update invoice totals from line items
                        if weight > 0:
                            # Use the line item weight as ship weight if not already set
                            if not current_invoice.get('ship_weight'):
                                current_invoice['ship_weight'] = weight
                            # Add to total weight
                            current_invoice['weight'] = current_invoice.get('weight', 0.0) + weight
                        
                        if quantity > 0:
                            # Add to total pieces
                            current_invoice['pieces'] = current_invoice.get('pieces', 0) + int(quantity)

                elif segment_id == 'L5':
                    # Line item description
                    if current_invoice and current_invoice['line_items'] and len(elements) >= 3:
                        description = elements[2]
                        # Update the last line item with description
                        current_invoice['line_items'][-1]['description'] = description

                elif segment_id == 'C3':
                    # Currency and Exchange Rate segment
                    # Format: C3*[Currency]*[Exchange Rate]*[From Currency]*[To Currency]
                    # Example: C3*AUD*.611224*AUD*USD or C3*AUD*0001*AUD*AUD
                    if current_invoice and len(elements) >= 2:
                        current_invoice['currency'] = elements[1]
                        if len(elements) >= 3 and elements[2]:
                            try:
                                exchange_rate = float(elements[2])
                                
                                # Determine if this is a meaningful exchange rate
                                is_meaningful_rate = False
                                if len(elements) >= 5:
                                    from_curr = elements[3]
                                    to_curr = elements[4]
                                    # Meaningful if different currencies or rate is not 1.0
                                    is_meaningful_rate = (from_curr != to_curr) or (exchange_rate != 1.0)
                                else:
                                    # If no currency info, consider it meaningful if rate != 1.0
                                    is_meaningful_rate = (exchange_rate != 1.0)
                                
                                # Update exchange rate if:
                                # 1. We don't have one yet, OR
                                # 2. Current one is 1.0 and this one is meaningful, OR  
                                # 3. This one is meaningful and replaces existing
                                current_rate = current_invoice.get('exchange_rate', 1.0)
                                if (current_rate == 1.0 and is_meaningful_rate) or not current_invoice.get('exchange_rate'):
                                    current_invoice['exchange_rate'] = exchange_rate
                                    
                                    # Also update currency conversion info for meaningful rates
                                    if len(elements) >= 5:
                                        current_invoice['from_currency'] = elements[3]
                                        current_invoice['to_currency'] = elements[4]
                                        
                            except (ValueError, TypeError):
                                pass

                elif segment_id == 'L5':
                    # Charge description segment
                    if current_invoice and len(elements) >= 2:
                        charge = {
                            'charge_type': elements[1],
                            'description': elements[2] if len(elements) > 2 else '',
                            'rate': None,
                            'quantity': None,
                            'amount': None
                        }
                        current_charges.append(charge)

                elif segment_id == 'L0':
                    # Charge rate and quantity segment
                    if current_charges and len(elements) >= 3:
                        current_charges[-1]['rate'] = float(elements[1]) if elements[1] else None
                        current_charges[-1]['quantity'] = float(elements[2]) if elements[2] else None

                elif segment_id == 'L1':
                    # Charge amount segment
                    if current_invoice and len(elements) >= 9:
                        line_number = elements[1]
                        raw_amount = float(elements[4]) if elements[4] else None
                        charge_code = elements[8] if len(elements) > 8 else ''
                        description = elements[12] if len(elements) > 12 else ''
                        
                        if raw_amount:
                            # All amounts in L1 segments are in cents, divide by 100
                            raw_amount = raw_amount / 100
                        
                        charge = {
                            'charge_type': charge_code,
                            'description': description,
                            'rate': None,
                            'quantity': None,
                            'amount': raw_amount
                        }
                        current_charges.append(charge)
                        
                        # Extract shipping mode from charge descriptions
                        if description and current_invoice:
                            desc_upper = description.upper()
                            if 'AIR FREIGHT' in desc_upper:
                                current_invoice['shipping_mode'] = 'AIR FREIGHT'
                            elif 'OCEAN' in desc_upper or 'SEA' in desc_upper:
                                current_invoice['shipping_mode'] = 'OCEAN'
                            elif 'GROUND' in desc_upper or 'TRUCK' in desc_upper:
                                current_invoice['shipping_mode'] = 'GROUND'

                elif segment_id == 'SE':
                    # End of transaction set
                    if current_invoice:
                        current_invoice['charges'] = current_charges
                        # Post-process invoice data
                        self._post_process_invoice(current_invoice)
                        invoices.append(current_invoice)
                        current_invoice = None
                        current_charges = []
            
            # Handle case where last invoice wasn't closed with SE segment
            if current_invoice:
                current_invoice['charges'] = current_charges
                # Post-process invoice data
                self._post_process_invoice(current_invoice)
                invoices.append(current_invoice)
                
        except Exception as e:
            print(f"Error parsing EDI content: {e}")
            # Try to extract basic information even if full parsing fails
            basic_invoice = self._extract_basic_info(content)
            if basic_invoice:
                invoices.append(basic_invoice)
        
        return invoices
    
    def _init_invoice(self) -> Dict[str, Any]:
        """Initialize a new invoice dictionary."""
        return {
            'invoice_number': '',
            
            # Client/Account Information
            'client_code': '',
            'carrier_code': '',
            'account_number': '',
            'account_period': '',
            'billed_to_type': '',
            
            # Tracking & Status
            'tracking_number': '',
            'invoice_date': '',
            'invoice_status': '',
            'audit_exception_status': '',
            
            # Shipper Information
            'shipper_name': '',
            'shipper_address': '',
            'shipper_city': '',
            'shipper_state': '',
            'shipper_postal_code': '',
            'shipper_country': '',
            
            # Consignee Information
            'consignee_name': '',
            'consignee_address': '',
            'consignee_city': '',
            'consignee_state': '',
            'consignee_postal_code': '',
            'consignee_country': '',
            
            # Bill To Information
            'bill_to_name': '',
            'bill_to_address': '',
            'bill_to_city': '',
            'bill_to_state': '',
            'bill_to_postal_code': '',
            'bill_to_country': '',
            
            # Vessel/Container Information
            'vessel_name': '',
            'container_number': '',
            'bill_of_lading': '',
            'booking_number': '',
            
            # Ports and Routing
            'origin_port': '',
            'destination_port': '',
            
            # Dates
            'pickup_date': '',
            'delivery_date': '',
            'service_date': '',
            'ship_date': '',
            'shipment_entered_date': '',
            'invoice_created_date': '',
            
            # Reference Numbers
            'reference_number': '',
            'pro_number': '',
            
            # Financial Information
            'total_charges': 0.0,
            'net_charge': 0.0,
            'invoice_amount': 0.0,
            'check_number': '',
            'check_date': '',
            
            # Weight and Measurements
            'weight': 0.0,
            'bill_weight': 0.0,
            'ship_weight': 0.0,
            'pieces': 0,
            'volume': 0.0,
            'declared_value': 0.0,
            
            # Currency and Exchange
            'currency': 'USD',
            'exchange_rate': 1.0,
            
            # Service Information
            'shipping_mode': '',
            'service_type': '',
            'delivery_commitment': '',
            'commodity_type': '',
            
            # Business Information
            'vendor_number': '',
            'customer_vat_registration': '',
            'sap_plant': '',
            'shipper_company_code': '',
            'mode': '',
            'allocation_percentage': 100.0,
            'master_shipper_address': '',
            'company_code': '',
            'shipper_description': '',
            'gl_account': '',
            'carrier_name': '',
            'direction': '',
            'charge_group': '',
            'recipient_description': '',
            'partner_bank_type': '',
            'profit_center': '',
            'carrier_vat_registration': '',
            'recipient_type': '',
            'carrier_country': '',
            'shipper_plant': '',
            'tax_code': '',
            
            # System fields
            'description': '',
            'charges': [],
            'reference_numbers': [],
            'line_items': [],
            'charge_codes': []
        }
    
    def _extract_basic_info(self, content: str) -> Dict[str, Any]:
        """Extract basic information if full parsing fails."""
        try:
            basic_invoice = self._init_invoice()
            
            # Try to find invoice number
            invoice_match = re.search(r'B3\*([^*]+)', content)
            if invoice_match:
                basic_invoice['invoice_number'] = invoice_match.group(1)
            
            # Try to find charges
            charge_matches = re.findall(r'L1\*[^*]*\*([0-9.]+)', content)
            total = 0.0
            charges = []
            
            for i, charge in enumerate(charge_matches):
                try:
                    amount = float(charge)
                    total += amount
                    charges.append({
                        'type': f'Charge_{i+1}',
                        'amount': amount,
                        'description': f'Extracted charge {i+1}'
                    })
                except:
                    pass
            
            basic_invoice['total_charges'] = total
            basic_invoice['charges'] = charges
            basic_invoice['shipper_name'] = 'Unknown Shipper'
            basic_invoice['consignee_name'] = 'Unknown Consignee'
            
            return basic_invoice if total > 0 else None
            
        except:
            return None
    
    def _post_process_invoice(self, invoice: Dict[str, Any]) -> None:
        """Post-process invoice data to calculate derived fields."""
        
        # Calculate ship weight from line items weights - use largest weight as main shipment weight
        if invoice.get('line_items'):
            weights = [item.get('weight', 0.0) for item in invoice['line_items'] if item.get('weight', 0.0) > 0]
            if weights:
                invoice['ship_weight'] = max(weights)  # Use largest weight as ship weight
                # Also calculate total weight and pieces from line items
                invoice['weight'] = sum(weights)
                pieces = [item.get('quantity', 0.0) for item in invoice['line_items'] if item.get('quantity', 0.0) > 0]
                if pieces:
                    invoice['pieces'] = int(sum(pieces))
        
        # Extract commodity type from line items descriptions
        if invoice.get('line_items'):
            commodities = []
            for item in invoice['line_items']:
                desc = item.get('description', '').upper()
                if desc and desc not in commodities:
                    commodities.append(desc)
            
            if commodities:
                invoice['commodity_type'] = ', '.join(commodities)
        
        # Set default shipping mode based on service type
        if not invoice.get('shipping_mode') and invoice.get('service_type'):
            if invoice['service_type'] in ['FCL', 'LCL']:
                invoice['shipping_mode'] = 'OCEAN'
    
    def validate_edi_format(self, content: str) -> bool:
        """Validate if content appears to be valid EDI format."""
        # Check for basic EDI structure
        has_segments = '~' in content
        has_elements = '*' in content
        has_transaction_start = 'ST*' in content
        
        return has_segments and has_elements and has_transaction_start
