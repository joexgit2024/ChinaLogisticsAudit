"""
FedEx Rate Card Loader

This module loads rate card data from fedex_analysis.json into the database tables.
It parses the JSON structure and populates all necessary tables for comprehensive
rate card management.
"""

import json
import sqlite3
from datetime import datetime, date
from fedex_rate_card_schema import FedExRateCardSchema
import os

class FedExRateCardLoader:
    def __init__(self, db_path='fedex_audit.db'):
        self.db_path = db_path
        self.schema = FedExRateCardSchema(db_path)
        
    def load_from_json(self, json_file_path='fedex_analysis.json'):
        """Load rate card data from fedex_analysis.json"""
        try:
            if not os.path.exists(json_file_path):
                print(f"Error: {json_file_path} not found")
                return False
            
            # Try different encodings
            encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252']
            data = None
            
            for encoding in encodings:
                try:
                    with open(json_file_path, 'r', encoding=encoding) as f:
                        data = json.load(f)
                    print(f"Successfully loaded with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
                except json.JSONDecodeError as e:
                    print(f"JSON decode error with {encoding}: {e}")
                    continue
            
            if data is None:
                print("Could not load JSON file with any encoding")
                return False
            
            # Ensure tables exist
            self.schema.create_all_tables()
            
            # Load data in order of dependencies
            print("Loading FedEx rate card data...")
            
            # 1. Load regions
            self._load_regions(data)
            
            # 2. Load zones
            self._load_zones(data)
            
            # 3. Load countries
            self._load_countries(data)
            
            # 4. Load service types
            self._load_service_types(data)
            
            # 5. Load rate cards
            self._load_rate_cards(data)
            
            # 6. Load surcharges
            self._load_surcharges(data)
            
            print("FedEx rate card data loaded successfully!")
            return True
            
        except Exception as e:
            print(f"Error loading rate card data: {e}")
            return False
    
    def _load_regions(self, data):
        """Load region data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        regions = []
        
        # Extract regions from zones data if available
        if 'zones' in data:
            zones_data = data['zones']
            for zone_key, zone_info in zones_data.items():
                if isinstance(zone_info, dict) and 'countries' in zone_info:
                    for country_info in zone_info['countries']:
                        if isinstance(country_info, dict):
                            region_code = country_info.get('region_code', 'UNK')
                            region_name = country_info.get('region', region_code)
                            
                            if region_code and region_code not in [r[0] for r in regions]:
                                regions.append((region_code, region_name, True, datetime.now()))
        
        # Insert regions
        if regions:
            cursor.executemany('''
                INSERT OR IGNORE INTO fedex_zone_regions 
                (region_code, region_name, active, created_at)
                VALUES (?, ?, ?, ?)
            ''', regions)
            print(f"Loaded {len(regions)} regions")
        
        conn.commit()
        conn.close()
    
    def _load_zones(self, data):
        """Load zone matrix data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        zones = []
        
        if 'zones' in data:
            zones_data = data['zones']
            for zone_key, zone_info in zones_data.items():
                # Extract zone letter from key (e.g., "zone_a" -> "A")
                zone_letter = zone_key.replace('zone_', '').upper()
                
                zone_name = f"Zone {zone_letter}"
                description = f"FedEx shipping zone {zone_letter}"
                
                if isinstance(zone_info, dict):
                    if 'description' in zone_info:
                        description = zone_info['description']
                    elif 'countries' in zone_info:
                        country_count = len(zone_info['countries'])
                        description = f"Zone {zone_letter} - {country_count} countries"
                
                zones.append((zone_letter, zone_name, description, True, datetime.now()))
        
        # Insert zones
        if zones:
            cursor.executemany('''
                INSERT OR IGNORE INTO fedex_zone_matrix 
                (zone_letter, zone_name, description, active, created_at)
                VALUES (?, ?, ?, ?, ?)
            ''', zones)
            print(f"Loaded {len(zones)} zones")
        
        conn.commit()
        conn.close()
    
    def _load_countries(self, data):
        """Load country-zone mappings"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        countries = []
        
        if 'zones' in data:
            zones_data = data['zones']
            for zone_key, zone_info in zones_data.items():
                zone_letter = zone_key.replace('zone_', '').upper()
                
                if isinstance(zone_info, dict) and 'countries' in zone_info:
                    for country_info in zone_info['countries']:
                        if isinstance(country_info, dict):
                            country_code = country_info.get('country_code', '')
                            country_name = country_info.get('country', '')
                            region_code = country_info.get('region_code', 'UNK')
                            region_name = country_info.get('region', region_code)
                            sub_region = country_info.get('sub_region')
                            currency_code = country_info.get('currency')
                            exchange_rate = country_info.get('exchange_rate')
                            
                            if country_code and country_name:
                                countries.append((
                                    country_code, country_name, region_code, region_name,
                                    sub_region, zone_letter, currency_code, exchange_rate,
                                    True, datetime.now()
                                ))
        
        # Insert countries
        if countries:
            cursor.executemany('''
                INSERT OR IGNORE INTO fedex_country_zones 
                (country_code, country_name, region_code, region_name, sub_region, 
                 zone_letter, currency_code, exchange_rate, active, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', countries)
            print(f"Loaded {len(countries)} countries")
        
        conn.commit()
        conn.close()
    
    def _load_service_types(self, data):
        """Load service types from rates data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        services = []
        service_codes = set()
        
        # Extract service types from rates
        if 'rates' in data:
            rates_data = data['rates']
            for service_key, service_rates in rates_data.items():
                if isinstance(service_rates, dict):
                    # Create service code from key
                    service_code = service_key.upper().replace(' ', '_')
                    service_name = service_key.replace('_', ' ').title()
                    
                    # Generate description based on service type
                    if 'express' in service_key.lower():
                        description = "Express delivery service"
                    elif 'economy' in service_key.lower():
                        description = "Economy delivery service"
                    elif 'priority' in service_key.lower():
                        description = "Priority delivery service"
                    elif 'international' in service_key.lower():
                        description = "International shipping service"
                    else:
                        description = f"FedEx {service_name} service"
                    
                    if service_code not in service_codes:
                        services.append((service_code, service_name, description, True, datetime.now()))
                        service_codes.add(service_code)
        
        # Add default services if none found
        if not services:
            default_services = [
                ('IE', 'International Economy', 'International Economy delivery service', True, datetime.now()),
                ('IP', 'International Priority', 'International Priority delivery service', True, datetime.now()),
                ('IF', 'International First', 'International First delivery service', True, datetime.now()),
            ]
            services.extend(default_services)
        
        # Insert services
        if services:
            cursor.executemany('''
                INSERT OR IGNORE INTO fedex_service_types 
                (service_code, service_name, description, active, created_at)
                VALUES (?, ?, ?, ?, ?)
            ''', services)
            print(f"Loaded {len(services)} service types")
        
        conn.commit()
        conn.close()
    
    def _load_rate_cards(self, data):
        """Load rate card data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        rate_cards = []
        
        if 'rates' in data:
            rates_data = data['rates']
            for service_key, service_rates in rates_data.items():
                service_code = service_key.upper().replace(' ', '_')
                
                if isinstance(service_rates, dict):
                    for zone_key, zone_rates in service_rates.items():
                        if zone_key.startswith('zone_') and isinstance(zone_rates, dict):
                            zone_letter = zone_key.replace('zone_', '').upper()
                            
                            # Process weight-based rates
                            for weight_key, rate_value in zone_rates.items():
                                if isinstance(rate_value, (int, float)):
                                    weight_from = self._parse_weight_from_key(weight_key)
                                    weight_to = self._parse_weight_to_key(weight_key)
                                    
                                    rate_cards.append((
                                        service_code, zone_letter, weight_from, weight_to,
                                        'standard', rate_value, None, 'USD', True,
                                        datetime.now(), datetime.now()
                                    ))
        
        # Insert rate cards
        if rate_cards:
            cursor.executemany('''
                INSERT OR IGNORE INTO fedex_rate_cards 
                (service_code, zone_letter, weight_from, weight_to, rate_type, 
                 rate, rate_per_kg, currency_code, active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', rate_cards)
            print(f"Loaded {len(rate_cards)} rate cards")
        
        conn.commit()
        conn.close()
    
    def _load_surcharges(self, data):
        """Load surcharge data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        surcharges = []
        
        # Look for surcharge data in various places
        surcharge_sources = ['surcharges', 'additional_charges', 'fees']
        
        for source in surcharge_sources:
            if source in data:
                surcharge_data = data[source]
                if isinstance(surcharge_data, dict):
                    for surcharge_key, surcharge_info in surcharge_data.items():
                        surcharge_code = surcharge_key.upper()
                        surcharge_name = surcharge_key.replace('_', ' ').title()
                        
                        if isinstance(surcharge_info, dict):
                            rate = surcharge_info.get('rate', 0)
                            is_percentage = surcharge_info.get('is_percentage', False)
                            description = surcharge_info.get('description', f"{surcharge_name} surcharge")
                            surcharge_type = surcharge_info.get('type', 'other')
                            min_charge = surcharge_info.get('min_charge')
                            max_charge = surcharge_info.get('max_charge')
                            
                        elif isinstance(surcharge_info, (int, float)):
                            rate = surcharge_info
                            is_percentage = False
                            description = f"{surcharge_name} surcharge"
                            surcharge_type = 'other'
                            min_charge = None
                            max_charge = None
                        else:
                            continue
                        
                        surcharges.append((
                            surcharge_code, surcharge_name, description, surcharge_type,
                            None, rate, None, is_percentage, False, min_charge, max_charge,
                            'USD', True, date.today(), None, datetime.now()
                        ))
        
        # Add common FedEx surcharges if none found
        if not surcharges:
            default_surcharges = [
                ('FUEL', 'Fuel Surcharge', 'Variable fuel surcharge', 'fuel', None, 15.0, None, True, False, None, None, 'USD', True, date.today(), None, datetime.now()),
                ('RES', 'Residential Surcharge', 'Residential delivery surcharge', 'residential', None, 5.20, None, False, False, None, None, 'USD', True, date.today(), None, datetime.now()),
                ('DAS', 'Delivery Area Surcharge', 'Extended delivery area surcharge', 'delivery', None, 6.50, None, False, False, None, None, 'USD', True, date.today(), None, datetime.now()),
                ('OS', 'Oversize Surcharge', 'Oversize package surcharge', 'handling', None, 85.00, None, False, False, None, None, 'USD', True, date.today(), None, datetime.now()),
            ]
            surcharges.extend(default_surcharges)
        
        # Insert surcharges
        if surcharges:
            cursor.executemany('''
                INSERT OR IGNORE INTO fedex_surcharges 
                (surcharge_code, surcharge_name, description, surcharge_type, zone_letter,
                 rate, rate_per_kg, is_percentage, weight_based, min_charge, max_charge,
                 currency_code, active, effective_date, expiry_date, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', surcharges)
            print(f"Loaded {len(surcharges)} surcharges")
        
        conn.commit()
        conn.close()
    
    def _parse_weight_from_key(self, weight_key):
        """Parse weight_from from weight key like 'kg_0_5' or 'up_to_5kg'"""
        try:
            if 'kg_' in weight_key:
                parts = weight_key.split('_')
                if len(parts) >= 2:
                    return float(parts[1])
            elif 'up_to_' in weight_key:
                return 0.0
            elif weight_key.isdigit():
                return float(weight_key)
        except:
            pass
        return 0.0
    
    def _parse_weight_to_key(self, weight_key):
        """Parse weight_to from weight key"""
        try:
            if 'kg_' in weight_key:
                parts = weight_key.split('_')
                if len(parts) >= 3:
                    return float(parts[2])
                elif len(parts) == 2:
                    # Single weight, assume it's up to next kg
                    return float(parts[1]) + 1
            elif 'up_to_' in weight_key:
                # Extract number from 'up_to_5kg'
                import re
                match = re.search(r'(\d+(?:\.\d+)?)', weight_key)
                if match:
                    return float(match.group(1))
            elif weight_key.isdigit():
                return float(weight_key) + 1
        except:
            pass
        return None
    
    def get_load_summary(self):
        """Get summary of loaded data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        summary = {}
        
        tables = [
            ('fedex_zone_regions', 'Regions'),
            ('fedex_zone_matrix', 'Zones'),
            ('fedex_country_zones', 'Countries'),
            ('fedex_service_types', 'Service Types'),
            ('fedex_rate_cards', 'Rate Cards'),
            ('fedex_surcharges', 'Surcharges')
        ]
        
        for table, label in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            summary[label] = count
        
        conn.close()
        return summary

if __name__ == '__main__':
    # Run the loader
    loader = FedExRateCardLoader()
    
    if loader.load_from_json():
        print("\nLoad Summary:")
        summary = loader.get_load_summary()
        for category, count in summary.items():
            print(f"  {category}: {count}")
    else:
        print("Failed to load rate card data")
