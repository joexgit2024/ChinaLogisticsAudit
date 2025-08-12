"""
Air Freight Rate Card data model
"""
from app.database import get_db_connection
import pandas as pd
import os
import sqlite3
import datetime

def create_rate_card_table():
    """Create the rate_cards table if it doesn't exist."""
    conn = get_db_connection()
    
    conn.execute('''
    CREATE TABLE IF NOT EXISTS air_rate_cards (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        card_name TEXT NOT NULL,
        validity_start DATE,
        validity_end DATE,
        uploaded_file TEXT,
        uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Create the detailed rate card entries table
    conn.execute('''
    CREATE TABLE IF NOT EXISTS air_rate_entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rate_card_id INTEGER,
        lane_id TEXT,
        lane_description TEXT,
        origin_region TEXT,
        origin_country TEXT,
        lane_origin TEXT,
        origin_port_code TEXT,
        destination_region TEXT, 
        destination_country TEXT,
        lane_destination TEXT,
        destination_port_code TEXT,
        service TEXT,
        transit_time INTEGER,
        min_charge REAL,
        base_rate_lt1000kg REAL,
        base_rate_1000to2000kg REAL,
        base_rate_2000to3000kg REAL,
        base_rate_gt3000kg REAL,
        fuel_surcharge REAL,
        origin_fees REAL,
        destination_fees REAL,
        pss_charge REAL,
        pss_validity TEXT,
        total_min_charge REAL,
        breakeven_kg INTEGER,
        FOREIGN KEY (rate_card_id) REFERENCES air_rate_cards(id) ON DELETE CASCADE
    )
    ''')
    
    conn.commit()
    conn.close()

def import_rate_card(file_path, card_name=None):
    """Import a rate card from an Excel file."""
    # Generate a card name if not provided
    if not card_name:
        filename = os.path.basename(file_path)
        card_name = f"Rate Card - {filename.split('.')[0]}"
    
    # Extract validity dates from filename (if in expected format)
    validity_start = None
    validity_end = None
    
    filename = os.path.basename(file_path)
    if 'thru' in filename:
        try:
            date_parts = filename.split('_')
            for part in date_parts:
                if 'thru' in part:
                    dates = part.split('thru')
                    if len(dates) == 2:
                        start_str = dates[0].strip()
                        end_str = dates[1].strip().split('.')[0]
                        
                        # Parse dates in different formats
                        try:
                            validity_start = datetime.datetime.strptime(start_str, '%Y-%m-%d').date()
                        except ValueError:
                            try:
                                validity_start = datetime.datetime.strptime(start_str, '%m-%d').replace(year=datetime.date.today().year).date()
                            except:
                                pass
                                
                        try:
                            validity_end = datetime.datetime.strptime(end_str, '%Y-%m-%d').date()
                        except ValueError:
                            try:
                                validity_end = datetime.datetime.strptime(end_str, '%m-%d').replace(year=datetime.date.today().year).date()
                            except:
                                pass
        except:
            pass
    
    # Read the Excel file
    try:
        df = pd.read_excel(file_path)
        
        # Check if data is valid
        if len(df) == 0:
            return {"success": False, "message": "Excel file contains no data"}
        
        # Check for required columns
        required_columns = [
            'Lane ID', 'Lane Description', 'Origin Country', 'Lane Origin', 
            'Destinaiton Country', 'Lane Destination', 'Total Min Charge', 'Total Base Rate USD/KG \n< 1K (w/o fuel)'
        ]
        
        missing_columns = []
        for col in required_columns:
            if col not in df.columns:
                missing_columns.append(col)
        
        if missing_columns:
            return {"success": False, "message": f"Missing required columns: {', '.join(missing_columns)}"}
            
        # If validity dates not found in filename, try to extract from dataframe
        if not validity_start or not validity_end:
            if 'Rate Validity' in df.columns:
                # Try to extract from rate validity column
                validity_texts = df['Rate Validity'].dropna().unique()
                if len(validity_texts) > 0:
                    validity_text = validity_texts[0]
                    if ' - ' in validity_text or ' to ' in validity_text:
                        separator = ' - ' if ' - ' in validity_text else ' to '
                        dates = validity_text.split(separator)
                        if len(dates) == 2:
                            try:
                                validity_start = datetime.datetime.strptime(dates[0].strip(), '%Y-%m-%d').date()
                                validity_end = datetime.datetime.strptime(dates[1].strip(), '%Y-%m-%d').date()
                            except:
                                pass
            
        # Add rate card to database
        conn = get_db_connection()
        cursor = conn.execute('''
            INSERT INTO air_rate_cards (card_name, validity_start, validity_end, uploaded_file, uploaded_at)
            VALUES (?, ?, ?, ?, datetime('now'))
        ''', (card_name, validity_start, validity_end, os.path.basename(file_path)))
        
        rate_card_id = cursor.lastrowid
        
        # Process rate entries
        entries_added = 0
        
        for _, row in df.iterrows():
            # Skip rows without lane ID
            if pd.isna(row.get('Lane ID', pd.NA)):
                continue
                
            lane_id = str(row.get('Lane ID', ''))
            lane_description = str(row.get('Lane Description', ''))
            origin_region = str(row.get('Origin Region', ''))
            origin_country = str(row.get('Origin Country', ''))
            lane_origin = str(row.get('Lane Origin', ''))
            origin_port_code = str(row.get('Origin Port Code\n(3 char IATA code)', ''))
            destination_region = str(row.get('Destination Region', ''))
            destination_country = str(row.get('Destinaiton Country', ''))
            lane_destination = str(row.get('Lane Destination', ''))
            destination_port_code = str(row.get('Destination Port Code\n(3 char IATA code)', ''))
            service = str(row.get('Service', ''))
            
            # Get transit time
            transit_time = None
            try:
                transit_time = int(row.get('Transit time\n(Business Days)', 0))
            except (ValueError, TypeError):
                pass
            
            # Get rates - handle string values that can't be converted
            try:
                min_charge = float(row.get('Total Min Charge', 0) or 0)
            except (ValueError, TypeError):
                min_charge = 0.0
                
            try:
                base_rate_lt1000kg = float(row.get('Total Base Rate USD/KG \n< 1K (w/o fuel)', 0) or 0)
            except (ValueError, TypeError):
                base_rate_lt1000kg = 0.0
                
            try:
                base_rate_1000to2000kg = float(row.get('Total Base Rate USD/KG \n1K - <2K (w/o fuel)', 0) or 0)
            except (ValueError, TypeError):
                base_rate_1000to2000kg = 0.0
                
            try:
                base_rate_2000to3000kg = float(row.get('Total Base Rate USD/KG \n2K - 3K (w/o fuel)', 0) or 0)
            except (ValueError, TypeError):
                base_rate_2000to3000kg = 0.0
                
            try:
                base_rate_gt3000kg = float(row.get('Total Base Rate USD/KG \n>3K (w/o fuel)', 0) or 0)
            except (ValueError, TypeError):
                base_rate_gt3000kg = 0.0
            
            # Additional fees
            try:
                fuel_surcharge = float(row.get('ATA Fuel \n(USD/KG)', 0) or 0)
            except (ValueError, TypeError):
                fuel_surcharge = 0.0
                
            try:
                origin_fees = float(row.get('Origin Fees \n(THC, ISS, Screening, etc.) \n(USD/KG)', 0) or 0)
            except (ValueError, TypeError):
                origin_fees = 0.0
                
            try:
                destination_fees = float(row.get('Destination Fees \n(THC, ISS, Screening, etc.) \n(USD/KG)', 0) or 0)
            except (ValueError, TypeError):
                destination_fees = 0.0
                
            try:
                pss_charge = float(row.get('PSS (USD/KG)', 0) or 0)
            except (ValueError, TypeError):
                pss_charge = 0.0
            pss_validity = str(row.get('PSS Validity', ''))
            
            # Breakeven
            breakeven_kg = None
            try:
                breakeven_kg = int(row.get('Minium Charge Breakeven (KG)', 0) or 0)
            except (ValueError, TypeError):
                pass
                
            # Insert rate entry
            conn.execute('''
                INSERT INTO air_rate_entries (
                    rate_card_id, lane_id, lane_description, origin_region, origin_country, 
                    lane_origin, origin_port_code, destination_region, destination_country,
                    lane_destination, destination_port_code, service, transit_time,
                    min_charge, base_rate_lt1000kg, base_rate_1000to2000kg,
                    base_rate_2000to3000kg, base_rate_gt3000kg, fuel_surcharge,
                    origin_fees, destination_fees, pss_charge, pss_validity,
                    total_min_charge, breakeven_kg
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                rate_card_id, lane_id, lane_description, origin_region, origin_country, 
                lane_origin, origin_port_code, destination_region, destination_country,
                lane_destination, destination_port_code, service, transit_time,
                min_charge, base_rate_lt1000kg, base_rate_1000to2000kg,
                base_rate_2000to3000kg, base_rate_gt3000kg, fuel_surcharge,
                origin_fees, destination_fees, pss_charge, pss_validity,
                min_charge, breakeven_kg
            ))
            
            entries_added += 1
            
        conn.commit()
        conn.close()
        
        return {
            "success": True, 
            "message": f"Successfully imported rate card with {entries_added} entries",
            "rate_card_id": rate_card_id
        }
        
    except Exception as e:
        print(f"Error importing rate card: {str(e)}")
        return {"success": False, "message": f"Error importing rate card: {str(e)}"}

def get_rate_cards():
    """Get all rate cards from the database."""
    conn = get_db_connection()
    
    rate_cards = conn.execute('''
        SELECT id, card_name, validity_start, validity_end, uploaded_file, uploaded_at,
               (SELECT COUNT(*) FROM air_rate_entries WHERE rate_card_id = air_rate_cards.id) as entry_count
        FROM air_rate_cards
        ORDER BY uploaded_at DESC
    ''').fetchall()
    
    conn.close()
    return rate_cards

def get_rate_card(rate_card_id):
    """Get a rate card by ID."""
    conn = get_db_connection()
    
    rate_card = conn.execute('''
        SELECT * FROM air_rate_cards WHERE id = ?
    ''', (rate_card_id,)).fetchone()
    
    conn.close()
    return rate_card

def get_rate_entries(rate_card_id, filters=None):
    """Get rate entries for a specific rate card, with optional filtering."""
    conn = get_db_connection()
    
    query = "SELECT * FROM air_rate_entries WHERE rate_card_id = ?"
    params = [rate_card_id]
    
    if filters:
        if filters.get("origin_country"):
            query += " AND origin_country LIKE ?"
            params.append(f"%{filters['origin_country']}%")
        if filters.get("destination_country"):
            query += " AND destination_country LIKE ?"
            params.append(f"%{filters['destination_country']}%")
        if filters.get("service"):
            query += " AND service LIKE ?"
            params.append(f"%{filters['service']}%")
    
    query += " ORDER BY lane_id"
    
    entries = conn.execute(query, params).fetchall()
    conn.close()
    
    return entries

def get_applicable_rate(origin, destination, weight_kg, service_type=None, rate_card_id=None):
    """
    Get the applicable rate for a shipment based on origin, destination, and weight.
    
    Args:
        origin (str): Origin country or code
        destination (str): Destination country or code
        weight_kg (float): Shipment weight in kg
        service_type (str, optional): Service type filter
        rate_card_id (int, optional): Specific rate card ID to use
        
    Returns:
        dict: Rate information or None if not found
    """
    # Validate inputs
    try:
        weight_kg = float(weight_kg or 0)
    except (ValueError, TypeError):
        weight_kg = 0.0
        
    if weight_kg <= 0:
        return None
        
    conn = get_db_connection()
    
    # Build query conditions
    query_conditions = []
    params = []
    
    # Check if we need to search by country or port code
    if len(origin) <= 3:  # Assume it's a port code
        query_conditions.append("(origin_port_code = ? OR origin_port_code LIKE ?)")
        params.extend([origin, f"%{origin}%"])
    else:  # Assume it's a country
        query_conditions.append("(origin_country LIKE ? OR lane_origin LIKE ?)")
        params.extend([f"%{origin}%", f"%{origin}%"])
        
    if len(destination) <= 3:  # Assume it's a port code
        query_conditions.append("(destination_port_code = ? OR destination_port_code LIKE ?)")
        params.extend([destination, f"%{destination}%"])
    else:  # Assume it's a country
        query_conditions.append("(destination_country LIKE ? OR lane_destination LIKE ?)")
        params.extend([f"%{destination}%", f"%{destination}%"])
    
    if service_type:
        query_conditions.append("service LIKE ?")
        params.append(f"%{service_type}%")
    
    # If specific rate card provided
    if rate_card_id:
        query_conditions.append("rate_card_id = ?")
        params.append(rate_card_id)
    else:
        # Otherwise use the most recently uploaded valid rate card
        current_date = datetime.date.today().isoformat()
        query_conditions.append("""
            rate_card_id IN (
                SELECT id FROM air_rate_cards 
                WHERE (validity_start IS NULL OR validity_start <= ?) 
                AND (validity_end IS NULL OR validity_end >= ?)
                ORDER BY uploaded_at DESC
                LIMIT 1
            )
        """)
        params.extend([current_date, current_date])
    
    # Build the WHERE clause
    where_clause = " AND ".join(query_conditions)
    
    query = f"""
        SELECT * FROM air_rate_entries
        WHERE {where_clause}
        ORDER BY lane_id
    """
    
    entries = conn.execute(query, params).fetchall()
    conn.close()
    
    if not entries:
        return None
    
    # Find best applicable rate based on weight
    best_match = None
    for entry in entries:
        # Convert to dict
        entry_dict = dict(entry)
        
        # Ensure all numeric fields have valid values (handle None)
        numeric_fields = [
            'base_rate_lt1000kg', 'base_rate_1000to2000kg', 'base_rate_2000to3000kg', 
            'base_rate_gt3000kg', 'fuel_surcharge', 'origin_fees', 'destination_fees', 
            'pss_charge', 'min_charge'
        ]
        
        for field in numeric_fields:
            if entry_dict[field] is None:
                entry_dict[field] = 0.0
            else:
                try:
                    entry_dict[field] = float(entry_dict[field])
                except (ValueError, TypeError):
                    entry_dict[field] = 0.0
        
        # Calculate the applicable rate based on weight
        if weight_kg < 1000:
            entry_dict['applicable_rate'] = entry_dict['base_rate_lt1000kg']
        elif weight_kg < 2000:
            entry_dict['applicable_rate'] = entry_dict['base_rate_1000to2000kg']
        elif weight_kg <= 3000:
            entry_dict['applicable_rate'] = entry_dict['base_rate_2000to3000kg']
        else:
            entry_dict['applicable_rate'] = entry_dict['base_rate_gt3000kg']
            
        # Calculate total cost including surcharges
        entry_dict['fuel_cost'] = entry_dict['fuel_surcharge'] * weight_kg
        entry_dict['origin_fees_cost'] = entry_dict['origin_fees'] * weight_kg
        entry_dict['destination_fees_cost'] = entry_dict['destination_fees'] * weight_kg
        entry_dict['pss_cost'] = entry_dict['pss_charge'] * weight_kg if entry_dict['pss_charge'] else 0
        
        entry_dict['base_cost'] = entry_dict['applicable_rate'] * weight_kg
        entry_dict['total_cost'] = (
            entry_dict['base_cost'] + 
            entry_dict['fuel_cost'] + 
            entry_dict['origin_fees_cost'] + 
            entry_dict['destination_fees_cost'] + 
            entry_dict['pss_cost']
        )
        
        # Check if minimum charge applies
        if entry_dict['total_cost'] < entry_dict['min_charge']:
            entry_dict['total_cost'] = entry_dict['min_charge']
            entry_dict['min_charge_applied'] = True
        else:
            entry_dict['min_charge_applied'] = False
            
        if best_match is None or entry_dict['total_cost'] < best_match['total_cost']:
            best_match = entry_dict
    
    return best_match
