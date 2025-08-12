import sqlite3

def update_air_rate_entries(db_path="dhl_audit.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check column names
    cursor.execute("PRAGMA table_info(air_rate_entries)")
    columns = [row[1] for row in cursor.fetchall()]
    print("Columns in air_rate_entries:", columns)

    # Adjust these if your actual column names differ
    origin_country_col = "origin_country"
    origin_port_code_col = "origin_port_code"
    destination_country_col = "destination_country"
    destination_port_code_col = "destination_port_code"

    # Update origin_port_code: set to origin_country + origin_port_code
    cursor.execute(f"""
        UPDATE air_rate_entries
        SET {origin_port_code_col} = {origin_country_col} || {origin_port_code_col}
        WHERE {origin_country_col} IS NOT NULL AND {origin_port_code_col} IS NOT NULL
    """)

    # Update destination_port_code: set to destination_country + destination_port_code
    cursor.execute(f"""
        UPDATE air_rate_entries
        SET {destination_port_code_col} = {destination_country_col} || {destination_port_code_col}
        WHERE {destination_country_col} IS NOT NULL AND {destination_port_code_col} IS NOT NULL
    """)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    update_air_rate_entries()
