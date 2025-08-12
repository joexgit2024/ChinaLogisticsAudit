import sqlite3

DB_PATH = 'dhl_audit.db'

def print_table_schema(table_name: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()
    conn.close()
    print(f"Schema for table '{table_name}':")
    for col in columns:
        print(f"  {col[1]} ({col[2]})")

if __name__ == '__main__':
    print_table_schema('ytd_audit_results')
