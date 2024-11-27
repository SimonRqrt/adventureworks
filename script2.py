import os
import csv
from dotenv import load_dotenv
import pyodbc

def load_env_variables():
    """Load environment variables from .env file."""
    load_dotenv()
    server = os.getenv("SERVER")
    database = os.getenv("DATABASE")
    username = os.getenv("USERNAME")
    password = os.getenv("PASSWORD")
    
    if not all([server, database, username, password]):
        raise ValueError("Missing one or more environment variables (SERVER, DATABASE, USERNAME, PASSWORD).")
    
    return server, database, username, password

def connect_to_database(server, database, username, password):
    """Establish a connection to the SQL Server database."""
    connection_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=30;"
    )
    return pyodbc.connect(connection_string)

def list_tables(cursor):
    """List all tables with their schemas."""
    cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA NOT IN ('HumanResources', 'Purchasing');")
    tables = []
    for row in cursor.fetchall():
        tables.append((row.TABLE_SCHEMA, row.TABLE_NAME))
    print(f"Found tables: {tables}")
    return tables


def export_table_to_csv(cursor, schema_name, table_name, output_dir):
    """Export the data from a table to a CSV file."""
    output_file = os.path.join(output_dir, f"{schema_name}_{table_name}.csv")

    # Vérifiez si le fichier existe déjà
    if os.path.exists(output_file):
        print(f"File {output_file} already exists. Skipping export.")
        return
    
    print(f"Exporting table {schema_name}.{table_name} to CSV...")
    cursor.execute(f"SELECT * FROM [{schema_name}].[{table_name}];")
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()

    with open(output_file, mode='w', newline='', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(columns)  # Write column headers
        writer.writerows(rows)   # Write rows
    
    print(f"Table {schema_name}.{table_name} exported to {output_file}.")

def main():
    # Load environment variables
    try:
        server, database, username, password = load_env_variables()
    except ValueError as e:
        print(e)
        return

    # Connect to the database
    try:
        conn = connect_to_database(server, database, username, password)
        cursor = conn.cursor()
        print("Connection successful!")
    except pyodbc.Error as e:
        print(f"Failed to connect: {e}")
        return
    
    # List tables
    try:
        tables = list_tables(cursor)
    except pyodbc.Error as e:
        print(f"Failed to list tables: {e}")
        return

    # Create output directory for CSV files
    output_dir = "csv_exports"
    os.makedirs(output_dir, exist_ok=True)

    # Export each table to a CSV
    for schema, table in list_tables(cursor):
        try:
            export_table_to_csv(cursor, schema, table, output_dir)
        except pyodbc.Error as e:
            print(f"Failed to export table {schema}.{table}: {e}")

    
    # Close the connection
    conn.close()
    print("Export complete.")

if __name__ == "__main__":
    main()
