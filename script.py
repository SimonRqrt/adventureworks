import os
import subprocess
from dotenv import load_dotenv
import pyodbc

def run_command(command):
    """Execute a shell command and print its output."""
    try:
        result = subprocess.run(command, shell=True, text=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Error: {e.stderr}")

def install_odbc_driver_18():
    """Install ODBC Driver 18 for SQL Server on macOS."""
    print("Installing ODBC Driver 18 for SQL Server on macOS...")
    
    # Update Homebrew
    print("Updating Homebrew...")
    run_command("brew update")
    
    # Install unixODBC
    print("Installing unixODBC...")
    run_command("brew install unixodbc")
    
    # Install Microsoft ODBC Driver 18 for SQL Server
    print("Installing Microsoft ODBC Driver 18 for SQL Server...")
    run_command("brew tap microsoft/mssql-release https://github.com/microsoft/homebrew-mssql-release")
    run_command("brew update")
    run_command("ACCEPT_EULA=Y brew install --no-sandbox msodbcsql18")
    
    print("ODBC Driver 18 installation complete.")

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

def test_pyodbc_connection(server, database, username, password):
    """Test a connection to SQL Server using pyodbc."""
    print("Testing pyodbc connection...")
    connection_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    try:
        conn = pyodbc.connect(connection_string)
        print("Connection successful!")
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION;")
        version = cursor.fetchone()
        print(f"SQL Server Version: {version[0]}")
        conn.close()
    except pyodbc.Error as e:
        print(f"Failed to connect: {e}")

if __name__ == "__main__":
    # Install ODBC Driver 18
    install_odbc_driver_18()
    
    # Load environment variables
    try:
        server, database, username, password = load_env_variables()
    except ValueError as e:
        print(e)
        exit(1)
    
    # Test connection
    test_pyodbc_connection(server, database, username, password)
