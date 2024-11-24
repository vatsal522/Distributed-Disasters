import pandas as pd
from cockroachdb.sqlalchemy import run_transaction
from sqlalchemy import create_engine, text

# Database connection details
DB_HOST = "192.168.0.15"
DB_PORT = 26256
DB_NAME = "defaultdb"
DB_USER = "root"
DB_PASSWORD = None 
SSL_MODE = "disable"

# SQLAlchemy connection URL
DB_URL = f"cockroachdb://{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode={SSL_MODE}"

# Function to insert data into a table
def insert_data_into_table(conn, table_name, dataframe, columns):
    """
    Insert data into the specified table.
    
    :param conn: Database connection object
    :param table_name: Name of the table
    :param dataframe: DataFrame containing the data
    :param columns: Columns to insert data into
    """
    values = [tuple(row) for row in dataframe[columns].itertuples(index=False)]
    placeholders = ", ".join([f":{col}" for col in columns])
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
    with conn.begin() as transaction:
        for value in values:
            transaction.execute(text(sql), {col: val for col, val in zip(columns, value)})

# Load data from Excel
excel_file = 'database_tables.xlsx'  # Replace with the path to your Excel file
sheets = {
    'Vehicles': ['model', 'manufacturer', 'autonomy_level', 'battery_level', 'current_location', 'status'],
    'Vehicle_Status': ['vehicle_id', 'timestamp', 'speed', 'direction', 'proximity_alert', 'road_condition', 'next_destination'],
    'Routes': ['vehicle_id', 'origin', 'destination', 'route_points'],
    'Control_Commands': ['vehicle_id', 'timestamp', 'command_type', 'details'],
    'Collision_Warnings': ['vehicle_id', 'timestamp', 'location', 'severity'],
    'Road_Sensors': ['location', 'sensor_type', 'status', 'last_updated'],
    'Traffic_Signals': ['location', 'status', 'last_updated'],
}

# Connect to the CockroachDB database using SQLAlchemy
engine = create_engine(DB_URL)
try:
    with engine.connect() as conn:
        print("Connected to the database successfully.")
        
        for sheet, columns in sheets.items():
            # Load the sheet into a DataFrame
            data = pd.read_excel(excel_file, sheet_name=sheet)

            # Convert 'battery_level' to decimal if in percentages
            if 'battery_level' in data.columns:
                data['battery_level'] = data['battery_level'].str.rstrip('%').astype(float)

            # Insert data into the corresponding table
            insert_data_into_table(conn, sheet.lower(), data, columns)
            print(f"Data inserted into {sheet.lower()} table successfully.")
        
        print("All data inserted successfully.")
except Exception as e:
    print(f"An error occurred: {e}")