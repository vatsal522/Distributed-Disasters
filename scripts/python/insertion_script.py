import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Database connection details
DB_HOST = "192.168.0.15"
DB_PORT = 26256
DB_NAME = "autonomous_vehicle_system"
DB_USER = "root"
DB_PASSWORD = None  # Replace with your password if applicableclear

# Function to insert data into a table
def insert_data_into_table(cursor, table_name, dataframe, columns):
    """
    Insert data into the specified table.
    
    :param cursor: Database cursor object
    :param table_name: Name of the table
    :param dataframe: DataFrame containing the data
    :param columns: Columns to insert data into
    """
    values = [tuple(row) for row in dataframe[columns].itertuples(index=False)]
    
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
    execute_values(cursor, sql, values)
    conn.commit()

# Load data from Excel
excel_file = '/Users/vatsalshah/Documents/Workspace/ASU Courses/SEM3/DDS/project/data/database_tables.xlsx'
sheets = {
    #'Vehicles': ['vehicle_id', 'model', 'manufacturer', 'autonomy_level', 'battery_level', 'latitude', 'longitude', 'status'],
    'Routes': ['route_id', 'vehicle_id', 'origin_latitude', 'origin_longitude', 'destination_latitude', 'destination_longitude', 'route_points'],
    'Control_Commands': ['command_id', 'vehicle_id', 'timestamp', 'command_type', 'details'],
    'Collision_Warnings': ['warning_id', 'vehicle_id', 'timestamp', 'latitude', 'longitude', 'severity'],
    'Road_Sensors': ['sensor_id', 'latitude', 'longitude', 'sensor_type', 'status', 'last_updated'],
    'Traffic_Signals': ['signal_id', 'latitude', 'longitude', 'status', 'last_updated'],
    'Vehicle_Status': ['vehicle_id', 'timestamp', 'speed', 'direction', 'proximity_alert', 'road_condition', 'latitude', 'longitude']
}

# Connect to the CockroachDB database
try:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()
    print("Connected to the database successfully.")

    for sheet, columns in sheets.items():
        # Load the sheet into a DataFrame
        data = pd.read_excel(excel_file, sheet_name=sheet)

        # Convert specific columns for formatting
        if 'battery_level' in data.columns:
            # Strip '%' and convert to float
            data['battery_level'] = data['battery_level'].str.rstrip('%').astype(float)

        if 'route_points' in data.columns:
            data['route_points'] = data['route_points'].apply(lambda x: x if isinstance(x, str) else "{}")  # Handle JSON-like data

        # Insert data into the respective table
        table_name = f"autonomous_vehicle_system.public.{sheet.lower()}"
        print(f"Inserting data into {table_name}...")
        insert_data_into_table(cursor, table_name, data, columns)
        print(f"Data inserted into {table_name} table successfully.")
        conn.commit()

    # Commit the transaction
    conn.commit()
    print("All data inserted successfully.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Database connection closed.")