import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Database connection details
DB_HOST = "192.168.0.15"
DB_PORT = 26256
DB_NAME = "defaultdb"
DB_USER = "root"
DB_PASSWORD = None  # Replace with your password if applicable

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

# Load data from Excel
excel_file = '../../data/database_tables.xlsx'  
sheets = {
    'Vehicles': ['model', 'manufacturer', 'autonomy_level', 'battery_level', 'current_location', 'status'],
    'Vehicle_Status': ['vehicle_id', 'timestamp', 'speed', 'direction', 'proximity_alert', 'road_condition', 'next_destination'],
    'Routes': ['vehicle_id', 'origin', 'destination', 'route_points'],
    'Control_Commands': ['vehicle_id', 'timestamp', 'command_type', 'details'],
    'Collision_Warnings': ['vehicle_id', 'timestamp', 'location', 'severity'],
    'Road_Sensors': ['location', 'sensor_type', 'status', 'last_updated'],
    'Traffic_Signals': ['location', 'status', 'last_updated'],
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

        # Convert 'battery_level' to decimal if in percentages
        if 'battery_level' in data.columns:
            data['battery_level'] = data['battery_level'].str.rstrip('%').astype(float)

        # Insert data into the corresponding table
        insert_data_into_table(cursor, sheet.lower(), data, columns)
        print(f"Data inserted into {sheet.lower()} table successfully.")

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