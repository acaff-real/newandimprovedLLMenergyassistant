import pandas as pd
import mysql.connector
from mysql.connector import Error

def create_table(cursor, table_name):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        Segment VARCHAR(50),
        Record_Date DATE,
        Contract_Type varchar(50),
        Instrument_Name VARCHAR(100),
        Highest_Price FLOAT,
        Lowest_Price FLOAT,
        Average_Price FLOAT,
        Weighted_Average FLOAT,
        Total_Traded_Volume_MWh FLOAT,
        No_of_Trades INT
    );
    """
    cursor.execute(create_table_query)

def excel_to_mysql_with_create(file_path, mysql_config, table_name):
    try:
        # Read Excel file
        df = pd.read_excel(file_path, engine='openpyxl')
        
        # Connect to MySQL
        connection = mysql.connector.connect(**mysql_config)
        cursor = connection.cursor()
        
        # Create table
        create_table(cursor, table_name)
        print(f"Table '{table_name}' is ready.")
        
        # Prepare insert query
        cols = ", ".join(df.columns)
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"
        
        # Convert dataframe rows to tuples, also convert pandas timestamps to python date
        data = []
        for row in df.itertuples(index=False):
            new_row = []
            for val in row:
                if pd.isna(val):
                    new_row.append(None)
                elif isinstance(val, pd.Timestamp):
                    new_row.append(val.date())  # Convert to date
                else:
                    new_row.append(val)
            data.append(tuple(new_row))
        
        # Insert all data
        cursor.executemany(insert_query, data)
        connection.commit()
        
        print(f"Inserted {cursor.rowcount} rows into '{table_name}'.")
    
    except Error as e:
        print(f"MySQL Error: {e}")
    except Exception as ex:
        print(f"General Error: {ex}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

# Example usage
mysql_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'password1234',
    'database': 'iexinternetdatacenter'
}

file_path = 'trainingdat2a.xlsx'
table_name = 'energy_bids_TAM'

excel_to_mysql_with_create(file_path, mysql_config, table_name)
