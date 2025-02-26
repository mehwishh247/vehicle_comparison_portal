import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path

# Loading parent folders' paths
ROOT_DIR = Path().cwd()
BASE_DIR = Path(__file__).resolve().parent.parent

load_dotenv(BASE_DIR / 'config/.env')

# Database connection settings
DB_NAME = "vehicle_comparison"
DB_USER = "postgres"
DB_PASSWORD = os.getenv('DB_PASSWORD')  # Replace with your actual password
DB_HOST = "localhost"
DB_PORT = "5432"

# List of U.S. states with their EIA regions
states = [
    ("AL", "Alabama", "PADD_3"),
    ("AK", "Alaska", "PADD_5"),
    ("AZ", "Arizona", "PADD_5"),
    ("AR", "Arkansas", "PADD_3"),
    ("CA", "California", "PADD_5"),
    ("CO", "Colorado", "PADD_4"),
    ("CT", "Connecticut", "PADD_1"),
    ("DE", "Delaware", "PADD_1"),
    ("FL", "Florida", "PADD_1"),
    ("GA", "Georgia", "PADD_1"),
    ("HI", "Hawaii", "Separate Energy Market"),
    ("ID", "Idaho", "PADD_4"),
    ("IL", "Illinois", "PADD_2"),
    ("IN", "Indiana", "PADD_2"),
    ("IA", "Iowa", "PADD_2"),
    ("KS", "Kansas", "PADD_2"),
    ("KY", "Kentucky", "PADD_2"),
    ("LA", "Louisiana", "PADD_3"),
    ("ME", "Maine", "PADD_1"),
    ("MD", "Maryland", "PADD_1"),
    ("MA", "Massachusetts", "PADD_1"),
    ("MI", "Michigan", "PADD_2"),
    ("MN", "Minnesota", "PADD_2"),
    ("MS", "Mississippi", "PADD_3"),
    ("MO", "Missouri", "PADD_2"),
    ("MT", "Montana", "PADD_4"),
    ("NE", "Nebraska", "PADD_2"),
    ("NV", "Nevada", "PADD_5"),
    ("NH", "New Hampshire", "PADD_1"),
    ("NJ", "New Jersey", "PADD_1"),
    ("NM", "New Mexico", "PADD_3"),
    ("NY", "New York", "PADD_1"),
    ("NC", "North Carolina", "PADD_1"),
    ("ND", "North Dakota", "PADD_2"),
    ("OH", "Ohio", "PADD_2"),
    ("OK", "Oklahoma", "PADD_2"),
    ("OR", "Oregon", "PADD_5"),
    ("PA", "Pennsylvania", "PADD_1"),
    ("RI", "Rhode Island", "PADD_1"),
    ("SC", "South Carolina", "PADD_1"),
    ("SD", "South Dakota", "PADD_2"),
    ("TN", "Tennessee", "PADD_1"),
    ("TX", "Texas", "PADD_3"),
    ("UT", "Utah", "PADD_4"),
    ("VT", "Vermont", "PADD_1"),
    ("VA", "Virginia", "PADD_1"),
    ("WA", "Washington", "PADD_5"),
    ("WV", "West Virginia", "PADD_1"),
    ("WI", "Wisconsin", "PADD_2"),
    ("WY", "Wyoming", "PADD_4"),
]

def create_table():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, 
            password=DB_PASSWORD, host=DB_HOST, 
            port=DB_PORT)

        query = '''CREATE TABLE IF NOT EXISTS states (
                    state_id SERIAL PRIMARY KEY,
                    state_code CHAR(2) UNIQUE NOT NULL,
                    state_name VARCHAR(50) NOT NULL,
                    region VARCHAR(50) NOT NULL
                    );'''
        
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()

        cursor.close()
        conn.close()

        print('STATES Table created successfully')

    except Exception as e:
        print(f"Error creating tables: {e}")
   

def insert_states():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, 
            password=DB_PASSWORD, host=DB_HOST, 
            port=DB_PORT)
        
        with conn.cursor() as cursor:    
            # Insert states into the table
            for state_code, state_name, region in states:
                cursor.execute(f"""
                    INSERT INTO states (state_code, state_name, region) 
                    VALUES ('{state_code}', '{state_name}', '{region}') 
                    ON CONFLICT (state_code) DO NOTHING;
                """)

        # Commit changes and close the connection
        conn.commit()
        conn.close()

        print("States table populated successfully!")

    except Exception as e:
        print(f"Error inserting states: {e}")

# Run the function
if __name__ == "__main__":
    create_table()
    insert_states()
