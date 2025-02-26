import os
from dotenv import load_dotenv
from pathlib import Path
import requests
from datetime import datetime, timedelta

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from energy_fuel_module.db.models import State 

# Loading parent folders' paths
ROOT_DIR = Path().cwd()
BASE_DIR = Path(__file__).resolve().parent.parent

load_dotenv(BASE_DIR / 'config/.env')

API_KEY = os.getenv('EIA_API_KEY')
BASE_URL = f'https://api.eia.gov/v2/petroleum/pri/gnd/data/?api_key={API_KEY}&frequency=weekly'

DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')


DB_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DB_URL)

def get_db_session():
    """Creates and returns a new SQLAlchemy session."""
    Session = sessionmaker(bind=engine)
    return Session()

def get_state_mappings():
    """Fetch all states and return {state_code: state_id} dictionary."""
    session = get_db_session()
    try:
        states = session.query(State).all()
        return {state.state_code: state.state_id for state in states}
    except Exception as e:
        print(f"Error fetching states: {e}")
        return {}
    finally:
        session.close()

def fetch_gas_data(fuel_type, state_code):
    """Fetch regular gas prices for a given state from the EIA API."""
    
    fuel_types = {
        "regular": "R1",  
        "premium": "P1" 
    }
    fuel_category = fuel_types.get(fuel_type)

    # Set weekly timeline for data
    today = datetime.today()
    last_monday = today - timedelta(days=today.weekday() + 7)  # Monday of last week
    last_sunday = last_monday + timedelta(days=6)

    params = {
        "data": ["value"],  
        "facets": {
            "product": [fuel_category], 
            "area": [state_code]  
        },
        "start": last_monday.strftime("%Y-%m-%d"),
        "end": last_sunday.strftime("%Y-%m-%d"),
        "sort": [{"column": "period", "direction": "desc"}],  
        "offset": 0,
        "length": 500  
    }

    try:
        response = requests.post(BASE_URL, json=params)
        response.raise_for_status()  # Raise error for failed requests
        data = response.json()
        return data
    
    except requests.exceptions.RequestException as e:
        print(f"API request failed for {state_code} ({fuel_type} gas): {e}")
        return None
    
def clean_fuel_data(response, state_id, fuel_type):
    """Extracts necessary fuel price data from API response and formats it."""
    if not response or "series" not in response:
        return []

    try:
        raw_data = response["series"][0]["data"] 
        cleaned_data = []
        for entry in raw_data:
            price_date = f"{entry[0][:4]}-{entry[0][4:6]}-{entry[0][6:]}"  
            price = round(float(entry[1]), 3)  

            cleaned_data.append({
                "state_id": state_id,
                "price_date": price_date,
                "fuel_type": fuel_type,
                "price": price,
                "source": "EIA"
            })
        return cleaned_data
    
    except (KeyError, IndexError, ValueError) as e:
        print(f"Data parsing error: {e}")
        return []  

def get_fuel_prices():
    """Calls all functions: fetches, cleans, and returns fuel price data."""
    state_mappings = get_state_mappings()
    fuel_types = ["regular", "premium"]
    data = []

    for state_code, state_id in state_mappings.items():
        print(f"Fetching data for {state_code}...")

        for fuel_type in fuel_types:
            response = fetch_gas_data(state_code, fuel_type)
            cleaned_data = clean_fuel_data(response, state_id, fuel_type)
            data.extend(cleaned_data)

    return data
