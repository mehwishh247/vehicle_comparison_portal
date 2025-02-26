import os
from dotenv import load_dotenv
from pathlib import Path
import requests
from datetime import datetime, relativedelta

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from energy_fuel_module.db.models import State 

# Loading parent folders' paths
ROOT_DIR = Path().cwd()
BASE_DIR = Path(__file__).resolve().parent.parent
BASE_URL = "https://api.eia.gov/v2/electricity/retail-sales/data/"

load_dotenv(BASE_DIR / 'config/.env')

API_KEY = os.getenv('EIA_API_KEY')

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

def fetch_electricity_data(state_code):
    """Fetch regular gas prices for a given state from the EIA API."""

    params = {
        "frequency": "monthly",
        "data": ["price"],
        "facets": {"stateid": [state_code]},
        "start": (datetime.today() - relativedelta(months=2)).strftime("%Y-%m"), 
        "sort": [{"column": "period", "direction": "desc"}],
        "offset": 0,
        "length": 5000  
    }

    try:
        headers = {"Accept": "application/json"}
        response = requests.get(f"{BASE_URL}?api_key={API_KEY}", headers=headers, params=params)        
        response.raise_for_status()  # Raise error for failed requests
        data = response.json()
        return data
    
    except requests.exceptions.RequestException as e:
        print(f"API request failed for {state_code}: {e}")
        return None


