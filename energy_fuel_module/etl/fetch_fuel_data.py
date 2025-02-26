import os
from dotenv import load_dotenv
from pathlib import Path
import requests

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from db.models import State 

# Loading parent folders' paths
ROOT_DIR = Path().cwd()
BASE_DIR = Path(__file__).resolve().parent.parent

load_dotenv(BASE_DIR / 'config/.env')

# https://api.eia.gov/series/?api_key=YOUR_API_KEY&series_id=PET.EMM_EPMR_PTE_XX_DPG.W

API_KEY = os.getenv('EIA_API_KEY')
BASE_URL = 'https://api.eia.gov/series/?'

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


def fetch_gas_data(series_id, state_code):
    """Fetch regular gas prices for a given state from the EIA API."""
    
    url = BASE_URL + f"api_key={API_KEY}&series_id={series_id}"

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise error for failed requests
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"API request failed for {state_code}: {e}")
        return None
    
def clean_fuel_data(response, state_id):
    """Extracts necessary fuel price data from API response and formats it."""
    if not response or "series" not in response:
        return []

    try:
        raw_data = response["series"][0]["data"]  # Data format: [["YYYYMMDD", price], ...]
        cleaned_data = []
        for entry in raw_data:
            price_date = f"{entry[0][:4]}-{entry[0][4:6]}-{entry[0][6:]}"  # Convert YYYYMMDD → YYYY-MM-DD
            price = round(float(entry[1]), 3)  # Convert to DECIMAL(10,3)
            cleaned_data.append({
                "state_id": state_id,
                "price_date": price_date,
                "fuel_type": "regular",
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
    data = []

    for state_code, state_id in state_mappings.items():
        print(f"Fetching data for {state_code}...")

        response = fetch_gas_data(f'PET.EMM_EPMR_PTE_{state_code}_DPG.W' ,state_code)
        regular_gas_data = clean_fuel_data(response, state_id)
        data.extend(regular_gas_data)

        response = fetch_gas_data(f'PET.EMM_EPMR_PTP_{state_code}_DPG.W' ,state_code)
        premium_gas_data = clean_fuel_data(response, state_id)
        data.extend(premium_gas_data)

    return data
