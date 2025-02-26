import os
from dotenv import load_dotenv
from pathlib import Path
import alchemy

# Loading parent folders' paths
ROOT_DIR = Path().cwd()
BASE_DIR = Path(__file__).resolve().parent.parent

load_dotenv()

# https://api.eia.gov/series/?api_key=YOUR_API_KEY&series_id=PET.EMM_EPMR_PTE_XX_DPG.W

API_KEY = os.getenv('EIA_API_KEY')
BASE_URL = 'https://api.eia.gov/series/?'


def fetch_regular_gas_data():



