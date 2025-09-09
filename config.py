import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # Azure OpenAI
    AZURE_OPENAI_KEY = os.getenv('AZURE_OPENAI_KEY')
    AZURE_OPENAI_ENDPOINT = os.getenv('AZURE_OPENAI_ENDPOINT', 'https://api-openai-service.openai.azure.com/')
    AZURE_OPENAI_DEPLOYMENT = os.getenv('AZURE_OPENAI_DEPLOYMENT', 'o4-mini')
    AZURE_OPENAI_VERSION = os.getenv('AZURE_OPENAI_VERSION', '2024-12-01-preview')

    # Asset Management Backend
    ASSET_BACKEND_URL = os.getenv('ASSET_BACKEND_URL', 'https://assetmanagement-production-f542.up.railway.app')

    # Flask
    FLASK_ENV = os.getenv('FLASK_ENV', 'development')
    FLASK_DEBUG = os.getenv('FLASK_DEBUG', 'True').lower() == 'true'
    PORT = int(os.getenv('PORT', 5001))

    # File paths
    HISTORICAL_CSV_PATH = 'data/historical_data.csv'

    # Asset mapping (match your portfolio system)
    ASSET_MAPPING = {
        'Equities': 'NIFTY50',
        'Gold': 'GOLD',
        'Bitcoin': 'BITCOIN',
        'REITs': 'REIT'
    }
