import os
from dotenv import load_dotenv

load_dotenv()

# APIs
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
USAJOBS_API_KEY = os.getenv("USAJOBS_API_KEY")
USAJOBS_USER_EMAIL = os.getenv("USAJOBS_USER_EMAIL")

# Job Keywords
JOB_KEYWORDS = os.getenv("JOB_KEYWORDS", "data scientist")

# Database Configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME", "job_analysis_db"),
}
