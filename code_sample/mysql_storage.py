"""
Job Market Analysis Database Setup
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import sys
import os
import logging
import argparse
import configparser
from datetime import datetime
from typing import Optional, Dict, Any, Tuple

# Configure logging
def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    """Set up logging with sensible defaults"""
    logger = logging.getLogger(__name__)
    
    # Clear any existing handlers to avoid duplicates
    if logger.hasHandlers():
        logger.handlers.clear()
    
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Console output
    console_handler = logging.StreamHandler(sys.stdout)
    console_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # Optional file logging for troubleshooting
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)
    
    return logger

# Start logging right away
logger = setup_logging()

class DatabaseConfig:
    """Handles database configuration from multiple sources"""
    
    def __init__(self, config_file: str = "config.ini", env_file: str = ".env"):
        self.config_file = config_file
        self.env_file = env_file
        self.config = {}
        self._load_configuration()
    
    def _load_configuration(self) -> None:
        """Load settings from config files and environment variables"""
        # Sensible defaults for local development
        default_config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': '',
            'database': 'job_analysis_db',
            'charset': 'utf8mb4',
            'pool_size': 5,
            'pool_reset_session': True,
            'autocommit': False
        }
        
        # Try to read from config.ini first
        config_parser = configparser.ConfigParser()
        if os.path.exists(self.config_file):
            try:
                config_parser.read(self.config_file)
                if 'database' in config_parser:
                    db_config = config_parser['database']
                    # Update defaults with any values from config file
                    for key in default_config:
                        if key in db_config:
                            if key == 'port':
                                default_config[key] = int(db_config[key])
                            else:
                                default_config[key] = db_config[key]
                    logger.info(f"Loaded settings from {self.config_file}")
            except Exception as e:
                logger.warning(f"Couldn't read config file: {e}")
        
        # Check environment variables (good for Docker/cloud deployments)
        env_vars = {
            'host': os.environ.get('DB_HOST'),
            'port': os.environ.get('DB_PORT'),
            'user': os.environ.get('DB_USER'),
            'password': os.environ.get('DB_PASSWORD'),
            'database': os.environ.get('DB_NAME'),
        }
        
        # Environment variables override config file settings
        for key, value in env_vars.items():
            if value:
                if key == 'port':
                    default_config[key] = int(value)
                else:
                    default_config[key] = value
        
        # For local development with .env files
        if os.path.exists(self.env_file):
            try:
                with open(self.env_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            if '=' in line:
                                key, value = line.split('=', 1)
                                key = key.strip()
                                value = value.strip().strip('"\'')
                                os.environ.setdefault(key, value)
            except Exception as e:
                logger.warning(f"Couldn't read .env file: {e}")
        
        self.config = default_config
    
    def validate(self) -> Tuple[bool, str]:
        """Make sure we have the minimum required settings"""
        if not self.config.get('database'):
            return False, "Database name is required"
        if not self.config.get('user'):
            return False, "Database user is required"
        
        # Warn about empty password but don't fail (for local dev)
        if not self.config.get('password'):
            logger.warning("No database password set - okay for local development")
        
        return True, "Configuration looks good"
    
    def get_connection_config(self) -> Dict[str, Any]:
        """Get the settings needed for MySQL connection"""
        return {
            'host': self.config['host'],
            'port': self.config['port'],
            'user': self.config['user'],
            'password': self.config['password'],
            'charset': self.config['charset'],
            'use_pure': True,
            'autocommit': self.config['autocommit']
        }

class DatabaseManager:
    """Manages the database connection and queries"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
    
    def connect(self, use_database: bool = True) -> bool:
        """Connect to MySQL database"""
        try:
            conn_config = self.config.get_connection_config()
            
            if use_database:
                conn_config['database'] = self.config.config['database']
                logger.info(f"Connecting to database: {self.config.config['database']}")
            else:
                logger.info("Connecting to MySQL server (no specific database)")
            
            self.connection = mysql.connector.connect(**conn_config)
            
            if self.connection.is_connected():
                server_info = self.connection.get_server_info()
                logger.info(f"Connected to MySQL server version {server_info}")
                return True
            
            logger.error("Connection failed")
            return False
                
        except Error as e:
            logger.error(f"Connection error: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close the database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Disconnected from database")
    
    def execute_query(self, query: str, params: tuple = None, fetch: bool = False) -> Optional[Any]:
        """Run a SQL query with proper error handling"""
        cursor = None
        try:
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query, params or ())
            
            if fetch:
                result = cursor.fetchall()
                return result
            
            self.connection.commit()
            return None
            
        except Error as e:
            logger.error(f"Query failed: {e}")
            logger.debug(f"Problem query: {query}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()

class DatabaseSetup:
    """Sets up the database structure"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.table_name = 'jobs'
    
    def initialize_database(self) -> bool:
        """Create the database if it doesn't exist"""
        try:
            # First connect without specifying database
            if not self.db.connect(use_database=False):
                return False
            
            database_name = self.db.config.config['database']
            
            # Create database with proper character set
            create_db_query = f"""
            CREATE DATABASE IF NOT EXISTS {database_name} 
            CHARACTER SET utf8mb4 
            COLLATE utf8mb4_unicode_ci
            """
            
            self.db.execute_query(create_db_query)
            logger.info(f"Database '{database_name}' ready")
            
            # Switch to our new database
            self.db.execute_query(f"USE {database_name}")
            
            return True
            
        except Error as e:
            logger.error(f"Failed to set up database: {e}")
            return False
    
    def create_jobs_table(self, drop_existing: bool = False) -> bool:
        """Create the main jobs table"""
        try:
            if drop_existing:
                self.db.execute_query(f"DROP TABLE IF EXISTS {self.table_name}")
                logger.info(f"Removed old table: {self.table_name}")
            
            # Table structure - designed for job market analysis
            table_definition = f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                job_id INT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(255),
                company VARCHAR(255),
                location VARCHAR(255),
                salary_avg DECIMAL(12, 2),
                salary_min DECIMAL(12, 2),
                salary_max DECIMAL(12, 2),
                is_remote TINYINT(1) DEFAULT 0,
                seniority VARCHAR(50),
                category VARCHAR(100),
                skills TEXT,
                post_date DATE,
                scraped_date DATETIME,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_company (company),
                INDEX idx_salary_avg (salary_avg),
                INDEX idx_is_remote (is_remote),
                INDEX idx_seniority (seniority),
                INDEX idx_category (category),
                INDEX idx_post_date (post_date),
                INDEX idx_location (location),
                FULLTEXT idx_title (title),
                FULLTEXT idx_skills (skills)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
            
            self.db.execute_query(table_definition)
            logger.info(f"Table '{self.table_name}' created")
            return True
            
        except Error as e:
            logger.error(f"Failed to create table: {e}")
            return False

class DataLoader:
    """Loads job data from CSV files"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.table_name = 'jobs'
    
    def load_from_csv(self, csv_file: str, batch_size: int = 250) -> Tuple[bool, int]:
        """Import data from a CSV file"""
        if not os.path.exists(csv_file):
            logger.error(f"Can't find data file: {csv_file}")
            return False, 0
        
        try:
            logger.info(f"Reading data from {csv_file}")
            df = pd.read_csv(csv_file)
            logger.info(f"Found {len(df)} records")
            
            # Clean up the data before loading
            df = self._clean_dataframe(df)
            
            # Clear any existing data
            self.db.execute_query(f"TRUNCATE TABLE {self.table_name}")
            logger.info("Cleared existing data")
            
            # Load in batches for better performance
            inserted_count = self._insert_batch_data(df, batch_size)
            
            logger.info(f"Successfully loaded {inserted_count} records")
            return True, inserted_count
            
        except Exception as e:
            logger.error(f"Error loading data: {e}", exc_info=True)
            return False, 0
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare the data for database insertion"""
        # Handle missing values
        df['skills'] = df['skills'].fillna('')
        df['seniority'] = df['seniority'].fillna('Not Specified')
        df['category'] = df['category'].fillna('Not Specified')
        df['location'] = df['location'].fillna('Not Specified')
        df['company'] = df['company'].fillna('Unknown')
        df['title'] = df['title'].fillna('Not Specified')
        
        # Convert salary columns to numbers
        for col in ['salary_avg', 'salary_min', 'salary_max']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Convert remote flag to 1/0
        if 'is_remote' in df.columns:
            df['is_remote'] = df['is_remote'].astype(int)
        
        # Trim long text fields
        df['title'] = df['title'].str.slice(0, 255)
        df['company'] = df['company'].str.slice(0, 255)
        df['location'] = df['location'].str.slice(0, 255)
        df['seniority'] = df['seniority'].str.slice(0, 50)
        df['category'] = df['category'].str.slice(0, 100)
        
        # Convert dates - important for Power BI!
        if 'post_date' in df.columns:
            df['post_date'] = pd.to_datetime(df['post_date'], errors='coerce')
            # Fill missing dates with scraped date or today
            if 'scraped_date' in df.columns:
                scraped_dates = pd.to_datetime(df['scraped_date'], errors='coerce')
                df['post_date'] = df['post_date'].fillna(scraped_dates)
            df['post_date'] = df['post_date'].fillna(pd.Timestamp('today'))
            df['post_date'] = df['post_date'].dt.date
        
        if 'scraped_date' in df.columns:
            df['scraped_date'] = pd.to_datetime(df['scraped_date'], errors='coerce')
            df['scraped_date'] = df['scraped_date'].fillna(pd.Timestamp.now())
        
        return df
    
    def _insert_batch_data(self, df: pd.DataFrame, batch_size: int) -> int:
        """Insert data in batches for better performance"""
        insert_query = f"""
        INSERT INTO {self.table_name} 
        (title, company, location, 
         salary_avg, salary_min, salary_max, is_remote, seniority, category, 
         skills, post_date, scraped_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Prepare records
        records = []
        for _, row in df.iterrows():
            record = (
                str(row.get('title', '')),
                str(row.get('company', '')),
                str(row.get('location', '')),
                float(row.get('salary_avg')) if pd.notna(row.get('salary_avg')) else None,
                float(row.get('salary_min')) if pd.notna(row.get('salary_min')) else None,
                float(row.get('salary_max')) if pd.notna(row.get('salary_max')) else None,
                int(row.get('is_remote', 0)),
                str(row.get('seniority', '')),
                str(row.get('category', '')),
                str(row.get('skills', '')),
                row.get('post_date'),
                row.get('scraped_date')
            )
            records.append(record)
        
        total_inserted = 0
        cursor = None
        
        try:
            cursor = self.db.connection.cursor()
            
            # Insert in batches
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                cursor.executemany(insert_query, batch)
                self.db.connection.commit()
                
                total_inserted += len(batch)
                if i % 1000 == 0 or total_inserted == len(records):
                    logger.info(f"  Progress: {total_inserted}/{len(records)} records")
            
            return total_inserted
            
        finally:
            if cursor:
                cursor.close()

class ViewManager:
    """Creates SQL views for analysis - COMPLETE VERSION with all views"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.views = self._get_view_definitions()
    
    def _get_view_definitions(self) -> Dict[str, str]:
        """Define all the analysis views - COMPLETE set with fixes"""
        return {
            # ===== CORE VIEWS FOR POWER BI =====
            'view_monthly_trends': """
            CREATE OR REPLACE VIEW view_monthly_trends AS
            SELECT 
                YEAR(post_date) as year,
                MONTH(post_date) as month,
                DATE_FORMAT(post_date, '%Y-%m') as year_month,
                COUNT(*) as job_count,
                ROUND(AVG(COALESCE(salary_avg, 0)), 2) as avg_salary,
                ROUND(MIN(COALESCE(salary_avg, 0)), 2) as min_salary,
                ROUND(MAX(COALESCE(salary_avg, 0)), 2) as max_salary,
                SUM(COALESCE(is_remote, 0)) as remote_jobs,
                ROUND((SUM(COALESCE(is_remote, 0)) / NULLIF(COUNT(*), 0)) * 100, 2) as remote_percentage,
                COUNT(DISTINCT company) as unique_companies,
                COUNT(DISTINCT location) as unique_locations,
                COUNT(DISTINCT category) as unique_categories
            FROM jobs
            WHERE post_date IS NOT NULL
            GROUP BY YEAR(post_date), MONTH(post_date), DATE_FORMAT(post_date, '%Y-%m')
            ORDER BY year DESC, month DESC;
            """,
            
            'view_date_dimension': """
            CREATE OR REPLACE VIEW view_date_dimension AS
            SELECT 
                dates.date_key,
                YEAR(dates.date_key) as year,
                MONTH(dates.date_key) as month,
                DAY(dates.date_key) as day,
                DATE_FORMAT(dates.date_key, '%Y-%m') as year_month,
                DATE_FORMAT(dates.date_key, '%b %Y') as month_year_display,
                QUARTER(dates.date_key) as quarter,
                WEEK(dates.date_key, 3) as week,
                DAYOFWEEK(dates.date_key) as day_of_week,
                DAYNAME(dates.date_key) as day_name,
                MONTHNAME(dates.date_key) as month_name,
                DAYOFYEAR(dates.date_key) as day_of_year
            FROM (
                SELECT '2023-01-01' + INTERVAL (a.a + (10 * b.a) + (100 * c.a) + (1000 * d.a)) DAY as date_key
                FROM (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS a
                CROSS JOIN (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS b
                CROSS JOIN (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS c
                CROSS JOIN (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS d
            ) dates
            WHERE dates.date_key BETWEEN '2023-01-01' AND '2024-12-31'
            ORDER BY dates.date_key;
            """,
            
            # ===== SUMMARY & OVERVIEW VIEWS =====
            'view_job_summary': """
            CREATE OR REPLACE VIEW view_job_summary AS
            SELECT 
                COUNT(*) as total_jobs,
                COUNT(DISTINCT company) as unique_companies,
                COUNT(DISTINCT location) as unique_locations,
                ROUND(AVG(COALESCE(salary_avg, 0)), 2) as average_salary,
                SUM(COALESCE(is_remote, 0)) as remote_jobs,
                ROUND((SUM(COALESCE(is_remote, 0)) / NULLIF(COUNT(*), 0)) * 100, 2) as remote_percentage,
                ROUND(MIN(COALESCE(salary_avg, 0)), 2) as minimum_salary,
                ROUND(MAX(COALESCE(salary_avg, 0)), 2) as maximum_salary
            FROM jobs
            WHERE salary_avg IS NOT NULL;
            """,
            
            'view_job_market_overview': """
            CREATE OR REPLACE VIEW view_job_market_overview AS
            SELECT 
                'Total Jobs' as metric,
                CAST(COUNT(*) AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci as value
            FROM jobs
            UNION ALL
            SELECT 
                'Average Salary',
                CONCAT('$', FORMAT(ROUND(AVG(COALESCE(salary_avg, 0)), 0), 0)) COLLATE utf8mb4_unicode_ci
            FROM jobs
            WHERE salary_avg IS NOT NULL
            UNION ALL
            SELECT 
                'Remote Jobs %',
                CONCAT(ROUND((SUM(COALESCE(is_remote, 0)) / COUNT(*)) * 100, 1), '%') COLLATE utf8mb4_unicode_ci
            FROM jobs
            UNION ALL
            SELECT 
                'Unique Companies',
                CAST(COUNT(DISTINCT company) AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
            FROM jobs
            UNION ALL
            SELECT 
                'Unique Locations',
                CAST(COUNT(DISTINCT location) AS CHAR CHARACTER SET utf8mb4) COLLATE utf8mb4_unicode_ci
            FROM jobs
            UNION ALL
            SELECT 
                'Top Location',
                COALESCE(
                    (SELECT location COLLATE utf8mb4_unicode_ci
                     FROM jobs 
                     WHERE location != '' 
                     GROUP BY location 
                     ORDER BY COUNT(*) DESC 
                     LIMIT 1),
                    'Not Available'
                ) COLLATE utf8mb4_unicode_ci
            FROM DUAL;
            """,
            
            # ===== LOCATION ANALYSIS VIEWS =====
            'view_location_analysis': """
            CREATE OR REPLACE VIEW view_location_analysis AS
            SELECT 
                COALESCE(NULLIF(location, ''), 'Not Specified') as location,
                COUNT(*) as job_count,
                ROUND(AVG(COALESCE(salary_avg, 0)), 2) as average_salary,
                ROUND(MIN(COALESCE(salary_avg, 0)), 2) as minimum_salary,
                ROUND(MAX(COALESCE(salary_avg, 0)), 2) as maximum_salary,
                SUM(COALESCE(is_remote, 0)) as remote_positions,
                ROUND((SUM(COALESCE(is_remote, 0)) / COUNT(*)) * 100, 2) as remote_percentage
            FROM jobs
            WHERE salary_avg IS NOT NULL
            GROUP BY location
            ORDER BY job_count DESC;
            """,
            
            'view_high_paying_locations': """
            CREATE OR REPLACE VIEW view_high_paying_locations AS
            SELECT 
                location,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as average_salary,
                ROUND(MIN(salary_avg), 2) as min_salary,
                ROUND(MAX(salary_avg), 2) as max_salary
            FROM jobs
            WHERE location IS NOT NULL AND location != '' AND salary_avg IS NOT NULL
            GROUP BY location
            HAVING COUNT(*) >= 3
            ORDER BY average_salary DESC
            LIMIT 20;
            """,
            
            'view_top_locations_by_jobs': """
            CREATE OR REPLACE VIEW view_top_locations_by_jobs AS
            SELECT 
                location,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as average_salary
            FROM jobs
            WHERE location IS NOT NULL AND location != ''
            GROUP BY location
            ORDER BY job_count DESC
            LIMIT 15;
            """,
            
            'view_location_salary_distribution': """
            CREATE OR REPLACE VIEW view_location_salary_distribution AS
            SELECT 
                location,
                CASE 
                    WHEN salary_avg < 50000 THEN 'Under 50K'
                    WHEN salary_avg BETWEEN 50000 AND 74999 THEN '50K - 75K'
                    WHEN salary_avg BETWEEN 75000 AND 99999 THEN '75K - 100K'
                    WHEN salary_avg BETWEEN 100000 AND 149999 THEN '100K - 150K'
                    WHEN salary_avg BETWEEN 150000 AND 199999 THEN '150K - 200K'
                    ELSE 'Over 200K'
                END as salary_range,
                COUNT(*) as job_count
            FROM jobs
            WHERE location IS NOT NULL AND location != '' AND salary_avg IS NOT NULL
            GROUP BY location, salary_range
            ORDER BY location, job_count DESC;
            """,
            
            'view_salary_by_category_location': """
            CREATE OR REPLACE VIEW view_salary_by_category_location AS
            SELECT 
                category,
                location,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as average_salary
            FROM jobs
            WHERE category IS NOT NULL AND location IS NOT NULL 
                AND category != '' AND location != '' AND salary_avg IS NOT NULL
            GROUP BY category, location
            HAVING COUNT(*) >= 2
            ORDER BY category, average_salary DESC;
            """,
            
            # ===== COMPANY ANALYSIS VIEWS =====
            'view_company_analysis': """
            CREATE OR REPLACE VIEW view_company_analysis AS
            SELECT 
                company,
                COUNT(*) as job_count,
                ROUND(AVG(COALESCE(salary_avg, 0)), 2) as average_salary,
                ROUND(MIN(COALESCE(salary_avg, 0)), 2) as minimum_salary,
                ROUND(MAX(COALESCE(salary_avg, 0)), 2) as maximum_salary,
                SUM(COALESCE(is_remote, 0)) as remote_positions,
                COUNT(DISTINCT location) as locations_count
            FROM jobs
            WHERE company IS NOT NULL AND company != '' AND salary_avg IS NOT NULL
            GROUP BY company
            HAVING COUNT(*) >= 2
            ORDER BY job_count DESC, average_salary DESC;
            """,
            
            'view_company_benchmark': """
            CREATE OR REPLACE VIEW view_company_benchmark AS
            SELECT 
                company,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as avg_salary,
                ROUND(AVG(salary_avg) / overall_avg.overall_avg * 100, 2) as salary_vs_market_pct,
                SUM(is_remote) as remote_jobs,
                ROUND(SUM(is_remote) / COUNT(*) * 100, 2) as remote_percentage
            FROM jobs,
                (SELECT AVG(salary_avg) as overall_avg FROM jobs WHERE salary_avg IS NOT NULL) overall_avg
            WHERE company IS NOT NULL AND company != '' AND salary_avg IS NOT NULL
            GROUP BY company
            HAVING COUNT(*) >= 5
            ORDER BY salary_vs_market_pct DESC;
            """,
            
            'view_top_companies_by_location': """
            CREATE OR REPLACE VIEW view_top_companies_by_location AS
            SELECT 
                location,
                company,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as average_salary
            FROM jobs
            WHERE location IS NOT NULL AND company IS NOT NULL 
                AND location != '' AND company != ''
            GROUP BY location, company
            HAVING COUNT(*) >= 2
            ORDER BY location, job_count DESC;
            """,
            
            # ===== SALARY ANALYSIS VIEWS =====
            'view_salary_ranges': """
            CREATE OR REPLACE VIEW view_salary_ranges AS
            SELECT 
                CASE 
                    WHEN salary_avg < 50000 THEN 'Under 50K'
                    WHEN salary_avg BETWEEN 50000 AND 74999 THEN '50K - 75K'
                    WHEN salary_avg BETWEEN 75000 AND 99999 THEN '75K - 100K'
                    WHEN salary_avg BETWEEN 100000 AND 149999 THEN '100K - 150K'
                    WHEN salary_avg BETWEEN 150000 AND 199999 THEN '150K - 200K'
                    ELSE 'Over 200K'
                END as salary_range,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as average_in_range,
                ROUND(MIN(salary_avg), 2) as min_in_range,
                ROUND(MAX(salary_avg), 2) as max_in_range
            FROM jobs
            WHERE salary_avg IS NOT NULL
            GROUP BY salary_range
            ORDER BY 
                CASE salary_range
                    WHEN 'Under 50K' THEN 1
                    WHEN '50K - 75K' THEN 2
                    WHEN '75K - 100K' THEN 3
                    WHEN '100K - 150K' THEN 4
                    WHEN '150K - 200K' THEN 5
                    ELSE 6
                END;
            """,
            
            # ===== CATEGORY ANALYSIS VIEWS =====
            'view_category_analysis': """
            CREATE OR REPLACE VIEW view_category_analysis AS
            SELECT 
                COALESCE(category, 'Not Specified') as category,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as average_salary,
                ROUND(MIN(salary_avg), 2) as min_salary,
                ROUND(MAX(salary_avg), 2) as max_salary,
                SUM(is_remote) as remote_jobs,
                ROUND(SUM(is_remote) / COUNT(*) * 100, 2) as remote_percentage
            FROM jobs
            WHERE salary_avg IS NOT NULL
            GROUP BY category
            ORDER BY job_count DESC;
            """,
            
            # ===== SENIORITY ANALYSIS VIEWS =====
            'view_seniority_analysis': """
            CREATE OR REPLACE VIEW view_seniority_analysis AS
            SELECT 
                COALESCE(seniority, 'Not Specified') as seniority_level,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as average_salary,
                ROUND(MIN(salary_avg), 2) as minimum_salary,
                ROUND(MAX(salary_avg), 2) as maximum_salary,
                SUM(is_remote) as remote_positions,
                ROUND(SUM(is_remote) / COUNT(*) * 100, 2) as remote_percentage
            FROM jobs
            WHERE salary_avg IS NOT NULL
            GROUP BY seniority_level
            ORDER BY average_salary DESC;
            """,
            
            # ===== REMOTE WORK ANALYSIS VIEWS =====
            'view_remote_analysis': """
            CREATE OR REPLACE VIEW view_remote_analysis AS
            SELECT 
                CASE is_remote 
                    WHEN 1 THEN 'Remote' 
                    ELSE 'On-site' 
                END as work_type,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as average_salary,
                ROUND(MIN(salary_avg), 2) as min_salary,
                ROUND(MAX(salary_avg), 2) as max_salary,
                COUNT(DISTINCT location) as locations_offered
            FROM jobs
            WHERE salary_avg IS NOT NULL
            GROUP BY work_type
            ORDER BY job_count DESC;
            """,
            
            # ===== SKILLS ANALYSIS VIEWS =====
            'view_skills_analysis': """
            CREATE OR REPLACE VIEW view_skills_analysis AS
            SELECT 
                skills,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as average_salary
            FROM jobs
            WHERE skills IS NOT NULL AND skills != '' AND salary_avg IS NOT NULL
            GROUP BY skills
            ORDER BY job_count DESC
            LIMIT 25;
            """,
            
            # ===== TREND ANALYSIS VIEWS =====
            'view_trend_analysis': """
            CREATE OR REPLACE VIEW view_trend_analysis AS
            SELECT 
                DATE_FORMAT(post_date, '%Y-%m') as month,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as average_salary,
                SUM(is_remote) as remote_count,
                ROUND(SUM(is_remote) / COUNT(*) * 100, 2) as remote_percentage,
                COUNT(DISTINCT company) as unique_companies
            FROM jobs
            WHERE post_date IS NOT NULL AND salary_avg IS NOT NULL
            GROUP BY DATE_FORMAT(post_date, '%Y-%m')
            ORDER BY month DESC;
            """
        }
    
    def create_views(self) -> Tuple[int, list]:
        """Create all the analysis views"""
        created = 0
        failed = []
        
        logger.info("Setting up analysis views...")
        
        for view_name, view_sql in self.views.items():
            try:
                self.db.execute_query(view_sql)
                logger.info(f"  Created: {view_name}")
                created += 1
            except Error as e:
                logger.error(f"  Failed to create {view_name}: {e}")
                failed.append(view_name)
        
        return created, failed

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Set up database for job market analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        '--config', 
        default='config.ini',
        help='Configuration file (default: config.ini)'
    )
    
    parser.add_argument(
        '--csv', 
        default='cleaned_jobs_simple.csv',
        help='CSV file with job data (default: cleaned_jobs_simple.csv)'
    )
    
    parser.add_argument(
        '--drop-table', 
        action='store_true',
        help='Delete existing table before creating new one'
    )
    
    parser.add_argument(
        '--reload-data', 
        action='store_true',
        help='Force reload data from CSV'
    )
    
    parser.add_argument(
        '--skip-data', 
        action='store_true',
        help='Skip loading data'
    )
    
    parser.add_argument(
        '--skip-views', 
        action='store_true',
        help='Skip creating views'
    )
    
    parser.add_argument(
        '--batch-size', 
        type=int, 
        default=250,
        help='Number of records to insert at once (default: 250)'
    )
    
    parser.add_argument(
        '--log-level', 
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='How detailed the logging should be (default: INFO)'
    )
    
    parser.add_argument(
        '--log-file', 
        help='Save logs to this file'
    )
    
    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='Check configuration without making changes'
    )
    
    return parser.parse_args()

def main():
    """Main function to run the setup"""
    args = parse_arguments()
    
    # Update logging with user's preferences
    global logger
    logger = setup_logging(args.log_level, args.log_file)
    
    print("\n" + "="*60)
    print("Job Market Database Setup - COMPLETE VERSION")
    print("="*60)
    
    # Load settings
    config = DatabaseConfig(config_file=args.config)
    is_valid, message = config.validate()
    
    if not is_valid:
        logger.error(f"Problem with settings: {message}")
        sys.exit(1)
    
    logger.info(f"Using database: {config.config['database']}")
    
    if args.dry_run:
        logger.info("Dry run complete - no changes made")
        sys.exit(0)
    
    # Set up database connection
    db_manager = DatabaseManager(config)
    
    try:
        # Create database structure
        db_setup = DatabaseSetup(db_manager)
        
        if not db_setup.initialize_database():
            logger.error("Couldn't set up database")
            sys.exit(1)
        
        # Create main table
        if not db_setup.create_jobs_table(drop_existing=args.drop_table):
            logger.error("Couldn't create jobs table")
            sys.exit(1)
        
        # Load data if needed
        data_loader = DataLoader(db_manager)
        
        should_load = False
        if args.reload_data:
            logger.info("Forcing data reload")
            should_load = True
        elif not args.skip_data:
            # Simple check - if table is empty, load data
            result = db_manager.execute_query("SELECT COUNT(*) as count FROM jobs", fetch=True)
            if result and result[0]['count'] == 0:
                logger.info("Table is empty, loading data")
                should_load = True
            elif not args.skip_data:
                response = input("Table has data. Reload? (y/n): ")
                should_load = response.lower() in ['y', 'yes']
        
        if should_load and not args.skip_data:
            success, count = data_loader.load_from_csv(args.csv, args.batch_size)
            if not success:
                logger.warning("Data loading didn't work")
        else:
            logger.info("Skipping data load")
            count = 0
        
        # Create analysis views
        if not args.skip_views:
            view_manager = ViewManager(db_manager)
            created_count, failed_views = view_manager.create_views()
            
            if created_count > 0:
                logger.info(f"Created {created_count} views")
            
            if failed_views:
                logger.warning(f"Couldn't create these views: {failed_views}")
        
        # Show summary
        print("\n" + "="*40)
        print("SETUP COMPLETE")
        print("="*40)

        print(f"\nDatabase: {config.config['database']}@{config.config['host']}:{config.config['port']}")
        print(f"Jobs table: Ready | Views: {created_count if 'created_count' in locals() else 0} | Jobs: {count if 'count' in locals() else 0}")

        print("\nKey views: monthly_trends, date_dimension, location_analysis, company_analysis, salary_ranges")

        print("\nPower BI: Connect to MySQL, import monthly_trends, date_dimension, location_analysis")
        print("Test: SELECT * FROM view_job_summary; SELECT * FROM view_top_locations_by_jobs LIMIT 5;")

        print("\nReady for analysis!")
        
    except Exception as e:
        logger.error(f"Something went wrong: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Clean up connection
        if db_manager.connection and db_manager.connection.is_connected():
            db_manager.disconnect()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user")
        sys.exit(0)