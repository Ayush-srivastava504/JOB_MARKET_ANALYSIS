"""
Database setup and view creation for job market analysis.
Deployment-ready version with logging, error handling, and configuration management.
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
import json

# Setup logging
def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None) -> logging.Logger:
    """Configure logging for the application"""
    logger = logging.getLogger(__name__)
    
    if logger.hasHandlers():
        logger.handlers.clear()
    
    logger.setLevel(getattr(logging, log_level.upper()))
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_format = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s'
        )
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)
    
    return logger

# Initialize logger
logger = setup_logging()

class DatabaseConfig:
    """Database configuration manager"""
    
    def __init__(self, config_file: str = "config.ini", env_file: str = ".env"):
        self.config_file = config_file
        self.env_file = env_file
        self.config = {}
        self._load_configuration()
    
    def _load_configuration(self) -> None:
        """Load configuration from multiple sources with precedence"""
        # Default configuration
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
        
        # 1. Load from config.ini
        config_parser = configparser.ConfigParser()
        if os.path.exists(self.config_file):
            try:
                config_parser.read(self.config_file)
                if 'database' in config_parser:
                    db_config = config_parser['database']
                    default_config.update({
                        'host': db_config.get('host', default_config['host']),
                        'port': int(db_config.get('port', default_config['port'])),
                        'user': db_config.get('user', default_config['user']),
                        'password': db_config.get('password', default_config['password']),
                        'database': db_config.get('database', default_config['database']),
                    })
                logger.info(f"Loaded configuration from {self.config_file}")
            except Exception as e:
                logger.warning(f"Failed to read config file: {e}")
        
        # 2. Load from environment variables
        env_vars = {
            'host': os.environ.get('DB_HOST'),
            'port': os.environ.get('DB_PORT'),
            'user': os.environ.get('DB_USER'),
            'password': os.environ.get('DB_PASSWORD'),
            'database': os.environ.get('DB_NAME'),
        }
        
        # Update with environment variables (if set)
        for key, value in env_vars.items():
            if value:
                if key == 'port':
                    default_config[key] = int(value)
                else:
                    default_config[key] = value
        
        # 3. Load from .env file if exists and not loaded from env
        if os.path.exists(self.env_file):
            try:
                with open(self.env_file, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            key, value = line.split('=', 1)
                            key = key.strip()
                            value = value.strip().strip('"\'')
                            os.environ.setdefault(key, value)
            except Exception as e:
                logger.warning(f"Failed to read .env file: {e}")
        
        self.config = default_config
    
    def validate(self) -> Tuple[bool, str]:
        """Validate database configuration"""
        required_fields = ['user', 'database']
        for field in required_fields:
            if not self.config.get(field):
                return False, f"Missing required configuration: {field}"
        
        if not self.config.get('password'):
            logger.warning("Database password is empty - ensure your database allows passwordless access")
        
        return True, "Configuration validated successfully"
    
    def get_connection_config(self) -> Dict[str, Any]:
        """Get connection configuration dictionary"""
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
    """Manages database operations"""
    
    def __init__(self, config: DatabaseConfig):
        self.config = config
        self.connection = None
    
    def connect(self, use_database: bool = True) -> bool:
        """Establish database connection"""
        try:
            conn_config = self.config.get_connection_config()
            
            if use_database:
                conn_config['database'] = self.config.config['database']
                logger.info(f"Attempting to connect to database: {self.config.config['database']}")
            else:
                logger.info("Connecting to MySQL server without database")
            
            self.connection = mysql.connector.connect(**conn_config)
            
            if self.connection.is_connected():
                server_info = self.connection.get_server_info()
                logger.info(f"Connected to MySQL server {server_info}")
                return True
            else:
                logger.error("Connection failed: not connected")
                return False
                
        except Error as e:
            logger.error(f"Connection error: {e}")
            return False
    
    def disconnect(self) -> None:
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logger.info("Database connection closed")
    
    def execute_query(self, query: str, params: tuple = None, fetch: bool = False) -> Optional[Any]:
        """Execute a SQL query"""
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
            logger.error(f"Query execution error: {e}")
            logger.debug(f"Failed query: {query}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
    
    def execute_script(self, script: str) -> bool:
        """Execute a SQL script with multiple statements"""
        cursor = None
        try:
            cursor = self.connection.cursor()
            
            # Split by semicolon, but handle stored procedures, etc.
            statements = script.split(';')
            
            for statement in statements:
                statement = statement.strip()
                if statement:
                    cursor.execute(statement)
            
            self.connection.commit()
            return True
            
        except Error as e:
            logger.error(f"Script execution error: {e}")
            if self.connection:
                self.connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()

class DatabaseSetup:
    """Handles database setup and initialization"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.table_name = 'jobs'
    
    def initialize_database(self) -> bool:
        """Create database if it doesn't exist"""
        try:
            # Connect without database to create it
            if not self.db.connect(use_database=False):
                return False
            
            database_name = self.db.config.config['database']
            
            # Create database
            create_db_query = f"""
            CREATE DATABASE IF NOT EXISTS {database_name} 
            CHARACTER SET utf8mb4 
            COLLATE utf8mb4_unicode_ci
            """
            
            self.db.execute_query(create_db_query)
            logger.info(f"Database '{database_name}' created or already exists")
            
            # Switch to the database
            self.db.execute_query(f"USE {database_name}")
            
            return True
            
        except Error as e:
            logger.error(f"Database initialization failed: {e}")
            return False
    
    def create_jobs_table(self, drop_existing: bool = False) -> bool:
        """Create the jobs table"""
        try:
            if drop_existing:
                self.db.execute_query(f"DROP TABLE IF EXISTS {self.table_name}")
                logger.info(f"Dropped existing table: {self.table_name}")
            
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
                FULLTEXT idx_title (title),
                FULLTEXT idx_skills (skills)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
            
            self.db.execute_query(table_definition)
            logger.info(f"Table '{self.table_name}' created or already exists")
            return True
            
        except Error as e:
            logger.error(f"Table creation failed: {e}")
            return False
    
    def adjust_sql_mode(self) -> bool:
        """Adjust SQL mode for compatibility"""
        try:
            result = self.db.execute_query("SELECT @@SESSION.sql_mode", fetch=True)
            if result:
                current_mode = result[0]['@@SESSION.sql_mode']
                
                # Remove ONLY_FULL_GROUP_BY if present
                if 'ONLY_FULL_GROUP_BY' in current_mode:
                    new_mode = current_mode.replace('ONLY_FULL_GROUP_BY', '')
                    new_mode = new_mode.replace(',,', ',').strip(',')
                    self.db.execute_query(f"SET SESSION sql_mode = '{new_mode}'")
                    logger.info("SQL mode adjusted for analytical queries")
            
            return True
        except Error as e:
            logger.warning(f"SQL mode adjustment note: {e}")
            return False

class DataLoader:
    """Handles data loading operations"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.table_name = 'jobs'
    
    def check_existing_data(self) -> int:
        """Check if table has existing data"""
        try:
            result = self.db.execute_query(
                f"SELECT COUNT(*) as count FROM {self.table_name}", 
                fetch=True
            )
            return result[0]['count'] if result else 0
        except Error as e:
            logger.error(f"Failed to check existing data: {e}")
            return 0
    
    def load_from_csv(self, csv_file: str, batch_size: int = 250) -> Tuple[bool, int]:
        """Load data from CSV file"""
        if not os.path.exists(csv_file):
            logger.error(f"Data file not found: {csv_file}")
            return False, 0
        
        try:
            logger.info(f"Loading data from {csv_file}")
            df = pd.read_csv(csv_file)
            logger.info(f"Found {len(df)} records in CSV")
            
            # Clean and prepare data
            df = self._clean_dataframe(df)
            
            # Clear existing data
            self.db.execute_query(f"TRUNCATE TABLE {self.table_name}")
            logger.info("Cleared existing data from table")
            
            # Insert data in batches
            inserted_count = self._insert_batch_data(df, batch_size)
            
            logger.info(f"Successfully loaded {inserted_count} records")
            return True, inserted_count
            
        except Exception as e:
            logger.error(f"Data load error: {e}", exc_info=True)
            return False, 0
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and prepare dataframe for database insertion"""
        # Fill NaN values
        df['skills'] = df['skills'].fillna('')
        df['seniority'] = df['seniority'].fillna('Not Specified')
        df['category'] = df['category'].fillna('Not Specified')
        df['location'] = df['location'].fillna('Not Specified')
        df['company'] = df['company'].fillna('Unknown')
        
        # Convert salary columns
        for col in ['salary_avg', 'salary_min', 'salary_max']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Ensure string columns don't exceed max length
        df['title'] = df['title'].str.slice(0, 255)
        df['company'] = df['company'].str.slice(0, 255)
        df['location'] = df['location'].str.slice(0, 255)
        df['seniority'] = df['seniority'].str.slice(0, 50)
        df['category'] = df['category'].str.slice(0, 100)
        
        return df
    
    def _insert_batch_data(self, df: pd.DataFrame, batch_size: int) -> int:
        """Insert data in batches"""
        insert_query = f"""
        INSERT INTO {self.table_name} 
        (title, company, location, salary_avg, salary_min, salary_max,
         is_remote, seniority, category, skills, post_date, scraped_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
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
            
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                cursor.executemany(insert_query, batch)
                self.db.connection.commit()
                
                total_inserted += len(batch)
                if i % 1000 == 0 or total_inserted == len(records):
                    logger.info(f"  Inserted {total_inserted}/{len(records)} records")
            
            return total_inserted
            
        finally:
            if cursor:
                cursor.close()

class ViewManager:
    """Manages SQL view creation and management"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.views = self._get_view_definitions()
    
    def _get_view_definitions(self) -> Dict[str, str]:
        """Get SQL view definitions"""
        return {
            'view_job_summary': """
            CREATE OR REPLACE VIEW view_job_summary AS
            SELECT 
                COUNT(*) as total_jobs,
                COUNT(DISTINCT company) as unique_companies,
                ROUND(AVG(salary_avg), 2) as average_salary,
                SUM(is_remote) as remote_jobs,
                ROUND((SUM(is_remote) / COUNT(*)) * 100, 2) as remote_percentage,
                ROUND(MIN(salary_avg), 2) as minimum_salary,
                ROUND(MAX(salary_avg), 2) as maximum_salary
            FROM jobs
            WHERE salary_avg IS NOT NULL;
            """,
            
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
                ROUND(AVG(salary_avg), 2) as average_in_range
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
            
            'view_company_analysis': """
            CREATE OR REPLACE VIEW view_company_analysis AS
            SELECT 
                company,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as average_salary,
                ROUND(MIN(salary_avg), 2) as minimum_salary,
                ROUND(MAX(salary_avg), 2) as maximum_salary,
                SUM(is_remote) as remote_positions
            FROM jobs
            WHERE company IS NOT NULL AND company != ''
            GROUP BY company
            HAVING COUNT(*) >= 2
            ORDER BY job_count DESC, average_salary DESC;
            """,
            
            'view_seniority_analysis': """
            CREATE OR REPLACE VIEW view_seniority_analysis AS
            SELECT 
                COALESCE(seniority, 'Not Specified') as seniority_level,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as average_salary,
                ROUND(MIN(salary_avg), 2) as minimum_salary,
                ROUND(MAX(salary_avg), 2) as maximum_salary
            FROM jobs
            WHERE salary_avg IS NOT NULL
            GROUP BY seniority_level
            ORDER BY average_salary DESC;
            """,
            
            'view_skills_analysis': """
            CREATE OR REPLACE VIEW view_skills_analysis AS
            WITH RECURSIVE numbers AS (
                SELECT 1 as n
                UNION ALL
                SELECT n + 1 FROM numbers WHERE n < 10
            ),
            skills_split AS (
                SELECT 
                    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(j.skills, ',', n.n), ',', -1)) as skill,
                    j.salary_avg
                FROM jobs j
                JOIN numbers n 
                ON CHAR_LENGTH(j.skills) - CHAR_LENGTH(REPLACE(j.skills, ',', '')) >= n.n - 1
                WHERE j.skills IS NOT NULL AND j.skills != ''
            )
            SELECT 
                skill,
                COUNT(*) as demand_count,
                ROUND(AVG(salary_avg), 2) as average_salary
            FROM skills_split
            WHERE skill != ''
            GROUP BY skill
            ORDER BY demand_count DESC
            LIMIT 25;
            """,
            
            'view_remote_analysis': """
            CREATE OR REPLACE VIEW view_remote_analysis AS
            SELECT 
                CASE is_remote 
                    WHEN 1 THEN 'Remote' 
                    ELSE 'On-site' 
                END as work_type,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as average_salary
            FROM jobs
            WHERE salary_avg IS NOT NULL
            GROUP BY work_type;
            """,
            
            'view_category_analysis': """
            CREATE OR REPLACE VIEW view_category_analysis AS
            SELECT 
                COALESCE(category, 'Not Specified') as category,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as average_salary
            FROM jobs
            WHERE salary_avg IS NOT NULL
            GROUP BY category
            ORDER BY job_count DESC;
            """,
            
            'view_trend_analysis': """
            CREATE OR REPLACE VIEW view_trend_analysis AS
            SELECT 
                DATE_FORMAT(post_date, '%Y-%m') as month,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as average_salary,
                SUM(is_remote) as remote_count
            FROM jobs
            WHERE post_date IS NOT NULL AND salary_avg IS NOT NULL
            GROUP BY DATE_FORMAT(post_date, '%Y-%m')
            ORDER BY month DESC;
            """
        }
    
    def create_views(self) -> Tuple[int, list]:
        """Create all views"""
        created = 0
        failed = []
        
        logger.info("Creating analysis views...")
        
        for view_name, view_sql in self.views.items():
            try:
                self.db.execute_query(view_sql)
                logger.info(f"  ✓ Created: {view_name}")
                created += 1
            except Error as e:
                logger.error(f"  ✗ Failed to create {view_name}: {e}")
                failed.append(view_name)
        
        return created, failed
    
    def verify_views(self) -> Dict[str, int]:
        """Verify that views contain data"""
        results = {}
        
        for view_name in self.views.keys():
            try:
                result = self.db.execute_query(
                    f"SELECT COUNT(*) as count FROM {view_name}", 
                    fetch=True
                )
                if result:
                    results[view_name] = result[0]['count']
                    logger.debug(f"View {view_name}: {result[0]['count']} rows")
            except Error as e:
                logger.warning(f"Failed to verify view {view_name}: {e}")
                results[view_name] = -1
        
        return results

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Database setup for job market analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --config config.ini --csv data/jobs.csv
  %(prog)s --drop-table --reload-data
  %(prog)s --skip-data --skip-views
        """
    )
    
    parser.add_argument(
        '--config', 
        default='config.ini',
        help='Configuration file (default: config.ini)'
    )
    
    parser.add_argument(
        '--csv', 
        default='cleaned_jobs_simple.csv',
        help='CSV data file to load (default: cleaned_jobs_simple.csv)'
    )
    
    parser.add_argument(
        '--drop-table', 
        action='store_true',
        help='Drop existing table before creating'
    )
    
    parser.add_argument(
        '--reload-data', 
        action='store_true',
        help='Force reload data from CSV'
    )
    
    parser.add_argument(
        '--skip-data', 
        action='store_true',
        help='Skip data loading step'
    )
    
    parser.add_argument(
        '--skip-views', 
        action='store_true',
        help='Skip view creation step'
    )
    
    parser.add_argument(
        '--batch-size', 
        type=int, 
        default=250,
        help='Batch size for data loading (default: 250)'
    )
    
    parser.add_argument(
        '--log-level', 
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--log-file', 
        help='Log file path'
    )
    
    parser.add_argument(
        '--dry-run', 
        action='store_true',
        help='Validate configuration without making changes'
    )
    
    return parser.parse_args()

def main():
    """Main execution function"""
    args = parse_arguments()
    
    # Setup logging with command line arguments
    global logger
    logger = setup_logging(args.log_level, args.log_file)
    
    logger.info("=" * 60)
    logger.info("Job Market Analysis Database Setup")
    logger.info("=" * 60)
    
    # Load configuration
    config = DatabaseConfig(config_file=args.config)
    is_valid, message = config.validate()
    
    if not is_valid:
        logger.error(f"Configuration error: {message}")
        sys.exit(1)
    
    logger.info(f"Configuration loaded from {args.config}")
    logger.info(f"Target database: {config.config['database']}")
    
    if args.dry_run:
        logger.info("DRY RUN - Configuration validated successfully")
        logger.info("No changes were made to the database")
        sys.exit(0)
    
    # Initialize database manager
    db_manager = DatabaseManager(config)
    
    try:
        # Setup database
        db_setup = DatabaseSetup(db_manager)
        
        if not db_setup.initialize_database():
            logger.error("Failed to initialize database")
            sys.exit(1)
        
        # Create table
        if not db_setup.create_jobs_table(drop_existing=args.drop_table):
            logger.error("Failed to create jobs table")
            sys.exit(1)
        
        # Adjust SQL mode
        db_setup.adjust_sql_mode()
        
        # Load data
        data_loader = DataLoader(db_manager)
        existing_count = data_loader.check_existing_data()
        
        should_load_data = False
        if existing_count == 0:
            logger.info(f"No existing data found in table")
            should_load_data = True
        elif args.reload_data:
            logger.info(f"Found {existing_count} existing records, force reloading")
            should_load_data = True
        elif not args.skip_data:
            response = input(f"Found {existing_count} existing records. Reload data? (yes/no): ")
            should_load_data = response.lower() in ['yes', 'y']
        
        if should_load_data and not args.skip_data:
            success, count = data_loader.load_from_csv(args.csv, args.batch_size)
            if not success:
                logger.warning("Data loading failed or was skipped")
        else:
            logger.info("Skipping data loading")
        
        # Create views
        if not args.skip_views:
            view_manager = ViewManager(db_manager)
            created_count, failed_views = view_manager.create_views()
            
            if created_count > 0:
                logger.info(f"Successfully created {created_count} views")
            
            if failed_views:
                logger.warning(f"Failed to create {len(failed_views)} views: {failed_views}")
            
            # Verify views
            view_stats = view_manager.verify_views()
            if view_stats.get('view_job_summary', 0) > 0:
                logger.info("Views created and verified successfully")
        
        # Display summary
        logger.info("\n" + "=" * 60)
        logger.info("SETUP COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        
        summary = {
            "database": config.config['database'],
            "host": config.config['host'],
            "port": config.config['port'],
            "table": "jobs",
            "views_created": created_count if 'created_count' in locals() else 0,
            "data_loaded": count if 'count' in locals() else existing_count
        }
        
        logger.info("Summary:")
        for key, value in summary.items():
            logger.info(f"  {key}: {value}")
        
        logger.info("\nPower BI Connection Details:")
        logger.info(f"  Server: {config.config['host']}:{config.config['port']}")
        logger.info(f"  Database: {config.config['database']}")
        logger.info(f"  Username: {config.config['user']}")
        
    except Exception as e:
        logger.error(f"Setup failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if db_manager.connection and db_manager.connection.is_connected():
            db_manager.disconnect()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nSetup interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)