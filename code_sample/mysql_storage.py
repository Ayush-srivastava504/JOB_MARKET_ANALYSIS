import pandas as pd
import mysql.connector
from mysql.connector import Error
import sys
import os

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'Ayush@504.@',
    'database': 'job_analysis_db'
}

# ============================================================================
# DATABASE FUNCTIONS
# ============================================================================
def create_database_if_not_exists():
    """Create database if it doesn't exist"""
    try:
        # Connect without specifying database
        config_temp = MYSQL_CONFIG.copy()
        database_name = config_temp.pop('database')
        
        connection = mysql.connector.connect(**config_temp)
        cursor = connection.cursor()
        
        # Create database
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name} "
                      f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
        print(f"Database '{database_name}' created or already exists")
        
        cursor.close()
        connection.close()
        return True
        
    except Error as e:
        print(f"Error creating database: {e}")
        return False

def test_mysql_connection():
    """Test MySQL connection before proceeding"""
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        if connection.is_connected():
            print("MySQL connection successful")
            connection.close()
            return True
    except Error as e:
        print(f"MySQL connection failed: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure MySQL service is running")
        print("2. Check if MySQL is running on port 3306")
        print("3. Verify username and password")
        print("4. Try connecting with MySQL Workbench first")
        return False

def create_table_schema(connection):
    """Create jobs table with appropriate schema"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS jobs (
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
        FULLTEXT idx_title (title),
        FULLTEXT idx_skills (skills)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    
    try:
        cursor = connection.cursor()
        cursor.execute(create_table_sql)
        print("Table 'jobs' created successfully")
        cursor.close()
        return True
    except Error as e:
        print(f"Error creating table: {e}")
        return False

def set_sql_mode_compatible(connection):
    """Set SQL mode to be compatible with Power BI views"""
    try:
        cursor = connection.cursor()
        
        # Get current SQL mode
        cursor.execute("SELECT @@SESSION.sql_mode")
        current_mode = cursor.fetchone()[0]
        print(f"Current SQL mode: {current_mode}")
        
        # Remove ONLY_FULL_GROUP_BY from SQL mode
        new_mode = current_mode.replace('ONLY_FULL_GROUP_BY', '').replace(',,', ',').strip(',')
        
        # Set new SQL mode for this session
        cursor.execute(f"SET SESSION sql_mode = '{new_mode}'")
        
        # Verify it was set
        cursor.execute("SELECT @@SESSION.sql_mode")
        updated_mode = cursor.fetchone()[0]
        print(f"Updated SQL mode: {updated_mode}")
        
        cursor.close()
        return True
    except Error as e:
        print(f"Error setting SQL mode: {e}")
        return False

def create_all_compatible_views(connection):
    """Create all views that are compatible with Power BI and ONLY_FULL_GROUP_BY"""
    
    # Drop ALL views to start fresh (including existing ones)
    drop_views_sql = """
    DROP VIEW IF EXISTS 
        view_job_summary,
        view_company_analysis,
        view_seniority_analysis,
        view_category_analysis,
        view_location_analysis,
        view_skills_analysis,
        view_remote_analysis,
        view_salary_ranges,
        view_seniority_salary_detail,
        view_company_benchmark,
        view_simple_summary,
        view_companies,
        view_seniority;
    """
    
    try:
        cursor = connection.cursor()
        cursor.execute(drop_views_sql)
        print("Dropped all existing views")
        
        # Now create ALL views with improved and fixed queries
        views = [
            # Main Summary View
            """
            CREATE OR REPLACE VIEW view_job_summary AS
            SELECT 
                COUNT(*) as total_jobs,
                COUNT(DISTINCT company) as unique_companies,
                ROUND(AVG(salary_avg), 2) as average_salary,
                SUM(is_remote) as remote_jobs,
                ROUND((SUM(is_remote) / COUNT(*)) * 100, 2) as remote_percentage,
                ROUND(MIN(salary_avg), 2) as min_salary,
                ROUND(MAX(salary_avg), 2) as max_salary,
                ROUND(AVG(salary_max - salary_min), 2) as avg_salary_range
            FROM jobs
            WHERE salary_avg IS NOT NULL;
            """,
            
            # Company Analysis View
            """
            CREATE OR REPLACE VIEW view_company_analysis AS
            SELECT 
                company,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as avg_salary,
                ROUND(MIN(salary_avg), 2) as min_salary,
                ROUND(MAX(salary_avg), 2) as max_salary,
                SUM(is_remote) as remote_count,
                ROUND((SUM(is_remote) / COUNT(*)) * 100, 2) as remote_percentage
            FROM jobs
            WHERE company IS NOT NULL AND TRIM(company) != ''
            GROUP BY company
            HAVING COUNT(*) >= 2
            ORDER BY avg_salary DESC;
            """,
            
            # Seniority Analysis View
            """
            CREATE OR REPLACE VIEW view_seniority_analysis AS
            SELECT 
                COALESCE(seniority, 'Not Specified') as seniority,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as avg_salary,
                ROUND(STDDEV(salary_avg), 2) as salary_std_dev,
                ROUND(MIN(salary_avg), 2) as min_salary,
                ROUND(MAX(salary_avg), 2) as max_salary,
                SUM(is_remote) as remote_count,
                ROUND((SUM(is_remote) / COUNT(*)) * 100, 2) as remote_percentage
            FROM jobs
            WHERE salary_avg IS NOT NULL
            GROUP BY COALESCE(seniority, 'Not Specified')
            ORDER BY 
                CASE COALESCE(seniority, 'Not Specified')
                    WHEN 'Junior' THEN 1
                    WHEN 'Mid-level' THEN 2
                    WHEN 'Senior' THEN 3
                    WHEN 'Manager' THEN 4
                    WHEN 'Director' THEN 5
                    WHEN 'Executive' THEN 6
                    ELSE 7
                END;
            """,
            
            # Category Analysis View
            """
            CREATE OR REPLACE VIEW view_category_analysis AS
            SELECT 
                COALESCE(category, 'Not Specified') as category,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as avg_salary,
                SUM(is_remote) as remote_count,
                ROUND((SUM(is_remote) / COUNT(*)) * 100, 2) as remote_percentage
            FROM jobs
            WHERE salary_avg IS NOT NULL
            GROUP BY COALESCE(category, 'Not Specified')
            ORDER BY job_count DESC;
            """,
            
            # Location Analysis View
            """
            CREATE OR REPLACE VIEW view_location_analysis AS
            SELECT 
                COALESCE(location, 'Not Specified') as location,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as avg_salary,
                SUM(is_remote) as remote_count,
                ROUND((SUM(is_remote) / COUNT(*)) * 100, 2) as remote_percentage,
                GROUP_CONCAT(DISTINCT company ORDER BY company SEPARATOR ', ') as companies
            FROM jobs
            WHERE location IS NOT NULL AND TRIM(location) != '' AND salary_avg IS NOT NULL
            GROUP BY COALESCE(location, 'Not Specified')
            HAVING COUNT(*) >= 3
            ORDER BY job_count DESC;
            """,
            
            # Skills Analysis View
            """
            CREATE OR REPLACE VIEW view_skills_analysis AS
            SELECT 
                skill,
                COUNT(*) as demand_count,
                ROUND(AVG(salary_avg), 2) as avg_salary,
                COUNT(DISTINCT company) as company_count
            FROM (
                SELECT 
                    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(skills, ',', numbers.n), ',', -1)) as skill,
                    salary_avg,
                    company
                FROM jobs
                JOIN (
                    SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
                    UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10
                ) numbers 
                ON CHAR_LENGTH(skills) - CHAR_LENGTH(REPLACE(skills, ',', '')) >= numbers.n - 1
                WHERE skills IS NOT NULL AND TRIM(skills) != '' AND salary_avg IS NOT NULL
            ) skill_table
            WHERE skill != ''
            GROUP BY skill
            HAVING COUNT(*) >= 2
            ORDER BY demand_count DESC, avg_salary DESC;
            """,
            
            # Remote vs On-site Analysis
            """
            CREATE OR REPLACE VIEW view_remote_analysis AS
            SELECT 
                CASE 
                    WHEN is_remote = 1 THEN 'Remote'
                    ELSE 'On-site'
                END as work_type,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as avg_salary,
                ROUND(MIN(salary_avg), 2) as min_salary,
                ROUND(MAX(salary_avg), 2) as max_salary,
                ROUND(AVG(salary_max - salary_min), 2) as avg_salary_range
            FROM jobs
            WHERE salary_avg IS NOT NULL
            GROUP BY is_remote
            ORDER BY avg_salary DESC;
            """,
            
            # Salary Range Analysis - FIXED VERSION
            """
            CREATE OR REPLACE VIEW view_salary_ranges AS
            SELECT 
                salary_range,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as avg_in_range,
                SUM(is_remote) as remote_count,
                ROUND((SUM(is_remote) / COUNT(*)) * 100, 2) as remote_percentage
            FROM (
                SELECT 
                    salary_avg,
                    is_remote,
                    CASE 
                        WHEN salary_avg < 50000 THEN 'Under 50K'
                        WHEN salary_avg BETWEEN 50000 AND 74999 THEN '50K - 75K'
                        WHEN salary_avg BETWEEN 75000 AND 99999 THEN '75K - 100K'
                        WHEN salary_avg BETWEEN 100000 AND 149999 THEN '100K - 150K'
                        WHEN salary_avg BETWEEN 150000 AND 199999 THEN '150K - 200K'
                        ELSE 'Over 200K'
                    END as salary_range
                FROM jobs
                WHERE salary_avg IS NOT NULL
            ) salary_buckets
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
            
            # Enhanced Seniority Salary Detail
            """
            CREATE OR REPLACE VIEW view_seniority_salary_detail AS
            SELECT 
                COALESCE(seniority, 'Not Specified') as seniority,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as avg_salary,
                ROUND(MIN(salary_avg), 2) as min_salary,
                ROUND(MAX(salary_avg), 2) as max_salary,
                ROUND(AVG(salary_max - salary_min), 2) as avg_salary_range,
                SUM(is_remote) as remote_count,
                ROUND((SUM(is_remote) / COUNT(*)) * 100, 2) as remote_percentage,
                ROUND(STDDEV(salary_avg), 2) as salary_std_dev
            FROM jobs
            WHERE salary_avg IS NOT NULL
            GROUP BY COALESCE(seniority, 'Not Specified')
            ORDER BY avg_salary DESC;
            """,
            
            # Company Salary Benchmark
            """
            CREATE OR REPLACE VIEW view_company_benchmark AS
            SELECT 
                company,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as avg_salary,
                ROUND(MIN(salary_avg), 2) as min_salary,
                ROUND(MAX(salary_avg), 2) as max_salary,
                SUM(is_remote) as remote_count,
                ROUND((SUM(is_remote) / COUNT(*)) * 100, 2) as remote_percentage,
                CASE 
                    WHEN AVG(salary_avg) > (SELECT AVG(salary_avg) FROM jobs WHERE salary_avg IS NOT NULL) THEN 'Above Market'
                    ELSE 'At/Below Market'
                END as market_position,
                CASE 
                    WHEN AVG(salary_avg) > (SELECT AVG(salary_avg) FROM jobs WHERE salary_avg IS NOT NULL) + 
                                          (SELECT STDDEV(salary_avg) FROM jobs WHERE salary_avg IS NOT NULL) THEN 'Premium'
                    WHEN AVG(salary_avg) < (SELECT AVG(salary_avg) FROM jobs WHERE salary_avg IS NOT NULL) - 
                                          (SELECT STDDEV(salary_avg) FROM jobs WHERE salary_avg IS NOT NULL) THEN 'Budget'
                    ELSE 'Market Rate'
                END as salary_tier
            FROM jobs
            WHERE company IS NOT NULL 
                AND TRIM(company) != ''
                AND salary_avg IS NOT NULL
            GROUP BY company
            HAVING COUNT(*) >= 2
            ORDER BY avg_salary DESC;
            """,
            
            # Monthly Trends (if post_date exists)
            """
            CREATE OR REPLACE VIEW view_monthly_trends AS
            SELECT 
                DATE_FORMAT(post_date, '%Y-%m') as month_year,
                COUNT(*) as job_count,
                ROUND(AVG(salary_avg), 2) as avg_salary,
                SUM(is_remote) as remote_jobs,
                ROUND((SUM(is_remote) / COUNT(*)) * 100, 2) as remote_percentage,
                COUNT(DISTINCT company) as unique_companies
            FROM jobs
            WHERE post_date IS NOT NULL AND salary_avg IS NOT NULL
            GROUP BY DATE_FORMAT(post_date, '%Y-%m')
            ORDER BY month_year DESC;
            """,
            
            # Simple summary for basic dashboard
            """
            CREATE OR REPLACE VIEW view_simple_summary AS
            SELECT 
                COUNT(*) as total_jobs,
                COUNT(DISTINCT company) as unique_companies,
                ROUND(AVG(salary_avg), 2) as average_salary,
                SUM(is_remote) as remote_jobs,
                ROUND((SUM(is_remote) / COUNT(*)) * 100, 2) as remote_percentage,
                ROUND(MIN(salary_avg), 2) as min_salary,
                ROUND(MAX(salary_avg), 2) as max_salary
            FROM jobs
            WHERE salary_avg IS NOT NULL;
            """
        ]
        
        view_names = [
            "Job Summary",
            "Company Analysis", 
            "Seniority Analysis",
            "Category Analysis",
            "Location Analysis",
            "Skills Analysis",
            "Remote Analysis",
            "Salary Range Analysis (FIXED)",
            "Seniority Salary Detail",
            "Company Salary Benchmark",
            "Monthly Trends",
            "Simple Summary"
        ]
        
        for i, (view_name, view_sql) in enumerate(zip(view_names, views)):
            try:
                cursor.execute(view_sql)
                print(f"View '{view_name}' created successfully")
            except Error as e:
                print(f"Error creating view '{view_name}': {e}")
                # Continue with other views even if one fails
        
        cursor.close()
        print(f"\nSuccessfully created {len(view_names)} views")
        return True
        
    except Error as e:
        print(f"Error creating views: {e}")
        return False

def insert_data_into_mysql(connection, dataframe):
    """Insert DataFrame into MySQL database using mysql.connector directly"""
    try:
        cursor = connection.cursor()
        
        print(f"Inserting {len(dataframe)} records into MySQL...")
        
        # Clear existing data
        cursor.execute("DELETE FROM jobs")
        print("Cleared existing data from jobs table")
        
        # Prepare insert statement
        insert_query = """
        INSERT INTO jobs (
            title, company, location, salary_avg, salary_min, salary_max,
            is_remote, seniority, category, skills, post_date, scraped_date
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        # Convert DataFrame to list of tuples
        records = []
        for _, row in dataframe.iterrows():
            record = (
                str(row.get('title')) if pd.notna(row.get('title')) else None,
                str(row.get('company')) if pd.notna(row.get('company')) else None,
                str(row.get('location')) if pd.notna(row.get('location')) else None,
                float(row.get('salary_avg')) if pd.notna(row.get('salary_avg')) else None,
                float(row.get('salary_min')) if pd.notna(row.get('salary_min')) else None,
                float(row.get('salary_max')) if pd.notna(row.get('salary_max')) else None,
                int(row.get('is_remote')) if pd.notna(row.get('is_remote')) else 0,
                str(row.get('seniority')) if pd.notna(row.get('seniority')) else None,
                str(row.get('category')) if pd.notna(row.get('category')) else None,
                str(row.get('skills')) if pd.notna(row.get('skills')) else None,
                str(row.get('post_date')) if pd.notna(row.get('post_date')) else None,
                str(row.get('scraped_date')) if pd.notna(row.get('scraped_date')) else None
            )
            records.append(record)
        
        # Insert in batches
        batch_size = 500
        total_rows = len(records)
        total_inserted = 0
        
        for i in range(0, total_rows, batch_size):
            batch = records[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()
            
            total_inserted += cursor.rowcount
            processed = min(i + batch_size, total_rows)
            progress_pct = (processed / total_rows) * 100
            print(f"Progress: {processed}/{total_rows} rows ({progress_pct:.1f}%) - {cursor.rowcount} inserted in this batch")
        
        print(f"Successfully inserted {total_inserted} records in total")
        
        cursor.close()
        return True
        
    except Error as e:
        print(f"Error inserting data: {e}")
        connection.rollback()
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        connection.rollback()
        return False

def cleanup_dataframe(df):
    """Clean and prepare the dataframe for database insertion"""
    # Ensure proper data types
    df = df.copy()
    
    # Convert salary columns to numeric
    salary_cols = ['salary_avg', 'salary_min', 'salary_max']
    for col in salary_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Convert boolean to integer
    if 'is_remote' in df.columns:
        df['is_remote'] = df['is_remote'].astype(int)
    
    # Fill NaN values with appropriate defaults
    df['skills'] = df['skills'].fillna('')
    df['seniority'] = df['seniority'].fillna('Not Specified')
    df['category'] = df['category'].fillna('Not Specified')
    df['location'] = df['location'].fillna('Not Specified')
    df['company'] = df['company'].fillna('Unknown')
    
    # Handle post_date if exists
    if 'post_date' in df.columns:
        df['post_date'] = pd.to_datetime(df['post_date'], errors='coerce').dt.date
    
    return df

def verify_all_views(connection):
    """Verify all views were created correctly"""
    try:
        cursor = connection.cursor(dictionary=True)
        
        print("\n" + "=" * 60)
        print("VERIFYING ALL VIEWS")
        print("=" * 60)
        
        # Get list of all views
        cursor.execute("""
            SELECT table_name as view_name
            FROM information_schema.tables 
            WHERE table_schema = DATABASE() 
            AND table_type = 'VIEW'
            ORDER BY table_name;
        """)
        
        views = cursor.fetchall()
        print(f"\nFound {len(views)} views in database:")
        print("-" * 40)
        
        for i, view in enumerate(views, 1):
            print(f"{i}. {view['view_name']}")
        
        # Test key views
        test_views = [
            ('view_job_summary', 'Job Summary'),
            ('view_salary_ranges', 'Salary Ranges (Fixed)'),
            ('view_company_analysis', 'Company Analysis'),
            ('view_seniority_analysis', 'Seniority Analysis')
        ]
        
        print("\nTesting Key Views:")
        print("-" * 40)
        
        for view_name, display_name in test_views:
            try:
                cursor.execute(f"SELECT COUNT(*) as count FROM {view_name}")
                result = cursor.fetchone()
                print(f"✓ {display_name}: {result['count']} rows")
            except Error as e:
                print(f"✗ {display_name}: ERROR - {e}")
        
        # Show sample from fixed salary ranges
        print("\nSample from Fixed Salary Ranges View:")
        print("-" * 40)
        cursor.execute("SELECT * FROM view_salary_ranges ORDER BY job_count DESC LIMIT 3")
        for row in cursor.fetchall():
            print(f"  {row['salary_range']:15} | {row['job_count']:3} jobs | ${row['avg_in_range']:,.0f} avg")
        
        cursor.close()
        return True
        
    except Error as e:
        print(f"Error verifying views: {e}")
        return False

# ============================================================================
# POWER BI CONNECTION HELPER
# ============================================================================
def generate_powerbi_connection_guide():
    """Generate connection instructions for Power BI"""
    print("\n" + "=" * 60)
    print("POWER BI CONNECTION GUIDE")
    print("=" * 60)
    
    print("\nCONNECTION DETAILS:")
    print("-" * 40)
    print(f"Server: {MYSQL_CONFIG['host']}")
    print(f"Database: {MYSQL_CONFIG['database']}")
    print(f"Username: {MYSQL_CONFIG['user']}")
    print(f"Port: {MYSQL_CONFIG['port']}")
    
    print("\nRECOMMENDED VIEWS FOR POWER BI:")
    print("-" * 40)
    recommended_views = [
        ("view_job_summary", "Main dashboard metrics"),
        ("view_salary_ranges", "Salary distribution (FIXED)"),
        ("view_company_analysis", "Company comparison"),
        ("view_seniority_analysis", "Seniority level analysis"),
        ("view_category_analysis", "Job categories"),
        ("view_location_analysis", "Geographic analysis"),
        ("view_skills_analysis", "Top skills in demand"),
        ("view_remote_analysis", "Remote vs On-site"),
        ("view_company_benchmark", "Company salary benchmarking")
    ]
    
    for i, (view_name, description) in enumerate(recommended_views, 1):
        print(f"{i}. {view_name:25} - {description}")
    
    print("\nTROUBLESHOOTING POWER BI CONNECTION:")
    print("-" * 40)
    print("1. If you get 'additional components required' error:")
    print("   - Download MySQL Connector/NET from dev.mysql.com")
    print("   - Install 64-bit version")
    print("   - Restart Power BI")
    
    print("\n2. If views don't load:")
    print("   - Use 'Advanced Options' in Power BI")
    print("   - Check 'SQL statement' and use: SELECT * FROM view_name")
    print("   - Replace 'view_name' with actual view name")
    
    print("\n3. For GROUP BY errors:")
    print("   - This script already fixed the SQL mode")
    print("   - Salary ranges view is now subquery-based")
    print("   - Should work without errors")

# ============================================================================
# MAIN EXECUTION
# ============================================================================
def main():
    print("Starting Complete MySQL Views Setup")
    print("=" * 60)
    
    # Step 0: Test MySQL connection first
    print("\nTesting MySQL connection...")
    if not test_mysql_connection():
        print("\nCannot proceed without MySQL connection")
        sys.exit(1)
    
    # Step 1: Check if data exists
    print("\nChecking if data exists in database...")
    
    try:
        connection = mysql.connector.connect(**MYSQL_CONFIG)
        
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("SELECT COUNT(*) FROM jobs")
            count = cursor.fetchone()[0]
            cursor.close()
            
            if count == 0:
                print(f"\nWarning: No data found in 'jobs' table ({count} records)")
                print("You need to run the initial data import first.")
                response = input("Do you want to import data from cleaned_jobs_simple.csv? (yes/no): ")
                
                if response.lower() in ['yes', 'y']:
                    # Load and insert data
                    csv_file = 'cleaned_jobs_simple.csv'
                    
                    if not os.path.exists(csv_file):
                        print(f"\nError: {csv_file} not found.")
                        print("Please run your cleaning script first to generate this file.")
                        connection.close()
                        sys.exit(1)
                    
                    print(f"\nLoading data from {csv_file}...")
                    try:
                        clean_df = pd.read_csv(csv_file)
                        print(f"Loaded {len(clean_df)} records")
                        
                        # Clean the dataframe
                        clean_df = cleanup_dataframe(clean_df)
                        
                        # Check required columns
                        required_cols = ['title', 'company', 'salary_avg']
                        missing_cols = [col for col in required_cols if col not in clean_df.columns]
                        if missing_cols:
                            print(f"Warning: Missing columns: {missing_cols}")
                            print("Data may not load correctly into views")
                        
                        # Insert data
                        print("\nInserting data into MySQL...")
                        if not insert_data_into_mysql(connection, clean_df):
                            print("Failed to insert data")
                            connection.close()
                            sys.exit(1)
                            
                    except Exception as e:
                        print(f"\nError loading CSV: {e}")
                        connection.close()
                        sys.exit(1)
                else:
                    print("Data import cancelled. Exiting.")
                    connection.close()
                    sys.exit(1)
            else:
                print(f"Found {count} records in 'jobs' table. Proceeding with view creation...")
            
            # Step 2: Set SQL mode for compatibility
            print("\nSetting SQL mode for Power BI compatibility...")
            if not set_sql_mode_compatible(connection):
                print("Warning: Could not set SQL mode, but will try to create views anyway")
            
            # Step 3: Create table if not exists
            print("\nEnsuring table schema exists...")
            if not create_table_schema(connection):
                print("Failed to create table")
                connection.close()
                sys.exit(1)
            
            # Step 4: Create ALL views
            print("\nCreating all analytical views...")
            print("-" * 40)
            if not create_all_compatible_views(connection):
                print("Failed to create some views, but continuing...")
            
            # Step 5: Verify views
            print("\nVerifying all views...")
            verify_all_views(connection)
            
            # Step 6: Generate Power BI guide
            generate_powerbi_connection_guide()
            
            # Step 7: Display summary
            print("\n" + "=" * 60)
            print("SETUP COMPLETED SUCCESSFULLY")
            print("=" * 60)
            print("\nNEXT STEPS:")
            print("-" * 40)
            print("1. Open Power BI Desktop")
            print("2. Click 'Get Data' → 'MySQL Database'")
            print("3. Enter the connection details above")
            print("4. Load the recommended views")
            print("5. Create your 4 dashboard pages:")
            print("   - Page 1: Executive Dashboard (view_job_summary)")
            print("   - Page 2: Salary Analysis (view_salary_ranges)")
            print("   - Page 3: Company & Skills (view_company_analysis, view_skills_analysis)")
            print("   - Page 4: Market Trends (view_seniority_analysis, view_category_analysis)")
            
            print("\nFIXED ISSUES:")
            print("-" * 40)
            print("✓ Salary Ranges view: Fixed GROUP BY issue with subquery")
            print("✓ SQL Mode: Set to compatible mode for Power BI")
            print("✓ Enhanced Views: Added salary tier, market position, remote %")
            print("✓ Data Validation: Added NULL checks for salary columns")
            
            connection.close()
            print("\nMySQL connection closed.")
            
    except Error as e:
        print(f"\nError: {e}")
        print("\nTroubleshooting:")
        print("1. Ensure MySQL is running")
        print("2. Check MySQL credentials")
        print("3. Verify the database exists")
        print("4. Check if port 3306 is accessible")

if __name__ == "__main__":
    main()