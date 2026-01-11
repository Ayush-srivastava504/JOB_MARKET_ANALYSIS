"""
Power BI View Maintenance Script
Clean and maintain database views used in Power BI reports
"""

import mysql.connector
from mysql.connector import Error
import sys
import os
import configparser
from datetime import datetime, timedelta

class PowerBIViewMaintenance:
    """Tool to clean and maintain Power BI database views"""
    
    def __init__(self, config_file="config.ini"):
        self.config = self._load_config(config_file)
        self.connection = None
        self.database = self.config.get('database', 'job_analysis_db')
        
        # Essential views to never delete (your Power BI report views)
        self.protected_views = {
            'view_monthly_trends',
            'view_date_dimension',
            'view_location_analysis',
            'view_company_analysis',
            'view_job_summary',
            'view_job_market_overview',
            'jobs'  # Main table
        }
    
    def _load_config(self, config_file):
        """Load database configuration from file"""
        config = {
            'host': 'localhost',
            'port': 3306,
            'user': 'root',
            'password': '',
            'database': 'job_analysis_db'
        }
        
        if os.path.exists(config_file):
            try:
                parser = configparser.ConfigParser()
                parser.read(config_file)
                if 'database' in parser:
                    db_config = parser['database']
                    for key in config:
                        if key in db_config:
                            if key == 'port':
                                config[key] = int(db_config[key])
                            else:
                                config[key] = db_config[key]
            except Exception as e:
                print(f"Note: Couldn't read config file: {e}")
                print("Using default settings...")
        
        return config
    
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(
                host=self.config['host'],
                port=self.config['port'],
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database']
            )
            
            if self.connection.is_connected():
                print(f"Connected to database: {self.config['database']}")
                return True
            return False
            
        except Error as e:
            print(f"Connection error: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Disconnected from database")
    
    def list_all_views(self):
        """Show all views in the database"""
        cursor = None
        try:
            cursor = self.connection.cursor(buffered=True, dictionary=True)
            
            cursor.execute("""
                SELECT TABLE_NAME as view_name, 
                       CREATE_TIME as created,
                       UPDATE_TIME as last_updated
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s 
                    AND TABLE_TYPE = 'VIEW'
                ORDER BY TABLE_NAME
            """, (self.database,))
            
            views = cursor.fetchall()
            cursor.close()
            
            if not views:
                print("No views found in the database")
                return []
            
            print(f"\nFound {len(views)} views:")
            print("-" * 70)
            print(f"{'View Name':<35} {'Created':<20} {'Status':<15}")
            print("-" * 70)
            
            for view in views:
                view_name = view['view_name']
                created = view['created'].strftime('%Y-%m-%d') if view['created'] else 'Unknown'
                
                # Check if view is protected
                if view_name in self.protected_views:
                    status = "Protected"
                else:
                    status = "Can clean"
                
                print(f"{view_name:<35} {created:<20} {status:<15}")
            
            return views
            
        except Error as e:
            print(f"Error listing views: {e}")
            return []
    
    def find_old_views(self, months_old=6):
        """Find views older than specified months, handling future dates"""
        cursor = None
        try:
            cursor = self.connection.cursor(buffered=True, dictionary=True)
            
            cursor.execute("""
                SELECT TABLE_NAME as view_name, 
                       CREATE_TIME as created,
                       UPDATE_TIME as last_updated
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s 
                    AND TABLE_TYPE = 'VIEW'
                ORDER BY CREATE_TIME
            """, (self.database,))
            
            views = cursor.fetchall()
            cursor.close()
            
            old_views = []
            current_date = datetime.now()
            
            # Calculate cutoff date (6 months ago)
            if current_date.month <= months_old:
                cutoff_month = 12 + current_date.month - months_old
                cutoff_year = current_date.year - 1
            else:
                cutoff_month = current_date.month - months_old
                cutoff_year = current_date.year
            
            cutoff_date = datetime(cutoff_year, cutoff_month, 1)
            
            print(f"\nCurrent system date: {current_date.strftime('%Y-%m-%d')}")
            print(f"Looking for views created before: {cutoff_date.strftime('%B %Y')}")
            print("-" * 70)
            
            for view in views:
                view_name = view['view_name']
                
                # Skip protected views
                if view_name in self.protected_views:
                    continue
                
                created = view['created']
                if created:
                    # Handle dates in the future (if system date is wrong)
                    if created > current_date:
                        print(f"WARNING: {view_name:<35} has FUTURE date: {created.strftime('%Y-%m-%d')}")
                        # If it's more than 1 month in the future, consider it old
                        if (created - current_date).days > 30:
                            old_views.append(view_name)
                            print(f"  -> Marking as old (future date)")
                    elif created < cutoff_date:
                        old_views.append(view_name)
                        created_str = created.strftime('%Y-%m-%d')
                        print(f"{view_name:<35} (Created: {created_str})")
            
            if not old_views:
                print("No old views found to clean up")
            
            return old_views
            
        except Error as e:
            print(f"Error finding old views: {e}")
            return []
    
    def clean_all_non_essential_views(self):
        """Clean ALL non-essential views regardless of age"""
        cursor = None
        try:
            cursor = self.connection.cursor(buffered=True, dictionary=True)
            
            cursor.execute("""
                SELECT TABLE_NAME as view_name, 
                       CREATE_TIME as created
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s 
                    AND TABLE_TYPE = 'VIEW'
                ORDER BY TABLE_NAME
            """, (self.database,))
            
            views = cursor.fetchall()
            cursor.close()
            
            # Identify non-essential views
            non_essential_views = []
            for view in views:
                view_name = view['view_name']
                created = view['created'].strftime('%Y-%m-%d') if view['created'] else 'Unknown'
                
                if view_name not in self.protected_views:
                    non_essential_views.append({
                        'name': view_name,
                        'created': created
                    })
            
            if not non_essential_views:
                print("No non-essential views found.")
                return
            
            print(f"\nFound {len(non_essential_views)} non-essential views:")
            print("-" * 70)
            for view in non_essential_views:
                print(f"{view['name']:<35} (Created: {view['created']})")
            
            response = input(f"\nDelete ALL {len(non_essential_views)} non-essential views? (yes/no): ")
            if response.lower() not in ['yes', 'y']:
                print("Cleanup cancelled.")
                return
            
            success_count = 0
            failed_count = 0
            
            for view_info in non_essential_views:
                view_name = view_info['name']
                try:
                    cursor = self.connection.cursor(buffered=True)
                    
                    # Create backup first
                    backup_name = f"{view_name}_backup_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                    try:
                        cursor.execute(f"CREATE VIEW `{backup_name}` AS SELECT * FROM `{view_name}`")
                        self.connection.commit()
                        print(f"Created backup: {backup_name}")
                    except Exception as backup_error:
                        print(f"Note: Could not create backup for {view_name} (might be broken): {backup_error}")
                    
                    # Drop the view
                    cursor.execute(f"DROP VIEW IF EXISTS `{view_name}`")
                    self.connection.commit()
                    cursor.close()
                    
                    print(f"Deleted: {view_name}")
                    success_count += 1
                    
                except Error as e:
                    print(f"Failed to delete {view_name}: {e}")
                    failed_count += 1
            
            print(f"\nCleanup complete:")
            print(f"  Successfully deleted: {success_count} views")
            print(f"  Failed to delete: {failed_count} views")
            
            if success_count > 0:
                print(f"\nNote: Backups were created with '_backup_cleanup_YYYYMMDD_HHMMSS' suffix")
                print("      You can manually delete them when you're sure they're not needed")
            
        except Error as e:
            print(f"Error during cleanup: {e}")
    
    def clean_old_views(self, view_names=None, months_old=6):
        """Clean up old views with backup creation"""
        if view_names is None:
            view_names = self.find_old_views(months_old)
        
        if not view_names:
            print("Nothing to clean up")
            return
        
        print(f"\nFound {len(view_names)} old views to clean up:")
        for view in view_names:
            print(f"  - {view}")
        
        # Ask for confirmation
        response = input(f"\nDelete these {len(view_names)} old views? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Cleanup cancelled")
            return
        
        success_count = 0
        failed_count = 0
        
        try:
            for view_name in view_names:
                cursor = None
                try:
                    # Double-check it's not a protected view
                    if view_name in self.protected_views:
                        print(f"Skipping protected view: {view_name}")
                        continue
                    
                    cursor = self.connection.cursor(buffered=True)
                    
                    # Create backup first
                    backup_name = f"{view_name}_backup_{datetime.now().strftime('%Y%m%d')}"
                    try:
                        cursor.execute(f"CREATE VIEW `{backup_name}` AS SELECT * FROM `{view_name}`")
                        self.connection.commit()
                        print(f"Created backup: {backup_name}")
                    except Exception as backup_error:
                        print(f"Note: Could not create backup for {view_name} (might be broken): {backup_error}")
                    
                    # Drop the old view
                    cursor.execute(f"DROP VIEW IF EXISTS `{view_name}`")
                    self.connection.commit()
                    print(f"Deleted: {view_name}")
                    success_count += 1
                    
                except Error as e:
                    print(f"Failed to delete {view_name}: {e}")
                    failed_count += 1
                finally:
                    if cursor:
                        cursor.close()
            
            print(f"\nCleanup complete:")
            print(f"  Successfully deleted: {success_count} views")
            print(f"  Failed to delete: {failed_count} views")
            
            if success_count > 0:
                print(f"\nNote: Backups were created with '_backup_YYYYMMDD' suffix")
                print("      You can manually delete them when you're sure they're not needed")
            
        except Error as e:
            print(f"Error during cleanup: {e}")
    
    def validate_views(self):
        """Check if views are still working - FIXED VERSION"""
        cursor = None
        try:
            cursor = self.connection.cursor(buffered=True, dictionary=True)
            
            cursor.execute("""
                SELECT TABLE_NAME as view_name
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s 
                    AND TABLE_TYPE = 'VIEW'
                ORDER BY TABLE_NAME
            """, (self.database,))
            
            views = cursor.fetchall()
            cursor.close()
            
            print("\nValidating views...")
            print("-" * 70)
            
            working_views = []
            broken_views = []
            
            for view in views:
                view_name = view['view_name']
                
                try:
                    # Use a new cursor for each query to avoid "Unread result found" error
                    test_cursor = self.connection.cursor(buffered=True)
                    
                    # Test the view with a simple query
                    test_cursor.execute(f"SELECT 1 FROM `{view_name}` LIMIT 1")
                    test_cursor.fetchall()  # Consume all results
                    test_cursor.close()
                    
                    # Get column info with a new cursor
                    desc_cursor = self.connection.cursor(buffered=True, dictionary=True)
                    desc_cursor.execute(f"DESCRIBE `{view_name}`")
                    columns = desc_cursor.fetchall()
                    desc_cursor.close()
                    
                    print(f"{view_name:<35} (OK, {len(columns)} columns)")
                    working_views.append(view_name)
                    
                except Error as e:
                    print(f"{view_name:<35} (BROKEN: {str(e)[:50]}...)")
                    broken_views.append(view_name)
            
            print(f"\nValidation results:")
            print(f"  Working: {len(working_views)} views")
            print(f"  Broken: {len(broken_views)} views")
            
            if broken_views:
                print(f"\nBroken views found:")
                for i, view in enumerate(broken_views, 1):
                    print(f"  {i:2}. {view}")
            
            return working_views, broken_views
            
        except Error as e:
            print(f"Error validating views: {e}")
            return [], []
    
    def fix_broken_views(self):
        """Attempt to fix broken views by recreating them"""
        print("\nAttempting to fix broken views...")
        
        # First, validate to see which views are broken
        working_views, broken_views = self.validate_views()
        
        if not broken_views:
            print("\nNo broken views to fix.")
            return
        
        print(f"\nFound {len(broken_views)} broken views.")
        print("This will attempt to recreate the views from your backup views.")
        
        response = input("Continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Fix cancelled.")
            return
        
        fixed_count = 0
        failed_count = 0
        
        for view_name in broken_views:
            try:
                cursor = self.connection.cursor(buffered=True)
                
                # Check if there's a backup version
                backup_view = None
                
                # Look for backup views
                cursor.execute("""
                    SELECT TABLE_NAME 
                    FROM information_schema.TABLES 
                    WHERE TABLE_SCHEMA = %s 
                        AND TABLE_TYPE = 'VIEW'
                        AND TABLE_NAME LIKE %s
                    ORDER BY CREATE_TIME DESC
                    LIMIT 1
                """, (self.database, f"{view_name}_backup%"))
                
                backup_result = cursor.fetchone()
                
                if backup_result:
                    backup_view = backup_result[0]
                    print(f"\nFound backup: {backup_view}")
                    
                    # Try to recreate the original view from backup
                    try:
                        # Drop the broken view if it exists
                        cursor.execute(f"DROP VIEW IF EXISTS `{view_name}`")
                        
                        # Create the view from backup
                        cursor.execute(f"CREATE VIEW `{view_name}` AS SELECT * FROM `{backup_view}`")
                        self.connection.commit()
                        
                        print(f"  Recreated {view_name} from {backup_view}")
                        fixed_count += 1
                        
                    except Error as e:
                        print(f"  Failed to recreate {view_name}: {e}")
                        failed_count += 1
                
                else:
                    print(f"\nNo backup found for {view_name}")
                    failed_count += 1
                
                cursor.close()
                
            except Error as e:
                print(f"Error processing {view_name}: {e}")
                failed_count += 1
        
        print(f"\nFix attempt complete:")
        print(f"  Fixed: {fixed_count} views")
        print(f"  Failed: {failed_count} views")
        
        if fixed_count > 0:
            print("\nRe-validating fixed views...")
            self.validate_views()
    
    def clean_backup_views(self):
        """Clean up backup views to save space"""
        cursor = None
        try:
            cursor = self.connection.cursor(buffered=True, dictionary=True)
            
            cursor.execute("""
                SELECT TABLE_NAME as view_name, 
                       CREATE_TIME as created
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s 
                    AND TABLE_TYPE = 'VIEW'
                    AND (TABLE_NAME LIKE '%_backup_%' OR TABLE_NAME LIKE '%_backup_cleanup_%')
                ORDER BY CREATE_TIME
            """, (self.database,))
            
            backup_views = cursor.fetchall()
            cursor.close()
            
            if not backup_views:
                print("No backup views found.")
                return
            
            print(f"\nFound {len(backup_views)} backup views:")
            print("-" * 70)
            
            for view in backup_views[:15]:  # Show first 15
                view_name = view['view_name']
                created = view['created'].strftime('%Y-%m-%d') if view['created'] else 'Unknown'
                print(f"{view_name:<45} (Created: {created})")
            
            if len(backup_views) > 15:
                print(f"... and {len(backup_views) - 15} more backup views")
            
            response = input(f"\nDelete all {len(backup_views)} backup views? (yes/no): ")
            if response.lower() not in ['yes', 'y']:
                print("Cleanup cancelled.")
                return
            
            success_count = 0
            failed_count = 0
            
            for view in backup_views:
                try:
                    cursor = self.connection.cursor(buffered=True)
                    view_name = view['view_name']
                    
                    cursor.execute(f"DROP VIEW IF EXISTS `{view_name}`")
                    self.connection.commit()
                    cursor.close()
                    
                    print(f"Deleted backup: {view_name}")
                    success_count += 1
                    
                except Error as e:
                    print(f"Failed to delete {view_name}: {e}")
                    failed_count += 1
            
            print(f"\nBackup cleanup complete:")
            print(f"  Successfully deleted: {success_count} backup views")
            print(f"  Failed to delete: {failed_count} backup views")
            
        except Error as e:
            print(f"Error cleaning backup views: {e}")
    
    def check_essential_views(self):
        """Check if essential Power BI views exist"""
        print("\nChecking essential Power BI views...")
        
        cursor = None
        try:
            cursor = self.connection.cursor(buffered=True, dictionary=True)
            
            cursor.execute("""
                SELECT TABLE_NAME as view_name
                FROM information_schema.TABLES 
                WHERE TABLE_SCHEMA = %s 
                    AND TABLE_TYPE = 'VIEW'
                    AND TABLE_NAME IN (%s, %s, %s, %s, %s, %s)
            """, (self.database, 'view_monthly_trends', 'view_date_dimension', 
                  'view_location_analysis', 'view_company_analysis', 
                  'view_job_summary', 'view_job_market_overview'))
            
            existing_views = {row['view_name'] for row in cursor.fetchall()}
            cursor.close()
            
            # Check which essential views are missing
            essential_views = {
                'view_monthly_trends',
                'view_date_dimension',
                'view_location_analysis',
                'view_company_analysis',
                'view_job_summary',
                'view_job_market_overview'
            }
            
            missing_views = essential_views - existing_views
            
            if missing_views:
                print(f"Missing {len(missing_views)} essential views:")
                for view in missing_views:
                    print(f"  - {view}")
                
                response = input(f"\nDo you want to see SQL to recreate these views? (yes/no): ")
                if response.lower() in ['yes', 'y']:
                    print("\nSQL to recreate missing views:")
                    print("-" * 70)
                    
                    # Provide SQL templates for each missing view
                    for view_name in missing_views:
                        if view_name == 'view_monthly_trends':
                            print(f"\n-- {view_name}:")
                            print("""CREATE OR REPLACE VIEW view_monthly_trends AS
SELECT 
    YEAR(post_date) as year,
    MONTH(post_date) as month,
    DATE_FORMAT(post_date, '%Y-%m') as year_month,
    COUNT(*) as job_count,
    ROUND(AVG(COALESCE(salary_avg, 0)), 2) as avg_salary
FROM jobs
WHERE post_date IS NOT NULL
GROUP BY YEAR(post_date), MONTH(post_date), DATE_FORMAT(post_date, '%Y-%m')
ORDER BY year DESC, month DESC;""")
                        
                        elif view_name == 'view_date_dimension':
                            print(f"\n-- {view_name}:")
                            print("""CREATE OR REPLACE VIEW view_date_dimension AS
WITH RECURSIVE dates AS (
    SELECT DATE('2023-01-01') as date_key
    UNION ALL
    SELECT date_key + INTERVAL 1 DAY
    FROM dates
    WHERE date_key < DATE('2024-12-31')
)
SELECT 
    date_key,
    YEAR(date_key) as year,
    MONTH(date_key) as month,
    DATE_FORMAT(date_key, '%Y-%m') as year_month
FROM dates
ORDER BY date_key;""")
                        
                        elif view_name == 'view_location_analysis':
                            print(f"\n-- {view_name}:")
                            print("""CREATE OR REPLACE VIEW view_location_analysis AS
SELECT 
    COALESCE(NULLIF(location, ''), 'Not Specified') as location,
    COUNT(*) as job_count,
    ROUND(AVG(COALESCE(salary_avg, 0)), 2) as average_salary
FROM jobs
WHERE salary_avg IS NOT NULL
GROUP BY location
ORDER BY job_count DESC;""")
                    
                    print("\nNote: Run these SQL statements in your database client.")
            else:
                print("All essential views are present")
                
        except Error as e:
            print(f"Error checking views: {e}")

def show_menu():
    """Display the main menu"""
    print("\n" + "="*70)
    print("POWER BI VIEW MAINTENANCE TOOL")
    print("="*70)
    print("\nOptions:")
    print("1. List all views")
    print("2. Find old views (6+ months)")
    print("3. Clean old views")
    print("4. Clean ALL non-essential views (force)")
    print("5. Validate views (check if working)")
    print("6. Fix broken views")
    print("7. Clean backup views")
    print("8. Check essential Power BI views")
    print("9. Exit")
    
    while True:
        choice = input("\nEnter your choice (1-9): ").strip()
        if choice in ['1', '2', '3', '4', '5', '6', '7', '8', '9']:
            return choice
        print("Invalid choice. Please enter 1-9.")

def main():
    """Main function"""
    print("Starting Power BI View Maintenance...")
    
    # Initialize the maintenance tool
    tool = PowerBIViewMaintenance()
    
    if not tool.connect():
        print("Cannot connect to database. Check your config.ini file.")
        print("\nCreate a config.ini file with:")
        print("[database]")
        print("host = localhost")
        print("user = root")
        print("password = your_password")
        print("database = job_analysis_db")
        return
    
    try:
        while True:
            choice = show_menu()
            
            if choice == '1':
                tool.list_all_views()
                
            elif choice == '2':
                try:
                    months = int(input("How many months old? (default 6): ") or "6")
                    tool.find_old_views(months)
                except ValueError:
                    print("Please enter a valid number")
                
            elif choice == '3':
                try:
                    months = int(input("Clean views older than how many months? (default 6): ") or "6")
                    tool.clean_old_views(months_old=months)
                except ValueError:
                    print("Please enter a valid number")
                
            elif choice == '4':
                tool.clean_all_non_essential_views()
                
            elif choice == '5':
                tool.validate_views()
                
            elif choice == '6':
                tool.fix_broken_views()
                
            elif choice == '7':
                tool.clean_backup_views()
                
            elif choice == '8':
                tool.check_essential_views()
                
            elif choice == '9':
                print("\nGoodbye!")
                break
            
            input("\nPress Enter to continue...")
            
    except KeyboardInterrupt:
        print("\n\nStopped by user")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
    finally:
        tool.disconnect()

if __name__ == "__main__":
    main()