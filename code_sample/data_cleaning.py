"""
Job Market Data Cleaning Pipeline

"""

import pandas as pd
import numpy as np
import logging
import os
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import argparse
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('data_cleaning.log')
    ]
)
logger = logging.getLogger(__name__)


class JobDataCleaner:
    """
    Professional data cleaning pipeline for job market analysis data.
    Handles data normalization, validation, and transformation.
    """
    
    def __init__(self, input_path: str, output_path: Optional[str] = None):
        """
        Initialize the data cleaner.
        
        Parameters:
        -----------
        input_path : str
            Path to the raw input CSV file
        output_path : str, optional
            Path for the cleaned output CSV file
        """
        self.input_path = Path(input_path)
        
        if output_path:
            self.output_path = Path(output_path)
        else:
            # Generate timestamped output filename
            timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
            self.output_path = Path(f"cleaned_jobs_{timestamp}.csv")
        
        self.dataframe = None
        self.required_columns = [
            "title", "company", "location", 
            "salary_avg", "salary_min", "salary_max", 
            "is_remote", "seniority", "category", 
            "skills", "post_date", "scraped_date"
        ]
        
        self._log_initialization()
    
    def _log_initialization(self) -> None:
        """Log initialization details."""
        logger.info("Initializing JobDataCleaner")
        logger.info(f"Input file: {self.input_path.absolute()}")
        logger.info(f"Output file: {self.output_path.absolute()}")
        logger.info(f"Required columns: {len(self.required_columns)}")
    
    def execute(self) -> bool:
        """
        Execute the complete data cleaning pipeline.
        
        Returns:
        --------
        bool
            True if pipeline completed successfully, False otherwise
        """
        logger.info("=" * 60)
        logger.info("Starting Data Cleaning Pipeline")
        logger.info("=" * 60)
        
        try:
            # Load and validate input data
            if not self._load_data():
                logger.error("Failed to load input data")
                return False
            
            # Execute cleaning steps
            cleaning_steps = [
                self._normalize_column_names,
                self._remove_duplicate_columns,
                self._process_salary_data,
                self._handle_remote_indicator,
                self._parse_date_columns,
                self._select_final_columns,
                self._save_cleaned_data
            ]
            
            for step in cleaning_steps:
                try:
                    step()
                    logger.info(f"Completed: {step.__name__}")
                except Exception as e:
                    logger.error(f"Failed in {step.__name__}: {e}")
                    return False
            
            self._log_summary()
            return True
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            return False
    
    def _load_data(self) -> bool:
        """
        Load data from CSV file.
        
        Returns:
        --------
        bool
            True if data loaded successfully, False otherwise
        """
        try:
            if not self.input_path.exists():
                logger.error(f"Input file not found: {self.input_path}")
                return False
            
            logger.info(f"Loading data from {self.input_path}")
            self.dataframe = pd.read_csv(self.input_path)
            
            logger.info(f"Loaded {len(self.dataframe):,} records")
            logger.info(f"Data shape: {self.dataframe.shape}")
            logger.debug(f"Columns: {list(self.dataframe.columns)}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to load data: {e}")
            return False
    
    def _normalize_column_names(self) -> None:
        """Normalize column names to consistent format."""
        logger.info("Normalizing column names")
        
        original_columns = self.dataframe.columns.tolist()
        
        # Transform column names to snake_case
        self.dataframe.columns = (
            self.dataframe.columns
            .astype(str)
            .str.lower()
            .str.strip()
            .str.replace(r'\s+', '_', regex=True)
        )
        
        # Log column name changes
        changes = []
        for old, new in zip(original_columns, self.dataframe.columns):
            if old != new:
                changes.append((old, new))
        
        if changes:
            logger.debug(f"Column name changes: {len(changes)}")
            for old, new in changes[:5]:  # Log first 5 changes
                logger.debug(f"  {old} -> {new}")
    
    def _remove_duplicate_columns(self) -> None:
        """Remove duplicate columns from the dataset."""
        duplicate_count = self.dataframe.columns.duplicated().sum()
        
        if duplicate_count > 0:
            logger.warning(f"Found {duplicate_count} duplicate columns")
            self.dataframe = self.dataframe.loc[:, ~self.dataframe.columns.duplicated()]
            logger.info("Duplicate columns removed")
        else:
            logger.debug("No duplicate columns found")
    
    def _process_salary_data(self) -> None:
        """
        Process and clean salary data.
        Handles currency symbols, missing values, and invalid ranges.
        """
        logger.info("Processing salary data")
        
        # Identify all salary-related columns
        salary_columns = [col for col in self.dataframe.columns 
                         if 'salary' in col.lower()]
        
        if salary_columns:
            logger.debug(f"Found salary columns: {salary_columns}")
            
            # Clean each salary column
            for column in salary_columns:
                self._clean_salary_column(column)
        
        # Ensure required salary columns exist
        self._ensure_required_salary_columns()
        
        # Validate salary ranges
        self._validate_salary_ranges()
        
        # Impute missing salary values
        self._impute_missing_salaries()
    
    def _clean_salary_column(self, column: str) -> None:
        """Clean individual salary column."""
        try:
            # Store original non-null count
            original_non_null = self.dataframe[column].notna().sum()
            
            # Remove currency symbols and commas
            self.dataframe[column] = (
                self.dataframe[column]
                .astype(str)
                .str.replace(r'[\$,₹,£,€,¥,]', '', regex=True)
                .str.replace(' ', '')
            )
            
            # Convert to numeric, coercing errors to NaN
            self.dataframe[column] = pd.to_numeric(
                self.dataframe[column], 
                errors='coerce'
            )
            
            # Log cleaning results
            cleaned_non_null = self.dataframe[column].notna().sum()
            logger.debug(
                f"Cleaned {column}: {original_non_null} -> {cleaned_non_null} "
                f"non-null values"
            )
            
        except Exception as e:
            logger.warning(f"Could not clean column {column}: {e}")
    
    def _ensure_required_salary_columns(self) -> None:
        """Ensure all required salary columns exist."""
        required_columns = ["salary_avg", "salary_min", "salary_max"]
        
        for column in required_columns:
            if column not in self.dataframe.columns:
                logger.warning(f"Creating missing column: {column}")
                self.dataframe[column] = np.nan
    
    def _validate_salary_ranges(self) -> None:
        """Validate salary ranges (min <= max)."""
        mask = (
            self.dataframe["salary_min"].notna() & 
            self.dataframe["salary_max"].notna() & 
            (self.dataframe["salary_min"] > self.dataframe["salary_max"])
        )
        
        invalid_count = mask.sum()
        if invalid_count > 0:
            logger.warning(
                f"Found {invalid_count:,} records with invalid salary ranges "
                f"(min > max)"
            )
            self.dataframe.loc[mask, ["salary_min", "salary_max"]] = np.nan
    
    def _impute_missing_salaries(self) -> None:
        """Impute missing salary values using logical rules."""
        # Calculate average from min/max where average is missing
        avg_mask = (
            self.dataframe["salary_avg"].isna() & 
            self.dataframe["salary_min"].notna() & 
            self.dataframe["salary_max"].notna()
        )
        
        if avg_mask.any():
            self.dataframe.loc[avg_mask, "salary_avg"] = (
                self.dataframe.loc[avg_mask, "salary_min"] + 
                self.dataframe.loc[avg_mask, "salary_max"]
            ) / 2
            
            logger.debug(
                f"Calculated salary_avg for {avg_mask.sum():,} records"
            )
        
        # Fill remaining missing values with column medians
        for column in ["salary_avg", "salary_min", "salary_max"]:
            if column in self.dataframe.columns:
                missing_count = self.dataframe[column].isna().sum()
                
                if missing_count > 0:
                    median_value = self.dataframe[column].median()
                    self.dataframe[column] = self.dataframe[column].fillna(median_value)
                    
                    logger.debug(
                        f"Filled {missing_count:,} missing values in {column} "
                        f"with median: {median_value:,.0f}"
                    )
    
    def _handle_remote_indicator(self) -> None:
        """Standardize remote work indicator."""
        logger.info("Processing remote work indicator")
        
        if "is_remote" not in self.dataframe.columns:
            logger.info("Creating default is_remote column")
            self.dataframe["is_remote"] = 0
            return
        
        # Standardize boolean values
        remote_mapping = {
            'true': 1, 'false': 0,
            'yes': 1, 'no': 0,
            '1': 1, '0': 0,
            'remote': 1, 'onsite': 0,
            'hybrid': 0
        }
        
        # Clean and map values
        original_values = self.dataframe["is_remote"].astype(str).str.lower()
        
        self.dataframe["is_remote"] = (
            original_values
            .map(remote_mapping)
            .fillna(0)
            .astype(int)
        )
        
        # Log remote work statistics
        remote_count = self.dataframe["is_remote"].sum()
        total_count = len(self.dataframe)
        remote_percentage = (remote_count / total_count) * 100
        
        logger.info(
            f"Remote positions: {remote_count:,}/{total_count:,} "
            f"({remote_percentage:.1f}%)"
        )
    
    def _parse_date_columns(self) -> None:
        """Parse date columns to datetime format."""
        logger.info("Parsing date columns")
        
        date_columns = ["post_date", "scraped_date"]
        
        for column in date_columns:
            if column in self.dataframe.columns:
                try:
                    # Store original data type
                    original_dtype = str(self.dataframe[column].dtype)
                    
                    # Parse to datetime
                    self.dataframe[column] = pd.to_datetime(
                        self.dataframe[column], 
                        errors='coerce'
                    )
                    
                    # Log parsing results
                    null_count = self.dataframe[column].isna().sum()
                    logger.debug(
                        f"Parsed {column}: {original_dtype} -> datetime, "
                        f"{null_count} null values"
                    )
                    
                except Exception as e:
                    logger.warning(f"Could not parse {column}: {e}")
    
    def _select_final_columns(self) -> None:
        """Select and order final columns for output."""
        logger.info("Selecting final columns")
        
        # Create missing columns with appropriate defaults
        for column in self.required_columns:
            if column not in self.dataframe.columns:
                if column.startswith("salary"):
                    self.dataframe[column] = np.nan
                elif column == "is_remote":
                    self.dataframe[column] = 0
                elif column in ["post_date", "scraped_date"]:
                    self.dataframe[column] = pd.NaT
                else:
                    self.dataframe[column] = ""
        
        # Select existing required columns
        existing_required = [
            col for col in self.required_columns 
            if col in self.dataframe.columns
        ]
        
        # Keep additional columns for reference
        additional_columns = [
            col for col in self.dataframe.columns 
            if col not in self.required_columns
        ]
        
        # Combine columns (required first, then additional)
        final_columns = existing_required + additional_columns
        self.dataframe = self.dataframe[final_columns].copy()
        
        logger.info(f"Selected {len(existing_required)} required columns")
        if additional_columns:
            logger.info(f"Retained {len(additional_columns)} additional columns")
    
    def _save_cleaned_data(self) -> None:
        """Save cleaned data to CSV file."""
        try:
            # Ensure output directory exists
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save to CSV
            self.dataframe.to_csv(self.output_path, index=False)
            
            # Log file information
            file_size = self.output_path.stat().st_size / 1024  # KB
            logger.info(f"Saved cleaned data to {self.output_path}")
            logger.info(f"Records saved: {len(self.dataframe):,}")
            logger.info(f"Columns saved: {len(self.dataframe.columns)}")
            logger.info(f"File size: {file_size:.1f} KB")
            
        except Exception as e:
            logger.error(f"Failed to save cleaned data: {e}")
            raise
    
    def _log_summary(self) -> None:
        """Log summary statistics of cleaned data."""
        logger.info("\n" + "=" * 50)
        logger.info("Cleaning Summary")
        logger.info("=" * 50)
        
        logger.info(f"Total records: {len(self.dataframe):,}")
        logger.info(f"Total columns: {len(self.dataframe.columns)}")
        
        # Missing values summary
        missing_counts = self.dataframe.isnull().sum()
        missing_total = missing_counts.sum()
        
        if missing_total > 0:
            logger.warning(f"Total missing values: {missing_total:,}")
            
            # Top 5 columns with most missing values
            top_missing = missing_counts.nlargest(5)
            for col, count in top_missing.items():
                percentage = (count / len(self.dataframe)) * 100
                if count > 0:
                    logger.info(f"  {col}: {count:,} ({percentage:.1f}%)")
        
        # Salary statistics
        if "salary_avg" in self.dataframe.columns:
            salary_stats = {
                "Average": self.dataframe["salary_avg"].mean(),
                "Median": self.dataframe["salary_avg"].median(),
                "Minimum": self.dataframe["salary_avg"].min(),
                "Maximum": self.dataframe["salary_avg"].max()
            }
            
            logger.info("\nSalary Statistics:")
            for stat, value in salary_stats.items():
                if not pd.isna(value):
                    logger.info(f"  {stat}: ${value:,.0f}")
        
        # Remote work summary
        if "is_remote" in self.dataframe.columns:
            remote_percentage = self.dataframe["is_remote"].mean() * 100
            logger.info(f"Remote positions: {remote_percentage:.1f}%")
        
        logger.info("\n" + "=" * 50)


def parse_arguments():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='Clean job market data for analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python clean_job_data.py raw_jobs.csv
  python clean_job_data.py input.csv --output cleaned_data.csv
  python clean_job_data.py data/raw.csv --log-level DEBUG
        """
    )
    
    parser.add_argument(
        'input_file',
        nargs='?',
        default=None,
        help='Path to raw input CSV file (default: JOB_INPUT env var or auto-discovered job_data*.csv)'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='Output file path (default: auto-generated)',
        default=None
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )
    
    return parser.parse_args()


def find_input_file() -> Optional[str]:
    """Auto-discover a suitable input CSV file if one is not provided."""
    # Check JOB_INPUT env var first
    env_input = os.environ.get('JOB_INPUT')
    candidates = []
    if env_input:
        candidates.append(Path(env_input))

    # Check common filenames in project root
    candidates.extend(Path('.').glob('job_data*.csv'))

    # Check collected data directory
    collected_dir = Path('JOB_MARKET_ANALYSIS') / 'collected data'
    if collected_dir.exists():
        candidates.extend(collected_dir.glob('*.csv'))

    # Also search recursively for job_data*.csv
    candidates.extend(Path('.').rglob('job_data*.csv'))

    # Filter to existing files and remove duplicates
    existing = sorted({p.resolve() for p in candidates if p and p.exists()}, key=lambda p: p.stat().st_mtime, reverse=True)
    if not existing:
        return None
    chosen = existing[0]
    logger.info(f"Auto-selected input file: {chosen}")
    return str(chosen)


def main():

    """Main execution function."""
    args = parse_arguments()
    
    # Configure logging
    logger.setLevel(getattr(logging, args.log_level))
    
    # Determine input file (argument -> JOB_INPUT env -> auto-discovery)
    input_path = args.input_file or os.environ.get('JOB_INPUT') or find_input_file()
    if not input_path or not Path(input_path).exists():
        logger.error(f"Input file not found: {input_path or 'None'}")
        logger.error("Supply an input file path, set JOB_INPUT, or place a job_data*.csv in the project (e.g., JOB_MARKET_ANALYSIS/collected data/).")
        sys.exit(1)
    
    # Initialize and run cleaner
    cleaner = JobDataCleaner(str(input_path), args.output)
    
    if cleaner.execute():
        logger.info("Data cleaning completed successfully")
        sys.exit(0)
    else:
        logger.error("Data cleaning failed")
        sys.exit(1)


if __name__ == "__main__":
    main()