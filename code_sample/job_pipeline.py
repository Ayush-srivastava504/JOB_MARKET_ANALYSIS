import requests
import time
import logging
import pandas as pd
import sqlite3
import json
import csv
import hashlib
import os
import re
from datetime import datetime
from typing import List, Dict, Optional, Any, Tuple
from pathlib import Path
import sys
from dataclasses import dataclass
from dotenv import load_dotenv  

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('job_pipeline.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Configuration errors"""
    pass


@dataclass
class APIConfig:
    """Manage API configurations using environment variables"""
    
    @staticmethod
    def get_adzuna_config() -> Dict[str, str]:
        """
        Get Adzuna API credentials from environment variables
        Required: ADZUNA_APP_ID, ADZUNA_APP_KEY
        """
        app_id = os.getenv('ADZUNA_APP_ID')
        app_key = os.getenv('ADZUNA_APP_KEY')
        
        if not app_id or not app_key:
            raise ConfigError(
                "Missing Adzuna API credentials. Set ADZUNA_APP_ID and ADZUNA_APP_KEY "
                "environment variables or add them to a .env file."
            )
        
        return {
            'base_url': "https://api.adzuna.com/v1/api/jobs",
            'app_id': app_id,
            'app_key': app_key
        }
    
    @staticmethod
    def get_usajobs_config() -> Dict[str, str]:
        """
        Get USAJOBS API credentials from environment variables
        Required: USAJOBS_API_KEY, USAJOBS_USER_EMAIL
        """
        api_key = os.getenv('USAJOBS_API_KEY')
        user_email = os.getenv('USAJOBS_USER_EMAIL')
        
        if not api_key or not user_email:
            raise ConfigError(
                "Missing USAJOBS API credentials. Set USAJOBS_API_KEY and "
                "USAJOBS_USER_EMAIL environment variables."
            )
        
        return {
            'base_url': "https://data.usajobs.gov/api",
            'api_key': api_key,
            'user_email': user_email
        }
    
    @staticmethod
    def validate_all_configs() -> Tuple[bool, List[str]]:
        """Check all required API configurations"""
        errors = []
        
        try:
            APIConfig.get_adzuna_config()
        except ConfigError as e:
            errors.append(str(e))
        
        try:
            APIConfig.get_usajobs_config()
        except ConfigError as e:
            errors.append(str(e))
        
        return len(errors) == 0, errors


class AdzunaCollector:
    """Collect job data from Adzuna API"""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or APIConfig.get_adzuna_config()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'JobDataCollector/1.0',
            'Accept': 'application/json'
        })
    
    def fetch_jobs(self, keywords: str, country: str = 'us', 
                   max_results: int = 1000, delay: float = 1.0) -> List[Dict]:
        """
        Fetch jobs from Adzuna with pagination
        
        Args:
            keywords: Search terms
            country: Country code (default 'us')
            max_results: Maximum results to fetch
            delay: Delay between requests in seconds
        """
        all_jobs = []
        results_per_page = 50
        max_pages = (max_results // results_per_page) + 1
        
        for page in range(1, max_pages + 1):
            try:
                url = f"{self.config['base_url']}/{country}/search/{page}"
                
                params = {
                    'app_id': self.config['app_id'],
                    'app_key': self.config['app_key'],
                    'what': keywords,
                    'results_per_page': results_per_page,
                    'where': 'United States',
                    'max_days_old': 30,
                    'sort_by': 'date',
                    'content-type': 'application/json'
                }
                
                logger.info(f"Fetching Adzuna page {page} for '{keywords}'")
                
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 400:
                    logger.warning(f"API returned 400 for page {page}")
                    error_data = response.json() if response.content else {}
                    
                    if 'error' in error_data and 'app_id' in str(error_data.get('error', '')):
                        logger.error("Adzuna authentication failed")
                    break
                
                response.raise_for_status()
                
                data = response.json()
                jobs = data.get('results', [])
                
                if not jobs:
                    logger.info("No more jobs available")
                    break
                
                standardized_jobs = self._standardize_jobs(jobs)
                all_jobs.extend(standardized_jobs)
                
                logger.info(f"Page {page}: Collected {len(jobs)} jobs. Total: {len(all_jobs)}")
                
                time.sleep(delay)
                
                if len(all_jobs) >= max_results:
                    all_jobs = all_jobs[:max_results]
                    logger.info(f"Reached target of {max_results} jobs")
                    break
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error on page {page}: {e}")
                break
            except Exception as e:
                logger.error(f"Unexpected error on page {page}: {e}")
                break
        
        return all_jobs
    
    def _standardize_jobs(self, raw_jobs: List[Dict]) -> List[Dict]:
        """Convert Adzuna API response to standard format"""
        standardized = []
        
        for job in raw_jobs:
            try:
                salary_min = self._safe_float(job.get('salary_min'))
                salary_max = self._safe_float(job.get('salary_max'))
                
                salary_display = self._format_salary_display(salary_min, salary_max)
                post_date = self._parse_post_date(job.get('created'))
                
                std_job = {
                    'source': 'adzuna',
                    'source_id': str(job.get('id', '')),
                    'title': job.get('title', '').strip(),
                    'company': job.get('company', {}).get('display_name', 'Unknown'),
                    'company_standardized': self._standardize_company_name(
                        job.get('company', {}).get('display_name', 'Unknown')
                    ),
                    'location': job.get('location', {}).get('display_name', 'Remote'),
                    'location_standardized': self._standardize_location(
                        job.get('location', {}).get('display_name', 'Remote')
                    ),
                    'description': job.get('description', ''),
                    'url': job.get('redirect_url'),
                    'post_date': post_date,
                    'post_date_str': post_date,
                    'scraped_at': datetime.now().isoformat(),
                    'scraped_at_str': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'salary': salary_display,
                    'salary_min': salary_min,
                    'salary_max': salary_max,
                    'contract_type': job.get('contract_type', 'Full-time'),
                    'job_type': job.get('contract_type', 'Full-time'),
                    'category': job.get('category', {}).get('label', ''),
                    'required_skills': self._extract_skills(job.get('description', '')),
                    'is_remote': 'remote' in str(job.get('location', {})).lower(),
                    'work_arrangement': self._determine_work_arrangement(job)
                }
                
                if salary_min and salary_max:
                    std_job['salary_midpoint'] = (salary_min + salary_max) / 2
                    std_job['salary_range'] = f"${salary_min:,.0f}-${salary_max:,.0f}"
                elif salary_min:
                    std_job['salary_midpoint'] = salary_min
                    std_job['salary_range'] = f"${salary_min:,.0f}+"
                elif salary_max:
                    std_job['salary_midpoint'] = salary_max
                    std_job['salary_range'] = f"Up to ${salary_max:,.0f}"
                
                standardized.append(std_job)
                
            except Exception as e:
                logger.warning(f"Failed to standardize job: {e}")
                continue
        
        return standardized
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """Convert value to float safely"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            if isinstance(value, str):
                numbers = re.findall(r'\d+\.?\d*', value)
                if numbers:
                    try:
                        return float(numbers[0])
                    except (ValueError, TypeError):
                        return None
            return None
    
    def _format_salary_display(self, min_val: Optional[float], 
                              max_val: Optional[float]) -> str:
        """Format salary for display"""
        if min_val and max_val:
            return f"${min_val:,.0f} - ${max_val:,.0f}"
        elif min_val:
            return f"From ${min_val:,.0f}"
        elif max_val:
            return f"Up to ${max_val:,.0f}"
        return "Not specified"
    
    def _parse_post_date(self, date_str: Optional[str]) -> str:
        """Parse post date string"""
        if not date_str:
            return datetime.now().strftime('%Y-%m-%d')
        
        try:
            if 'T' in date_str:
                return date_str.split('T')[0]
            return date_str[:10]
        except Exception:
            return datetime.now().strftime('%Y-%m-%d')
    
    def _standardize_company_name(self, name: str) -> str:
        """Clean up company name"""
        if not name or name.lower() in ['unknown', 'confidential', 'private']:
            return "Unknown"
        
        name_clean = re.sub(
            r'\s+(Inc\.?|LLC|Corp\.?|Corporation|Ltd\.?|Company|Co\.?)$', 
            '', 
            name, 
            flags=re.IGNORECASE
        )
        
        name_clean = re.sub(r'[.,;]+$', '', name_clean)
        
        return name_clean.strip()
    
    def _standardize_location(self, location: str) -> str:
        """Clean up location string"""
        if not location:
            return "Remote"
        
        location_lower = location.lower()
        
        remote_keywords = ['remote', 'anywhere', 'virtual', 'telecommute', 'work from home']
        if any(term in location_lower for term in remote_keywords):
            return "Remote"
        
        match = re.search(r'([A-Za-z\s]+),\s*([A-Z]{2})', location)
        if match:
            city, state = match.groups()
            return f"{city.strip()}, {state}"
        
        return location.strip()
    
    def _extract_skills(self, description: str) -> List[str]:
        """Pull technical skills from description"""
        if not description:
            return []
        
        skills_keywords = {
            'python', 'sql', 'r', 'java', 'scala', 'javascript', 'typescript',
            'c++', 'c#', 'go', 'rust', 'php', 'ruby', 'swift', 'kotlin',
            'tensorflow', 'pytorch', 'scikit-learn', 'keras', 'mxnet',
            'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'terraform',
            'spark', 'hadoop', 'kafka', 'airflow', 'hive', 'presto',
            'tableau', 'powerbi', 'looker', 'snowflake', 'redshift', 'bigquery',
            'pandas', 'numpy', 'matplotlib', 'seaborn', 'plotly', 'd3.js',
            'react', 'angular', 'vue', 'node.js', 'django', 'flask', 'fastapi',
            'mongodb', 'postgresql', 'mysql', 'redis', 'elasticsearch',
            'git', 'jenkins', 'ansible', 'chef', 'puppet', 'circleci'
        }
        
        found_skills = []
        description_lower = description.lower()
        
        for skill in skills_keywords:
            if re.search(r'\b' + re.escape(skill) + r'\b', description_lower):
                found_skills.append(skill)
        
        return found_skills
    
    def _determine_work_arrangement(self, job: Dict) -> str:
        """Figure out work arrangement"""
        location_str = str(job.get('location', {})).lower()
        description = str(job.get('description', '')).lower()
        title = str(job.get('title', '')).lower()
        
        combined = f"{location_str} {description} {title}"
        
        if 'remote' in combined or 'work from home' in combined or 'telecommute' in combined:
            return 'Remote'
        elif 'hybrid' in combined:
            return 'Hybrid'
        return 'On-site'


class USAJobsCollector:
    """Collect job data from USAJOBS API"""
    
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or APIConfig.get_usajobs_config()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': self.config['user_email'],
            'Authorization-Key': self.config['api_key'],
            'Accept': 'application/json',
            'Host': 'data.usajobs.gov'
        })
    
    def fetch_jobs(self, keywords: str, max_results: int = 500) -> List[Dict]:
        """Fetch U.S. government jobs from USAJOBS"""
        all_jobs = []
        page = 1
        results_per_page = 100
        
        try:
            while len(all_jobs) < max_results:
                url = f"{self.config['base_url']}/search"
                params = {
                    'Keyword': keywords,
                    'ResultsPerPage': results_per_page,
                    'Page': page,
                    'DatePosted': 30,
                    'SortField': 'PublicationStartDate',
                    'SortDirection': 'Descending'
                }
                
                logger.info(f"Fetching USAJOBS page {page} for '{keywords}'")
                
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 401:
                    logger.error("USAJOBS authentication failed")
                    break
                
                response.raise_for_status()
                
                data = response.json()
                search_result = data.get('SearchResult', {})
                jobs = search_result.get('SearchResultItems', [])
                
                if not jobs:
                    logger.info("No more USAJOBS results")
                    break
                
                standardized_jobs = self._standardize_jobs(jobs)
                all_jobs.extend(standardized_jobs)
                
                logger.info(f"Page {page}: Collected {len(jobs)} jobs. Total: {len(all_jobs)}")
                
                total_count = search_result.get('SearchResultCountAll', 0)
                if len(all_jobs) >= min(max_results, total_count):
                    logger.info("Reached available results limit")
                    break
                
                page += 1
                time.sleep(0.5)
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching USAJOBS data: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in USAJOBS fetch: {e}")
        
        return all_jobs[:max_results]
    
    def _standardize_jobs(self, raw_jobs: List[Dict]) -> List[Dict]:
        """Standardize USAJOBS format"""
        standardized = []
        
        for item in raw_jobs:
            try:
                matched_object = item.get('MatchedObjectDescriptor', {})
                
                salary_min, salary_max = self._extract_salary(matched_object)
                salary_display = self._format_salary_display(salary_min, salary_max)
                
                std_job = {
                    'source': 'usajobs',
                    'source_id': matched_object.get('PositionID', ''),
                    'title': matched_object.get('PositionTitle', ''),
                    'company': matched_object.get('OrganizationName', 'U.S. Government'),
                    'company_standardized': self._standardize_company_name(
                        matched_object.get('OrganizationName', 'U.S. Government')
                    ),
                    'location': self._extract_location(matched_object),
                    'location_standardized': self._standardize_location(
                        self._extract_location(matched_object)
                    ),
                    'description': self._extract_description(matched_object),
                    'url': matched_object.get('PositionURI'),
                    'post_date': matched_object.get('PublicationStartDate', ''),
                    'post_date_str': matched_object.get('PublicationStartDate', ''),
                    'scraped_at': datetime.now().isoformat(),
                    'scraped_at_str': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'salary': salary_display,
                    'salary_min': salary_min,
                    'salary_max': salary_max,
                    'job_type': self._extract_job_type(matched_object),
                    'work_arrangement': self._determine_work_arrangement(matched_object),
                    'required_skills': self._extract_skills(matched_object),
                    'is_remote': self._is_remote(matched_object),
                    'job_grade': self._extract_job_grade(matched_object),
                    'clearance_required': self._check_clearance(matched_object),
                    'employment_type': self._extract_employment_type(matched_object)
                }
                
                if salary_min and salary_max:
                    std_job['salary_midpoint'] = (salary_min + salary_max) / 2
                    std_job['salary_range'] = f"${salary_min:,.0f}-${salary_max:,.0f}"
                elif salary_min:
                    std_job['salary_midpoint'] = salary_min
                    std_job['salary_range'] = f"${salary_min:,.0f}+"
                elif salary_max:
                    std_job['salary_midpoint'] = salary_max
                    std_job['salary_range'] = f"Up to ${salary_max:,.0f}"
                
                standardized.append(std_job)
                
            except Exception as e:
                logger.warning(f"Failed to standardize USAJOBS entry: {e}")
                continue
        
        return standardized
    
    def _extract_salary(self, job_data: Dict) -> tuple:
        """Get salary information"""
        salary_data = job_data.get('PositionRemuneration', [{}])
        
        if salary_data:
            min_salary = self._safe_float(salary_data[0].get('MinimumRange'))
            max_salary = self._safe_float(salary_data[0].get('MaximumRange'))
            return min_salary, max_salary
        
        return None, None
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """Convert to float safely"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _format_salary_display(self, min_val: Optional[float], 
                              max_val: Optional[float]) -> str:
        """Format salary for display"""
        if min_val and max_val:
            return f"${min_val:,.0f} - ${max_val:,.0f}"
        elif min_val:
            return f"From ${min_val:,.0f}"
        elif max_val:
            return f"Up to ${max_val:,.0f}"
        return "Not specified"
    
    def _extract_location(self, job_data: Dict) -> str:
        """Pull location from job data"""
        locations = job_data.get('PositionLocation', [])
        if locations:
            location_name = locations[0].get('LocationName', 'Multiple Locations')
            match = re.search(r'([A-Za-z\s]+),\s*([A-Z]{2})', location_name)
            if match:
                return location_name
            return location_name
        return 'Multiple Locations'
    
    def _extract_description(self, job_data: Dict) -> str:
        """Combine description fields"""
        description_parts = []
        
        qualification = job_data.get('QualificationSummary', '')
        if qualification:
            description_parts.append(f"Qualifications: {qualification}")
        
        major_duties = job_data.get('MajorDuties', [])
        if major_duties:
            description_parts.append("Major Duties:")
            description_parts.extend([f"- {duty}" for duty in major_duties])
        
        user_area = job_data.get('UserArea', {})
        details = user_area.get('Details', {})
        duties = details.get('Duties', '')
        if duties:
            description_parts.append(f"Additional Duties: {duties}")
        
        job_summary = user_area.get('Details', {}).get('JobSummary', '')
        if job_summary:
            description_parts.append(f"Summary: {job_summary}")
        
        return "\n\n".join(description_parts) if description_parts else ""
    
    def _extract_job_type(self, job_data: Dict) -> str:
        """Get job type/schedule"""
        schedule = job_data.get('PositionSchedule', [{}])
        if schedule:
            return schedule[0].get('Name', 'Full-time')
        return 'Full-time'
    
    def _determine_work_arrangement(self, job_data: Dict) -> str:
        """Determine work arrangement"""
        if self._is_remote(job_data):
            return 'Remote'
        
        description = self._extract_description(job_data).lower()
        if 'telework' in description or 'remote' in description:
            return 'Remote'
        elif 'hybrid' in description:
            return 'Hybrid'
        
        return 'On-site'
    
    def _extract_skills(self, job_data: Dict) -> List[str]:
        """Extract skills from description"""
        description = self._extract_description(job_data)
        
        if not description:
            return []
        
        skills_keywords = {
            'python', 'sql', 'r', 'java', 'scala', 'sas', 'stata',
            'tensorflow', 'pytorch', 'scikit-learn', 'keras',
            'aws', 'azure', 'gcp', 'docker', 'kubernetes',
            'spark', 'hadoop', 'kafka', 'airflow', 'hive',
            'tableau', 'powerbi', 'looker', 'snowflake', 'qlik',
            'pandas', 'numpy', 'matplotlib', 'seaborn',
            'security', 'clearance', 'government', 'federal',
            'compliance', 'regulatory', 'policy', 'analysis',
            'project management', 'agile', 'scrum', 'devops',
            'excel', 'access', 'powerpoint', 'word', 'outlook'
        }
        
        found_skills = []
        description_lower = description.lower()
        
        for skill in skills_keywords:
            if re.search(r'\b' + re.escape(skill) + r'\b', description_lower):
                found_skills.append(skill)
        
        return found_skills
    
    def _is_remote(self, job_data: Dict) -> bool:
        """Check if job is remote"""
        locations = job_data.get('PositionLocation', [])
        for location in locations:
            location_name = location.get('LocationName', '').lower()
            if 'remote' in location_name or 'telework' in location_name:
                return True
        
        user_area = job_data.get('UserArea', {})
        details = user_area.get('Details', {})
        telework = details.get('TeleworkEligible', False)
        
        return bool(telework)
    
    def _extract_job_grade(self, job_data: Dict) -> Optional[str]:
        """Get job grade"""
        job_grade = job_data.get('JobGrade', [{}])
        if job_grade:
            return job_grade[0].get('Code')
        return None
    
    def _check_clearance(self, job_data: Dict) -> bool:
        """Check for security clearance requirement"""
        user_area = job_data.get('UserArea', {})
        details = user_area.get('Details', {})
        clearance = details.get('SecurityClearanceRequired', False)
        
        description = self._extract_description(job_data).lower()
        if 'clearance' in description or 'security clearance' in description:
            return True
        
        return bool(clearance)
    
    def _extract_employment_type(self, job_data: Dict) -> str:
        """Get employment type"""
        return self._extract_job_type(job_data)
    
    def _standardize_company_name(self, name: str) -> str:
        """Clean up company name"""
        if not name or name.lower() in ['u.s. government', 'federal government']:
            return "U.S. Government"
        
        name_clean = re.sub(
            r'\s+(Agency|Department|Administration|Service|Bureau|Office|Commission)$', 
            '', 
            name, 
            flags=re.IGNORECASE
        )
        
        name_clean = re.sub(r'\bU\.S\.\b', 'US', name_clean)
        name_clean = re.sub(r'\bDept\.\b', 'Department', name_clean)
        
        return name_clean.strip()
    
    def _standardize_location(self, location: str) -> str:
        """Clean up location string"""
        if not location:
            return "Multiple Locations"
        
        location_lower = location.lower()
        
        if 'multiple' in location_lower or 'various' in location_lower:
            return "Multiple Locations"
        elif 'remote' in location_lower or 'telework' in location_lower:
            return "Remote"
        elif 'nationwide' in location_lower:
            return "Nationwide"
        
        return location.strip()


class DataExporter:
    """Handle data export to different formats"""
    
    def __init__(self, output_dir: str = 'exports'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def export_csv(self, jobs: List[Dict], filename: Optional[str] = None) -> str:
        """Export jobs to CSV"""
        if not jobs:
            logger.warning("No jobs to export to CSV")
            return ""
        
        try:
            df = pd.DataFrame(jobs)
            
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"job_export_{timestamp}.csv"
            
            filepath = self.output_dir / filename
            
            df_clean = self._clean_dataframe_for_csv(df)
            df_clean.to_csv(filepath, index=False, encoding='utf-8-sig')
            
            logger.info(f"Exported {len(df_clean)} jobs to CSV: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"CSV export failed: {e}")
            return ""
    
    def export_json(self, jobs: List[Dict], filename: Optional[str] = None) -> str:
        """Export jobs to JSON"""
        if not jobs:
            logger.warning("No jobs to export to JSON")
            return ""
        
        try:
            if not filename:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"job_export_{timestamp}.json"
            
            filepath = self.output_dir / filename
            
            export_data = {
                'metadata': {
                    'export_date': datetime.now().isoformat(),
                    'total_jobs': len(jobs),
                    'sources': list(set(job.get('source', 'unknown') for job in jobs)),
                    'keywords_used': os.getenv('JOB_KEYWORDS', 'data scientist')
                },
                'jobs': jobs
            }
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, default=str, ensure_ascii=False)
            
            logger.info(f"Exported {len(jobs)} jobs to JSON: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"JSON export failed: {e}")
            return ""
    
    def _clean_dataframe_for_csv(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean DataFrame for CSV export"""
        df_clean = df.copy()
        
        for column in df_clean.columns:
            if df_clean[column].apply(lambda x: isinstance(x, list)).any():
                df_clean[column] = df_clean[column].apply(
                    lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x
                )
        
        for column in df_clean.columns:
            if pd.api.types.is_datetime64_any_dtype(df_clean[column]):
                df_clean[column] = df_clean[column].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        df_clean = df_clean.fillna('')
        
        return df_clean


class DatabaseManager:
    """Handle database operations for job data"""
    
    def __init__(self, db_path: str = 'job_data.db'):
        self.db_path = Path(db_path)
        self._initialize_database()
    
    def _initialize_database(self) -> None:
        """Set up database tables"""
        try:
            connection = sqlite3.connect(self.db_path)
            cursor = connection.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    source_id TEXT,
                    title TEXT NOT NULL,
                    company TEXT,
                    company_standardized TEXT,
                    location TEXT,
                    location_standardized TEXT,
                    description TEXT,
                    salary_min REAL,
                    salary_max REAL,
                    salary_avg REAL,
                    salary_range TEXT,
                    is_remote INTEGER,
                    work_arrangement TEXT,
                    job_type TEXT,
                    post_date TEXT,
                    scraped_date TEXT,
                    url TEXT,
                    required_skills TEXT,
                    category TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source, source_id)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS skills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id INTEGER,
                    skill TEXT,
                    skill_category TEXT,
                    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE,
                    UNIQUE(job_id, skill)
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS company_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    company_name TEXT UNIQUE,
                    job_count INTEGER DEFAULT 0,
                    avg_salary REAL,
                    remote_jobs INTEGER DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_jobs_source ON jobs(source)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company_standardized)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_jobs_post_date ON jobs(post_date)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_jobs_salary ON jobs(salary_avg)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_skills_skill ON skills(skill)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_jobs_remote ON jobs(is_remote)')
            
            connection.commit()
            connection.close()
            
            logger.info(f"Database initialized: {self.db_path}")
            
        except Exception as e:
            logger.error(f"Database initialization failed: {e}")
            raise
    
    def store_jobs(self, jobs: List[Dict]) -> int:
        """Save jobs to database"""
        if not jobs:
            logger.warning("No jobs to store in database")
            return 0
        
        try:
            connection = sqlite3.connect(self.db_path)
            cursor = connection.cursor()
            
            stored_count = 0
            skill_inserts = []
            
            for job in jobs:
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO jobs 
                        (source, source_id, title, company, company_standardized, 
                         location, location_standardized, description,
                         salary_min, salary_max, salary_avg, salary_range, 
                         is_remote, work_arrangement, job_type, 
                         post_date, scraped_date, url, required_skills, category)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        job.get('source'),
                        job.get('source_id'),
                        job.get('title'),
                        job.get('company'),
                        job.get('company_standardized'),
                        job.get('location'),
                        job.get('location_standardized'),
                        job.get('description')[:10000],
                        job.get('salary_min'),
                        job.get('salary_max'),
                        job.get('salary_midpoint'),
                        job.get('salary_range'),
                        1 if job.get('is_remote') else 0,
                        job.get('work_arrangement'),
                        job.get('job_type'),
                        job.get('post_date'),
                        job.get('scraped_at_str'),
                        job.get('url'),
                        ', '.join(job.get('required_skills', [])),
                        job.get('category', '')
                    ))
                    
                    if cursor.rowcount > 0:
                        job_id = cursor.lastrowid
                        
                        skills = job.get('required_skills', [])
                        for skill in skills:
                            if skill and isinstance(skill, str):
                                skill_lower = skill.lower().strip()
                                category = self._categorize_skill(skill_lower)
                                skill_inserts.append((job_id, skill_lower, category))
                        
                        stored_count += 1
                    
                except sqlite3.IntegrityError as e:
                    logger.debug(f"Duplicate job skipped: {job.get('source_id')}")
                    continue
                except Exception as e:
                    logger.warning(f"Failed to store job: {e}")
                    continue
            
            if skill_inserts:
                cursor.executemany(
                    'INSERT OR IGNORE INTO skills (job_id, skill, skill_category) VALUES (?, ?, ?)',
                    skill_inserts
                )
            
            connection.commit()
            connection.close()
            
            logger.info(f"Stored {stored_count} jobs in database")
            return stored_count
            
        except Exception as e:
            logger.error(f"Database storage failed: {e}")
            return 0
    
    def _categorize_skill(self, skill: str) -> str:
        """Categorize skill into broad groups"""
        skill_categories = {
            'programming': ['python', 'java', 'javascript', 'c++', 'c#', 'r', 'sql', 'scala', 'go', 'rust'],
            'ml_ai': ['tensorflow', 'pytorch', 'scikit-learn', 'keras', 'machine learning', 'deep learning'],
            'cloud': ['aws', 'azure', 'gcp', 'docker', 'kubernetes', 'terraform'],
            'data_tools': ['spark', 'hadoop', 'kafka', 'airflow', 'hive'],
            'bi': ['tableau', 'powerbi', 'looker', 'qlik'],
            'databases': ['mysql', 'postgresql', 'mongodb', 'redis', 'snowflake'],
            'web': ['react', 'angular', 'vue', 'django', 'flask', 'node.js'],
            'stats': ['pandas', 'numpy', 'matplotlib', 'seaborn', 'statistics'],
            'devops': ['git', 'jenkins', 'ansible', 'ci/cd', 'linux'],
            'soft_skills': ['communication', 'leadership', 'problem solving', 'teamwork']
        }
        
        for category, keywords in skill_categories.items():
            if any(keyword in skill for keyword in keywords):
                return category
        
        return 'other'


class DataProcessor:
    """Process and standardize job data"""
    
    def process(self, jobs: List[Dict]) -> List[Dict]:
        """Process and add derived fields to job data"""
        processed_jobs = []
        
        for job in jobs:
            try:
                processed_job = job.copy()
                
                description = str(job.get('description', ''))
                processed_job['description_length'] = len(description)
                processed_job['title_word_count'] = len(str(job.get('title', '')).split())
                
                processed_job['seniority'] = self._infer_seniority(job.get('title', ''))
                processed_job['category'] = self._infer_category(job)
                processed_job['salary_score'] = self._calculate_salary_score(job)
                processed_job['experience_years'] = self._extract_experience(description)
                processed_job['processed_at'] = datetime.now().isoformat()
                
                processed_jobs.append(processed_job)
                
            except Exception as e:
                logger.warning(f"Failed to process job: {e}")
                continue
        
        return processed_jobs
    
    def _infer_seniority(self, title: str) -> str:
        """Guess seniority level from job title"""
        if not title:
            return "Not Specified"
        
        title_lower = title.lower()
        
        seniority_keywords = {
            'entry': ['entry', 'junior', 'jr.', 'associate', 'graduate', 'trainee', 'i ', 'level i'],
            'mid': ['mid', 'intermediate', 'ii', 'level ii', 'experienced'],
            'senior': ['senior', 'sr.', 'lead', 'principal', 'staff', 'iii', 'iv', 'v', 'level iii'],
            'manager': ['manager', 'mgr', 'supervisor', 'team lead'],
            'director': ['director', 'head of', 'vp', 'vice president'],
            'executive': ['chief', 'cfo', 'cto', 'ceo', 'president', 'executive']
        }
        
        for level, keywords in seniority_keywords.items():
            for keyword in keywords:
                if keyword in title_lower:
                    return level.capitalize()
        
        return "Not Specified"
    
    def _infer_category(self, job: Dict) -> str:
        """Guess category/industry from job data"""
        source = job.get('source', '')
        
        if source == 'usajobs':
            return 'Government'
        
        title = str(job.get('title', '')).lower()
        description = str(job.get('description', '')).lower()
        combined = f"{title} {description}"
        
        categories = {
            'data_science': ['data scientist', 'machine learning', 'ai ', 'ml engineer'],
            'data_analyst': ['data analyst', 'business analyst', 'analytics'],
            'data_engineer': ['data engineer', 'etl', 'data pipeline'],
            'software': ['software', 'developer', 'engineer', 'programmer'],
            'cloud': ['cloud', 'devops', 'sre', 'site reliability'],
            'bi': ['business intelligence', 'tableau', 'powerbi', 'bi '],
            'research': ['research', 'scientist', 'phd', 'research scientist'],
            'management': ['manager', 'director', 'head of', 'lead ']
        }
        
        for category, keywords in categories.items():
            for keyword in keywords:
                if keyword in combined:
                    return category.replace('_', ' ').title()
        
        return 'Technology' if source == 'adzuna' else 'General'
    
    def _calculate_salary_score(self, job: Dict) -> float:
        """Calculate normalized salary score"""
        salary_min = job.get('salary_min')
        salary_max = job.get('salary_max')
        
        if salary_min and salary_max:
            avg_salary = (salary_min + salary_max) / 2
            normalized = min(avg_salary / 200000, 1.0)
            return round(normalized, 2)
        elif salary_min:
            return round(min(salary_min / 200000, 1.0), 2)
        
        return 0.0
    
    def _extract_experience(self, description: str) -> Optional[int]:
        """Pull required years of experience from description"""
        patterns = [
            r'(\d+)[+]*\s*years?',
            r'(\d+)[+]*\s*yrs?',
            r'(\d+)[+]*\s*yr',
            r'experience.*?(\d+).*?years'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, description.lower())
            if matches:
                try:
                    years = max(int(match) for match in matches if match.isdigit())
                    return years
                except (ValueError, TypeError):
                    continue
        
        return None


class JobDataPipeline:
    """Main pipeline for collecting and processing job data"""
    
    def __init__(self, keywords: str, max_results: int = 1000):
        self.keywords = keywords
        self.max_results = max_results
        
        self._validate_configuration()
        
        self.adzuna_collector = AdzunaCollector()
        self.usajobs_collector = USAJobsCollector()
        self.data_processor = DataProcessor()
        self.database_manager = DatabaseManager()
        self.exporter = DataExporter()
    
    def _validate_configuration(self) -> None:
        """Check API configuration before starting"""
        try:
            APIConfig.get_adzuna_config()
            logger.info("Adzuna API configuration validated")
            
            APIConfig.get_usajobs_config()
            logger.info("USAJobs API configuration validated")
            
        except ConfigError as e:
            logger.error(f"Configuration error: {e}")
            print(f"\nConfiguration Error: {e}")
            print("\nSet up your environment variables:")
            print("=" * 50)
            print("1. Create a .env file in your project directory")
            print("2. Add the following variables:")
            print("\n   # Adzuna API")
            print("   ADZUNA_APP_ID=your_app_id_here")
            print("   ADZUNA_APP_KEY=your_app_key_here")
            print("\n   # USAJobs API")
            print("   USAJOBS_API_KEY=your_api_key_here")
            print("   USAJOBS_USER_EMAIL=your_email@example.com")
            print("\n3. Save the file and run again")
            print("=" * 50)
            sys.exit(1)
    
    def run(self) -> Dict:
        """Run the complete data pipeline"""
        logger.info("=" * 60)
        logger.info(f"Starting Job Data Pipeline for: {self.keywords}")
        logger.info("=" * 60)
        
        results = {
            'total_jobs': 0,
            'adzuna_jobs': 0,
            'usajobs_jobs': 0,
            'stored_jobs': 0,
            'export_files': [],
            'pipeline_start': datetime.now().isoformat(),
            'keywords': self.keywords
        }
        
        try:
            all_jobs = []
            
            logger.info("Collecting data from Adzuna...")
            try:
                adzuna_jobs = self.adzuna_collector.fetch_jobs(
                    self.keywords, 
                    max_results=self.max_results // 2
                )
                all_jobs.extend(adzuna_jobs)
                results['adzuna_jobs'] = len(adzuna_jobs)
                logger.info(f"Adzuna: Collected {len(adzuna_jobs)} jobs")
            except Exception as e:
                logger.error(f"Failed to collect from Adzuna: {e}")
            
            logger.info("Collecting data from USAJobs...")
            try:
                usajobs_jobs = self.usajobs_collector.fetch_jobs(
                    self.keywords,
                    max_results=self.max_results // 2
                )
                all_jobs.extend(usajobs_jobs)
                results['usajobs_jobs'] = len(usajobs_jobs)
                logger.info(f"USAJobs: Collected {len(usajobs_jobs)} jobs")
            except Exception as e:
                logger.error(f"Failed to collect from USAJobs: {e}")
            
            results['total_jobs'] = len(all_jobs)
            
            if not all_jobs:
                logger.warning("No jobs collected from any source")
                self._print_summary(results)
                return results
            
            logger.info(f"Collected {len(all_jobs)} total jobs")
            
            logger.info("Processing collected data...")
            processed_jobs = self.data_processor.process(all_jobs)
            logger.info(f"Processed {len(processed_jobs)} jobs")
            
            logger.info("Storing data in database...")
            stored_count = self.database_manager.store_jobs(processed_jobs)
            results['stored_jobs'] = stored_count
            
            logger.info("Exporting data...")
            
            csv_file = self.exporter.export_csv(processed_jobs)
            if csv_file:
                results['export_files'].append(('csv', csv_file))
            
            json_file = self.exporter.export_json(processed_jobs)
            if json_file:
                results['export_files'].append(('json', json_file))
            
            self._generate_summary(processed_jobs, results)
            
            results['pipeline_end'] = datetime.now().isoformat()
            results['duration_seconds'] = (
                datetime.fromisoformat(results['pipeline_end']) - 
                datetime.fromisoformat(results['pipeline_start'])
            ).total_seconds()
            
            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            results['error'] = str(e)
        
        self._print_summary(results)
        return results
    
    def _generate_summary(self, jobs: List[Dict], results: Dict) -> None:
        """Generate summary statistics"""
        if not jobs:
            return
        
        df = pd.DataFrame(jobs)
        
        source_counts = df['source'].value_counts()
        results['source_distribution'] = source_counts.to_dict()
        
        if 'is_remote' in df.columns:
            remote_count = df['is_remote'].sum()
            results['remote_jobs'] = int(remote_count)
            results['remote_percentage'] = round((remote_count / len(df)) * 100, 1)
        
        if 'salary_midpoint' in df.columns:
            salary_df = df[df['salary_midpoint'].notna() & (df['salary_midpoint'] > 0)]
            if not salary_df.empty:
                results['avg_salary'] = round(salary_df['salary_midpoint'].mean(), 2)
                results['median_salary'] = round(salary_df['salary_midpoint'].median(), 2)
                results['salary_jobs_count'] = len(salary_df)
        
        if 'seniority' in df.columns:
            seniority_counts = df['seniority'].value_counts()
            results['seniority_distribution'] = seniority_counts.to_dict()
        
        if 'company_standardized' in df.columns:
            top_companies = df['company_standardized'].value_counts().head(10)
            results['top_companies'] = top_companies.to_dict()
    
    def _print_summary(self, results: Dict) -> None:
        """Print summary to console"""
        print("\n" + "=" * 60)
        print("JOB DATA PIPELINE - SUMMARY REPORT")
        print("=" * 60)
        
        print(f"\nCollection Results:")
        print(f"   Total jobs collected: {results.get('total_jobs', 0)}")
        print(f"   - Adzuna: {results.get('adzuna_jobs', 0)} jobs")
        print(f"   - USAJobs: {results.get('usajobs_jobs', 0)} jobs")
        
        if results.get('stored_jobs', 0) > 0:
            print(f"\nStorage Results:")
            print(f"   Jobs stored in database: {results.get('stored_jobs', 0)}")
        
        if results.get('export_files'):
            print(f"\nExport Results:")
            for file_type, file_path in results.get('export_files', []):
                print(f"   {file_type.upper()}: {file_path}")
        
        if 'source_distribution' in results:
            print(f"\nSource Distribution:")
            for source, count in results['source_distribution'].items():
                percentage = (count / results['total_jobs']) * 100
                print(f"   {source.title()}: {count} ({percentage:.1f}%)")
        
        if 'remote_jobs' in results:
            print(f"\nRemote Work:")
            print(f"   Remote positions: {results['remote_jobs']} ({results.get('remote_percentage', 0)}%)")
        
        if 'avg_salary' in results:
            print(f"\nSalary Analysis:")
            print(f"   Average salary: ${results['avg_salary']:,.0f}")
            print(f"   Median salary: ${results['median_salary']:,.0f}")
            print(f"   Jobs with salary info: {results.get('salary_jobs_count', 0)}")
        
        if 'seniority_distribution' in results:
            print(f"\nSeniority Levels:")
            for level, count in results['seniority_distribution'].items():
                print(f"   {level}: {count}")
        
        if 'error' in results:
            print(f"\nPipeline Error: {results['error']}")
        
        if 'pipeline_end' in results:
            duration = results.get('duration_seconds', 0)
            print(f"\nPipeline Duration: {duration:.1f} seconds")
        
        print("\n" + "=" * 60)


def create_env_template() -> None:
    """Create a .env template file if it doesn't exist"""
    env_file = Path('.env')
    if not env_file.exists():
        template = """# Job Data Pipeline - Environment Variables

# Adzuna API Configuration
ADZUNA_APP_ID=your_app_id_here
ADZUNA_APP_KEY=your_app_key_here

# USAJobs API Configuration
USAJOBS_API_KEY=your_api_key_here
USAJOBS_USER_EMAIL=your_email@example.com

# Optional: Default search keywords
JOB_KEYWORDS=data scientist
"""
        with open(env_file, 'w') as f:
            f.write(template)
        print("Created .env template file. Edit it with your API keys.")
        return True
    return False


def main():
    """Main execution function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Job Market Data Collection Pipeline',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python collect_jobs.py "data scientist"
  python collect_jobs.py "machine learning engineer" --max-results 2000
  python collect_jobs.py "software developer" --log-level DEBUG
  python collect_jobs.py --setup

Get API Keys:
  Adzuna: https://developer.adzuna.com/
  USAJobs: https://developer.usajobs.gov/
        """
    )
    
    parser.add_argument(
        'keywords',
        nargs='?',
        default=os.getenv('JOB_KEYWORDS', 'data scientist'),
        help='Job search keywords'
    )
    
    parser.add_argument(
        '--max-results',
        type=int,
        default=1000,
        help='Maximum results to collect (default: 1000)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Set logging level (default: INFO)'
    )
    
    parser.add_argument(
        '--setup',
        action='store_true',
        help='Create a .env template file for API keys'
    )
    
    parser.add_argument(
        '--validate',
        action='store_true',
        help='Validate API configuration without running pipeline'
    )
    
    args = parser.parse_args()
    
    if args.setup:
        if create_env_template():
            print("\nEdit the .env file with your API keys and run again.")
        sys.exit(0)
    
    if args.validate:
        print("Validating API configuration...")
        try:
            APIConfig.get_adzuna_config()
            print("Adzuna API: Configuration valid")
        except ConfigError as e:
            print(f"Adzuna API: {e}")
        
        try:
            APIConfig.get_usajobs_config()
            print("USAJobs API: Configuration valid")
        except ConfigError as e:
            print(f"USAJobs API: {e}")
        
        print("\nTip: Use --setup to create a .env template")
        sys.exit(0)
    
    logger.setLevel(getattr(logging, args.log_level))
    
    pipeline = JobDataPipeline(args.keywords, args.max_results)
    results = pipeline.run()
    
    if results.get('total_jobs', 0) > 0:
        print(f"\nPipeline completed successfully.")
        print(f"Collected {results['total_jobs']} jobs.")
        if results.get('export_files'):
            print(f"Exported data to {len(results['export_files'])} files.")
        sys.exit(0)
    else:
        print("\nPipeline completed with no data collected.")
        print("Check your API keys and network connection.")
        sys.exit(1)


if __name__ == "__main__":
    main()