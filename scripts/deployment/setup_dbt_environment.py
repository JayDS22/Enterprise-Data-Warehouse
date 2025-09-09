#!/usr/bin/env python3
"""
DBT Environment Setup Script

This script sets up the complete DBT environment including:
- Profile configuration
- Package installation
- Model generation
- Initial data seeding
- Documentation generation

Usage:
    python setup_dbt_environment.py --env [dev|test|prod] [--skip-generation]
"""

import os
import sys
import argparse
import logging
import subprocess
import shutil
from pathlib import Path
from typing import Dict, List, Optional
import yaml
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DBTEnvironmentSetup:
    """Handles complete DBT environment setup"""
    
    def __init__(self, environment: str = 'dev'):
        self.environment = environment
        self.dbt_profiles_dir = Path.home() / '.dbt'
        self.project_root = Path.cwd()
        
    def validate_prerequisites(self) -> None:
        """Validate that prerequisites are installed"""
        logger.info("Validating prerequisites...")
        
        # Check if DBT is installed
        try:
            result = subprocess.run(['dbt', '--version'], 
                                  capture_output=True, text=True, check=True)
            logger.info(f"DBT version: {result.stdout.strip()}")
        except (subprocess.CalledProcessError, FileNotFoundError):
            logger.error("DBT is not installed. Please install with: pip install dbt-core dbt-snowflake")
            sys.exit(1)
        
        # Check if Python requirements are satisfied
        requirements_file = self.project_root / 'requirements.txt'
        if requirements_file.exists():
            try:
                subprocess.run(['pip', 'check'], check=True, capture_output=True)
                logger.info("Python requirements validated")
            except subprocess.CalledProcessError:
                logger.warning("Some Python requirements may be missing. Run: pip install -r requirements.txt")
        
        # Validate environment variables for the target environment
        if self.environment != 'dev' or not self._is_dry_run():
            required_vars = [
                'SNOWFLAKE_ACCOUNT',
                'SNOWFLAKE_USER', 
                'SNOWFLAKE_PASSWORD'
            ]
            
            missing_vars = [var for var in required_vars if not os.getenv(var)]
            if missing_vars:
                logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
                logger.info("Please set these variables in your .env file or environment")
                sys.exit(1)
    
    def _is_dry_run(self) -> bool:
        """Check if this is a dry run (no actual connections needed)"""
        return '--dry-run' in sys.argv
    
    def setup_dbt_profiles(self) -> None:
        """Setup DBT profiles.yml file"""
        logger.info("Setting up DBT profiles...")
        
        # Create .dbt directory if it doesn't exist
        self.dbt_profiles_dir.mkdir(exist_ok=True)
        
        # Check if profiles.yml already exists in project root
        project_profiles = self.project_root / 'profiles.yml'
        target_profiles = self.dbt_profiles_dir / 'profiles.yml'
        
        if project_profiles.exists():
            # Copy from project to .dbt directory
            shutil.copy2(project_profiles, target_profiles)
            logger.info(f"Copied profiles.yml to {target_profiles}")
        else:
            # Generate default profiles.yml
            self._generate_default_profiles(target_profiles)
        
        # Validate the profiles configuration
        try:
            result = subprocess.run(['dbt', 'debug'], 
                                  capture_output=True, text=True, check=True)
            logger.info("DBT profiles validation successful")
        except subprocess.CalledProcessError as e:
            logger.error(f"DBT profiles validation failed: {e.stderr}")
            logger.info("Please check your profiles.yml configuration and environment variables")
            sys.exit(1)
    
    def _generate_default_profiles(self, target_path: Path) -> None:
        """Generate a default profiles.yml file"""
        logger.info("Generating default profiles.yml...")
        
        # Get database names based on environment
        env_config = {
            'dev': {
                'database': os.getenv('SNOWFLAKE_DATABASE', 'DEV_DW'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'DEV_WH'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
            },
            'test': {
                'database': os.getenv('SNOWFLAKE_DATABASE', 'TEST_DW'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'TEST_WH'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
            },
            'prod': {
                'database': os.getenv('SNOWFLAKE_DATABASE', 'PROD_DW'),
                'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'PROD_WH'),
                'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')
            }
        }
        
        profiles_content = {
            'enterprise_dw': {
                'target': self.environment,
                'outputs': {
                    env: {
                        'type': 'snowflake',
                        'account': "{{ env_var('SNOWFLAKE_ACCOUNT') }}",
                        'user': "{{ env_var('SNOWFLAKE_USER') }}",
                        'password': "{{ env_var('SNOWFLAKE_PASSWORD') }}",
                        'role': "{{ env_var('SNOWFLAKE_ROLE') | default('TRANSFORMER', true) }}",
                        'database': config['database'],
                        'warehouse': config['warehouse'],
                        'schema': config['schema'],
                        'threads': 8 if env == 'prod' else 4,
                        'client_session_keep_alive': True,
                        'query_tag': f"dbt_{env}"
                    }
                    for env, config in env_config.items()
                }
            }
        }
        
        with open(target_path, 'w') as f:
            yaml.dump(profiles_content, f, default_flow_style=False, sort_keys=False)
        
        logger.info(f"Generated default profiles.yml at {target_path}")
    
    def install_dbt_packages(self) -> None:
        """Install DBT packages defined in packages.yml"""
        logger.info("Installing DBT packages...")
        
        packages_file = self.project_root / 'packages.yml'
        
        if not packages_file.exists():
            self._create_packages_file(packages_file)
        
        try:
            result = subprocess.run(['dbt', 'deps'], 
                                  cwd=self.project_root,
                                  capture_output=True, text=True, check=True)
            logger.info("DBT packages installed successfully")
            logger.debug(result.stdout)
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to install DBT packages: {e.stderr}")
            sys.exit(1)
    
    def _create_packages_file(self, packages_path: Path) -> None:
        """Create packages.yml file with common dependencies"""
        logger.info("Creating packages.yml with common dependencies...")
        
        packages_content = {
            'packages': [
                {
                    'package': 'dbt-labs/dbt_utils',
                    'version': '1.1.1'
                },
                {
                    'package': 'calogica/dbt_expectations',
                    'version': '0.10.1'
                },
                {
                    'package': 'dbt-labs/audit_helper',
                    'version': '0.9.0'
                },
                {
                    'package': 'dbt-labs/codegen',
                    'version': '0.12.1'
                },
                {
                    'package': 'brooklyn-data/dbt_artifacts',
                    'version': '2.6.1'
                }
            ]
        }
        
        with open(packages_path, 'w') as f:
            yaml.dump(packages_content, f, default_flow_style=False)
        
        logger.info(f"Created packages.yml at {packages_path}")
    
    def generate_models(self, skip_generation: bool = False) -> None:
        """Generate fact and dimension models"""
        if skip_generation:
            logger.info("Skipping model generation")
            return
            
        logger.info("Generating fact and dimension models...")
        
        generator_script = self.project_root / 'scripts' / 'utilities' / 'generate_fact_dimension_models.py'
        
        if generator_script.exists():
            try:
                subprocess.run([sys.executable, str(generator_script)], 
                             cwd=self.project_root, check=True)
                logger.info("Models generated successfully")
            except subprocess.CalledProcessError as e:
                logger.error(f"Model generation failed: {e}")
                logger.warning("Continuing with existing models...")
        else:
            logger.warning(f"Model generator script not found at {generator_script}")
            logger.info("Please ensure the generator script exists or skip generation")
    
    def create_directory_structure(self) -> None:
        """Create necessary directory structure for DBT"""
        logger.info("Creating directory structure...")
        
        directories = [
            'models/staging',
            'models/intermediate', 
            'models/marts/facts',
            'models/marts/dimensions',
            'tests/generic',
            'tests/singular',
            'macros',
            'snapshots',
            'analysis',
            'seeds/lookup_tables',
            'logs',
            'target',
            'governance',
            'documentation'
        ]
        
        for directory in directories:
            dir_path = self.project_root / directory
            dir_path.mkdir(parents=True, exist_ok=True)
            
            # Create .gitkeep files for empty directories
            gitkeep_file = dir_path / '.gitkeep'
            if not any(dir_path.iterdir()) and not gitkeep_file.exists():
                gitkeep_file.touch()
        
        logger.info("Directory structure created")
    
    def setup_git_ignores(self) -> None:
        """Setup .gitignore file for DBT project"""
        logger.info("Setting up .gitignore...")
        
        gitignore_path = self.project_root / '.gitignore'
        
        gitignore_content = """
# DBT
target/
dbt_packages/
logs/
.dbt/

# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg

# Environment variables
.env
.env.local
.env.*.local

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db

# Logs
*.log

# Data files (exclude sample data)
*.csv
*.xlsx
*.json
!seeds/**/*.csv
!seeds/**/*.json

# Credentials
credentials.json
service_account.json
profiles.yml.local
"""
        
        if not gitignore_path.exists():
            with open(gitignore_path, 'w') as f:
                f.write(gitignore_content.strip())
            logger.info("Created .gitignore file")
        else:
            logger.info(".gitignore already exists")
    
    def create_sample_seed_data(self) -> None:
        """Create sample seed data files"""
        logger.info("Creating sample seed data...")
        
        # Country codes lookup
        country_codes_path = self.project_root / 'seeds' / 'lookup_tables' / 'country_codes.csv'
        if not country_codes_path.exists():
            country_data = """country_code,country_name,region
USA,United States,North America
CAN,Canada,North America
GBR,United Kingdom,Europe
DEU,Germany,Europe
FRA,France,Europe
JPN,Japan,Asia
CHN,China,Asia
AUS,Australia,Oceania
BRA,Brazil,South America
IND,India,Asia"""
            
            with open(country_codes_path, 'w') as f:
                f.write(country_data)
            
            logger.info("Created country_codes.csv seed file")
        
        # Product categories lookup
        categories_path = self.project_root / 'seeds' / 'lookup_tables' / 'product_categories.csv'
        if not categories_path.exists():
            category_data = """category_id,category_name,parent_category_id,level
1,Electronics,,1
2,Clothing,,1
3,Home & Garden,,1
11,Computers,1,2
12,Mobile Phones,1,2
13,Audio & Video,1,2
21,Men's Clothing,2,2
22,Women's Clothing,2,2
23,Children's Clothing,2,2
31,Furniture,3,2
32,Garden Tools,3,2
33,Home Decor,3,2"""
            
            with open(categories_path, 'w') as f:
                f.write(category_data)
            
            logger.info("Created product_categories.csv seed file")
    
    def run_initial_dbt_commands(self, skip_run: bool = False) -> None:
        """Run initial DBT commands to validate setup"""
        logger.info("Running initial DBT commands...")
        
        commands = [
            (['dbt', 'debug'], "DBT debug check"),
            (['dbt', 'compile'], "DBT compilation check")
        ]
        
        if not skip_run:
            commands.extend([
                (['dbt', 'seed'], "Loading seed data"),
                (['dbt', 'run', '--select', 'staging'], "Running staging models")
            ])
        
        for command, description in commands:
            try:
                logger.info(f"Executing: {description}")
                result = subprocess.run(command, 
                                      cwd=self.project_root,
                                      capture_output=True, text=True, check=True)
                logger.info(f"✓ {description} completed successfully")
                
                # Log any warnings
                if 'WARNING' in result.stderr or 'WARN' in result.stderr:
                    logger.warning(f"Warnings in {description}:")
                    logger.warning(result.stderr)
                    
            except subprocess.CalledProcessError as e:
                logger.error(f"✗ {description} failed:")
                logger.error(e.stderr)
                if 'seed' not in command[1]:  # Don't fail on seed errors
                    logger.warning("Continuing despite error...")
    
    def generate_documentation(self) -> None:
        """Generate DBT documentation"""
        logger.info("Generating DBT documentation...")
        
        try:
            subprocess.run(['dbt', 'docs', 'generate'], 
                          cwd=self.project_root, check=True, capture_output=True)
            logger.info("✓ DBT documentation generated successfully")
            logger.info("Run 'dbt docs serve' to view the documentation")
        except subprocess.CalledProcessError as e:
            logger.error(f"Documentation generation failed: {e.stderr}")
            logger.warning("Continuing without documentation...")
    
    def create_environment_file(self) -> None:
        """Create sample .env file"""
        logger.info("Creating sample .env file...")
        
        env_file_
