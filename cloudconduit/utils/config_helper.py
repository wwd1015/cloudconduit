"""Simple configuration helper for CloudConduit non-critical parameters."""

import os
import yaml
from pathlib import Path
from typing import Dict, Any, Optional


def get_default_config_path() -> Path:
    """Get path to the default config file in the package."""
    return Path(__file__).parent.parent / "config.yaml"


def load_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """Load configuration from YAML file.
    
    Args:
        config_path: Path to config file. If None, uses package default.
        
    Returns:
        Configuration dictionary
    """
    if config_path is None:
        config_path = get_default_config_path()
    
    if not config_path.exists():
        return {}
    
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        print(f"Warning: Failed to load config from {config_path}: {e}")
        return {}


def push_config_to_env(config_path: Optional[Path] = None, override: bool = False) -> Dict[str, str]:
    """Push non-critical configuration parameters to environment variables.
    
    Args:
        config_path: Path to config file. If None, uses package default.
        override: Whether to override existing environment variables
        
    Returns:
        Dictionary of environment variables that were set
    """
    config = load_config(config_path)
    env_vars_set = {}
    
    # Snowflake parameters
    snowflake = config.get('snowflake', {})
    sf_mappings = {
        'account': 'SNOWFLAKE_ACCOUNT',      # Required
        'warehouse': 'SNOWFLAKE_WAREHOUSE',  # Required
        'database': 'SNOWFLAKE_DATABASE',    # Optional 
        'schema': 'SNOWFLAKE_SCHEMA',        # Optional
    }
    
    for config_key, env_key in sf_mappings.items():
        value = snowflake.get(config_key)
        if value and (env_key not in os.environ or override):
            os.environ[env_key] = str(value)
            env_vars_set[env_key] = str(value)
    
    # Databricks parameters
    databricks = config.get('databricks', {})
    db_mappings = {
        'server_hostname': 'DATABRICKS_SERVER_HOSTNAME',
        'http_path': 'DATABRICKS_HTTP_PATH',
        'catalog': 'DATABRICKS_CATALOG',
        'schema': 'DATABRICKS_SCHEMA'
    }
    
    for config_key, env_key in db_mappings.items():
        value = databricks.get(config_key)
        if value and (env_key not in os.environ or override):
            os.environ[env_key] = str(value)
            env_vars_set[env_key] = str(value)
    
    # S3/AWS parameters
    s3 = config.get('s3', {})
    s3_mappings = {
        'region_name': 'AWS_DEFAULT_REGION'
    }
    
    for config_key, env_key in s3_mappings.items():
        value = s3.get(config_key)
        if value and (env_key not in os.environ or override):
            os.environ[env_key] = str(value)
            env_vars_set[env_key] = str(value)
    
    return env_vars_set


def show_config(config_path: Optional[Path] = None) -> None:
    """Display current configuration.
    
    Args:
        config_path: Path to config file. If None, uses package default.
    """
    config = load_config(config_path)
    
    if not config:
        print("No configuration found.")
        return
    
    print("CloudConduit Configuration:")
    print("=" * 40)
    
    for service, params in config.items():
        print(f"\n{service.upper()}:")
        for key, value in params.items():
            print(f"  {key}: {value}")


def create_user_config(target_path: Path) -> None:
    """Create a user configuration file based on the package default.
    
    Args:
        target_path: Where to create the user config file
    """
    default_config = load_config()
    
    if not default_config:
        print("Warning: No default configuration found.")
        return
    
    # Add comments for user guidance
    config_with_comments = f"""# CloudConduit Configuration
# Copy and modify this file to customize your default parameters
# Only non-critical parameters are stored here
# Critical credentials (passwords, tokens) should be in environment variables or keychain

snowflake:
  account: "{default_config.get('snowflake', {}).get('account', 'your-account')}"     # Required: Your Snowflake account (without .snowflakecomputing.com)
  warehouse: "{default_config.get('snowflake', {}).get('warehouse', 'COMPUTE_WH')}"  # Required: Warehouse for compute resources
  database: "{default_config.get('snowflake', {}).get('database', 'ANALYTICS')}"     # Optional: Default database
  schema: "{default_config.get('snowflake', {}).get('schema', 'PUBLIC')}"            # Optional: Default schema

databricks:
  server_hostname: "{default_config.get('databricks', {}).get('server_hostname', '')}"  # your-workspace.cloud.databricks.com
  http_path: "{default_config.get('databricks', {}).get('http_path', '')}"              # /sql/1.0/warehouses/your-warehouse-id
  catalog: "{default_config.get('databricks', {}).get('catalog', 'main')}"
  schema: "{default_config.get('databricks', {}).get('schema', 'default')}"

s3:
  region_name: "{default_config.get('s3', {}).get('region_name', 'us-east-1')}"
"""
    
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with open(target_path, 'w') as f:
        f.write(config_with_comments)
    
    print(f"Created user configuration file: {target_path}")


def get_env_mappings() -> Dict[str, Dict[str, str]]:
    """Get mapping of configuration keys to environment variable names.
    
    Returns:
        Nested dictionary showing config->env mappings for each service
    """
    return {
        'snowflake': {
            'account': 'SNOWFLAKE_ACCOUNT',      # Required
            'warehouse': 'SNOWFLAKE_WAREHOUSE',  # Required 
            'database': 'SNOWFLAKE_DATABASE',    # Optional
            'schema': 'SNOWFLAKE_SCHEMA',        # Optional
        },
        'databricks': {
            'server_hostname': 'DATABRICKS_SERVER_HOSTNAME',
            'http_path': 'DATABRICKS_HTTP_PATH',
            'catalog': 'DATABRICKS_CATALOG',
            'schema': 'DATABRICKS_SCHEMA'
        },
        's3': {
            'region_name': 'AWS_DEFAULT_REGION'
        }
    }