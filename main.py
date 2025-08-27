#!/usr/bin/env python3
"""Example usage of CloudConduit package."""

import pandas as pd
from cloudconduit import (
    connect_snowflake, 
    connect_databricks, 
    connect_s3, 
    CloudConduit, 
    CredentialManager,
    push_config_to_env, 
    show_config, 
    create_user_config
)


def main():
    """Demonstrate CloudConduit usage."""
    print("CloudConduit - Unified API for Snowflake, Databricks, and S3")
    print("=" * 60)
    
    # Example 1: Using individual connectors
    print("\n1. Individual Connectors:")
    
    # Snowflake example (uses current system user and account from config as default)
    try:
        sf = connect_snowflake()  # Automatically uses current system user and account from config
        print("✓ Snowflake connector created")
        print(f"  Using username: {sf.username}")
        print(f"  Using account: {sf.account}")
        # sf.execute("SELECT CURRENT_USER(), CURRENT_ROLE()")
    except Exception as e:
        print(f"✗ Snowflake connection failed: {e}")
    
    # Databricks example
    try:
        db = connect_databricks()
        print("✓ Databricks connector created")
        # db.execute("SELECT current_user()")
    except Exception as e:
        print(f"✗ Databricks connection failed: {e}")
    
    # S3 example
    try:
        s3 = connect_s3()
        print("✓ S3 connector created")
        # s3.list_buckets()
    except Exception as e:
        print(f"✗ S3 connection failed: {e}")
    
    # Example 2: Using unified interface
    print("\n2. Unified Interface:")
    cc = CloudConduit()
    
    # Create sample DataFrame
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [100, 200, 300]
    })
    
    print("✓ Created sample DataFrame:")
    print(df)
    
    # Example operations (commented out to avoid actual connections)
    print("\n3. Example Operations (commented out):")
    print("# Upload DataFrame to Snowflake (uses system user and account automatically):")
    print("# sf = cc.snowflake()  # No account or username needed!")
    print("# sf.upload_df(df, 'my_table')")
    
    print("\n# Execute query:")
    print("# result = sf.execute('SELECT * FROM my_table')")
    
    print("\n# Copy table:")
    print("# sf.copy_table('source_table', 'target_table')")
    
    print("\n# Grant access:")
    print("# sf.grant_access('my_table', 'some_role', 'SELECT')")
    
    print("\n# Upload to S3:")
    print("# s3 = cc.s3()")
    print("# s3.upload_df(df, 'my-bucket', 'data/sample.csv', file_format='csv')")
    
    print("\n4. Environment Variables Setup:")
    print("Set these environment variables for authentication:")
    print("   Snowflake:")
    print("     Required:")
    print("       - SNOWFLAKE_ACCOUNT (your account identifier)")
    print("       - SNOWFLAKE_WAREHOUSE (compute resources)")
    print("       - SNOWFLAKE_PASSWORD or SNOWFLAKE_PRIVATE_KEY_PATH")
    print("     Optional:")
    print("       - SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA")
    print("       - SNOWFLAKE_AUTHENTICATOR (for SSO)")
    print("   Databricks:")
    print("     - DATABRICKS_SERVER_HOSTNAME")
    print("     - DATABRICKS_HTTP_PATH")
    print("     - DATABRICKS_ACCESS_TOKEN")
    print("   S3/AWS:")
    print("     - AWS_ACCESS_KEY_ID")
    print("     - AWS_SECRET_ACCESS_KEY")
    print("     - AWS_DEFAULT_REGION")
    
    print("\n5. Automatic Configuration Management:")
    print("CloudConduit offers flexible configuration loading!")
    print("   - Required: account, warehouse → YAML config file or env vars")
    print("   - Optional: database, schema → YAML config file or env vars")  
    print("   - Critical: passwords, tokens → Environment variables or keychain")
    print("")
    print("   # Option 1: Full import (auto-loads config)")
    print("   import cloudconduit")
    print("   sf = cloudconduit.connect_snowflake()  # Automatic!")
    print("")
    print("   # Option 2: Lightweight config loading")
    print("   from cloudconduit.utils.quick_config import load_config")
    print("   load_config()  # Loads without full import")
    print("   from cloudconduit.connectors.snowflake import SnowflakeConnector")
    print("   sf = SnowflakeConnector()")
    print("")
    print("   # Option 3: Manual operations:")
    print("   from cloudconduit import push_config_to_env, show_config")
    print("   push_config_to_env()      # Manual push")
    print("   show_config()             # View configuration")
    print("")
    print("   # VS Code Setup: See VSCODE_SETUP.md for permanent IDE configuration")
    print("   # Disable auto-loading: export CLOUDCONDUIT_DISABLE_AUTO_CONFIG=1")
    
    print("\n6. Snowflake Keychain Support (macOS):")
    print("Store password securely in keychain:")
    print("   from cloudconduit import CredentialManager")
    print("   cm = CredentialManager()")
    print("   # Store password in keychain:")
    print("   cm.set_credential('SNOWFLAKE_PASSWORD', 'your-password')")
    print("   # Username is auto-detected from system user")
    
    print("\n7. Configuration Demo:")
    try:
        # Show current package configuration
        print("   Package default configuration:")
        show_config()
        
        # Demonstrate pushing to environment
        print("\n   Pushing defaults to environment variables...")
        env_vars = push_config_to_env()
        if env_vars:
            print(f"   ✓ Set {len(env_vars)} environment variables:")
            for var, value in env_vars.items():
                print(f"     {var} = {value}")
        else:
            print("   ✓ All environment variables already set")
        
    except Exception as e:
        print(f"   ✗ Config demo failed: {e}")


if __name__ == "__main__":
    main()
