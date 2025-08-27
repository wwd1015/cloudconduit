#!/usr/bin/env python3
"""Example usage of CloudConduit package."""

import pandas as pd
from cloudconduit import (
    connect_snowflake, 
    connect_databricks, 
    connect_s3, 
    CloudConduit, 
    ConfigManager,
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
    
    print("\n5. Configuration Priority System:")
    print("CloudConduit follows this configuration priority order (highest to lowest):")
    print("   1. Function parameters (highest priority)")
    print("   2. Environment variables")
    print("   3. Keychain (credentials only, macOS)")
    print("   4. config.yaml defaults (lowest priority)")
    print("")
    print("   # Simple usage - no setup needed!")
    print("   import cloudconduit")
    print("   sf = cloudconduit.connect_snowflake()  # Uses defaults from config.yaml")
    print("")
    print("   # Override with function parameters:")
    print("   sf = cloudconduit.connect_snowflake('custom-user', {'warehouse': 'LARGE_WH'})")
    print("")
    print("   # Or set environment variables:")
    print("   os.environ['SNOWFLAKE_WAREHOUSE'] = 'LARGE_WH'")
    print("   sf = cloudconduit.connect_snowflake()")
    print("")
    print("   # VS Code Setup: See VSCODE_SETUP.md for permanent IDE configuration")
    
    print("\n6. Snowflake Keychain Support (macOS):")
    print("Store password securely in keychain:")
    print("   from cloudconduit import ConfigManager")
    print("   cm = ConfigManager()")
    print("   # Store password in keychain:")
    print("   cm.set_credential('SNOWFLAKE_PASSWORD', 'your-password')")
    print("   # Username is auto-detected from system user")
    
    print("\n7. Configuration Demo:")
    try:
        # Show current package configuration
        print("   Package default configuration:")
        cm = ConfigManager()
        cm.show_config()
        
        print("\n   ✓ Configuration loads automatically with priority system")
        print("     - No manual setup required!")
        print("     - Override any setting with environment variables")
        print("     - Or pass parameters directly to functions")
        
    except Exception as e:
        print(f"   ✗ Config demo failed: {e}")


if __name__ == "__main__":
    main()
