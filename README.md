# CloudConduit

A unified Python API for connecting to Snowflake, Databricks, and S3 with comprehensive credential management, including macOS keychain integration and environment variable support.

## Features

- **Unified Interface**: Single API to work with Snowflake, Databricks, and S3
- **Automatic User Detection**: Uses current system user as default for Snowflake connections
- **Credential Management**: Automatic credential retrieval from macOS keychain and environment variables
- **SSO Support**: Snowflake SSO authentication support
- **DataFrame Operations**: Easy pandas DataFrame upload/download
- **Common Operations**: Execute queries, copy tables, drop tables, grant access
- **Context Managers**: Clean connection management with `with` statements
- **Type Hints**: Full type annotation support

## Installation

### From Enterprise Artifactory (Recommended)

```bash
# Install from enterprise artifactory
pip install cloudconduit --index-url https://artifactory.yourcompany.com/artifactory/api/pypi/pypi-local/simple

# For development with optional dependencies
pip install cloudconduit[dev] --index-url https://artifactory.yourcompany.com/artifactory/api/pypi/pypi-local/simple
```

**Configure pip for permanent artifactory access:**

Create or update `~/.pip/pip.conf` (Linux/macOS) or `%APPDATA%\pip\pip.ini` (Windows):
```ini
[global]
index-url = https://artifactory.yourcompany.com/artifactory/api/pypi/pypi-local/simple
trusted-host = artifactory.yourcompany.com
extra-index-url = https://pypi.org/simple
```

Or set environment variables:
```bash
export PIP_INDEX_URL=https://artifactory.yourcompany.com/artifactory/api/pypi/pypi-local/simple
export PIP_TRUSTED_HOST=artifactory.yourcompany.com
export PIP_EXTRA_INDEX_URL=https://pypi.org/simple
```

### From GitHub Enterprise

```bash
# Install from GitHub Enterprise repository
pip install git+https://github.com/yourcompany/cloudconduit.git

# For development with optional dependencies
pip install git+https://github.com/yourcompany/cloudconduit.git[dev]
```

### From Local Source

```bash
# Clone the repository
git clone https://github.com/yourcompany/cloudconduit.git
cd cloudconduit

# Install in development mode
pip install -e .

# Or install with development dependencies
pip install -e .[dev]
```

### From Local Wheel/Tarball

```bash
# If you have a built distribution file
pip install cloudconduit-0.2.0-py3-none-any.whl

# Or from source distribution
pip install cloudconduit-0.2.0.tar.gz
```

## Quick Start

### First-Time Setup with Password Input

```python
import os
from cloudconduit import connect_snowflake, ConfigManager
import pandas as pd

# Option 1: Set password as environment variable
os.environ["SNOWFLAKE_PASSWORD"] = "your-password"
sf = connect_snowflake()  # Uses system user and account from config.yaml

# Option 2: Pass credentials directly in config
config = {"password": "your-password", "warehouse": "COMPUTE_WH"}
sf = connect_snowflake("your-username", config)

# Test connection
result = sf.execute("SELECT CURRENT_USER(), CURRENT_ROLE()")
print(result)
```

### Secure Keychain Setup (Recommended for Development)

```python
from cloudconduit import ConfigManager, connect_snowflake

# Store password securely in macOS keychain (one-time setup)
cm = ConfigManager()
cm.set_credential("SNOWFLAKE_PASSWORD", "your-password")
cm.set_credential("DATABRICKS_ACCESS_TOKEN", "your-databricks-token")
print("✓ Credentials stored securely in keychain")

# Now you can connect without exposing passwords
sf = connect_snowflake()  # Uses system user, config.yaml defaults, and keychain password
print(f"Connected as: {sf.username} to account: {sf.account}")
```

### Comprehensive Usage Examples

```python
from cloudconduit import connect_snowflake, connect_databricks, connect_s3
import pandas as pd

# Create sample data
df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "department": ["Engineering", "Sales", "Marketing", "HR", "Finance"],
    "salary": [75000, 65000, 60000, 55000, 70000],
    "hire_date": pd.date_range("2020-01-01", periods=5, freq="6M")
})

# === Snowflake Examples ===
sf = connect_snowflake()

# Upload DataFrame to Snowflake
sf.upload_df(df, "db.schema.tbl", if_exists="replace")
print("✓ Data uploaded to Snowflake")

# Execute queries
result = sf.execute("SELECT COUNT(*) as total_employees FROM employees")
print(f"Total employees: {result.iloc[0]['TOTAL_EMPLOYEES']}")

# Get data back as DataFrame
employees = sf.execute("SELECT * FROM employees WHERE salary > 60000")
print(f"High earners:\n{employees}")

# Copy table for backup
sf.copy_table("employees", "employees_backup")
print("✓ Table backed up")

# Grant access to role
sf.grant_access("employees", "ANALYST_ROLE", "SELECT")
print("✓ Access granted")

# List tables in current schema
tables = sf.list_tables()
print(f"Available tables: {tables}")

# Get table information
info = sf.get_table_info("employees")
print(f"Table info: {info}")

# Drop table when done
sf.drop_table("employees_backup")
print("✓ Backup table dropped")

# === Databricks Examples ===
db = connect_databricks()

# Upload DataFrame (creates table)
db.upload_df(df, "main.default.employees", if_exists="replace")
print("✓ Data uploaded to Databricks")

# Execute query
result = db.execute("SELECT department, AVG(salary) as avg_salary FROM main.default.employees GROUP BY department")
print(f"Average salary by department:\n{result}")

# List tables and schemas
schemas = db.list_schemas("main")
tables = db.list_tables("main", "default")
print(f"Schemas: {schemas}")
print(f"Tables: {tables}")

# === S3 Examples ===
s3 = connect_s3()

# Upload DataFrame as different formats
s3.upload_df(df, "my-data-bucket", "employees/data.csv", file_format="csv")
s3.upload_df(df, "my-data-bucket", "employees/data.parquet", file_format="parquet")
s3.upload_df(df, "my-data-bucket", "employees/data.json", file_format="json")
print("✓ Data uploaded to S3 in multiple formats")

# Download data back as DataFrame
downloaded_df = s3.download_df("my-data-bucket", "employees/data.parquet", file_format="parquet")
print(f"Downloaded data shape: {downloaded_df.shape}")

# List buckets and objects
buckets = s3.list_buckets()
objects = s3.list_objects("my-data-bucket", prefix="employees/")
print(f"Available buckets: {[b['Name'] for b in buckets]}")
print(f"Objects in employees/: {[obj['Key'] for obj in objects]}")

# Generate presigned URL for sharing
url = s3.create_presigned_url("my-data-bucket", "employees/data.csv", expiration=3600)
print(f"Shareable URL (1 hour): {url[:50]}...")

# Copy objects between locations
s3.copy_object("my-data-bucket", "employees/data.csv", "my-backup-bucket", "archive/employees_2023.csv")
print("✓ Data copied to backup location")
```

### Using Unified Interface

```python
from cloudconduit import CloudConduit
import pandas as pd

# Create unified interface - connects to all services
cc = CloudConduit()

# Access all connectors through one interface
sf = cc.snowflake()   # Uses keychain/env credentials automatically
db = cc.databricks()  # Uses keychain/env credentials automatically
s3 = cc.s3()          # Uses keychain/env credentials automatically

# Cross-platform data pipeline example
df = pd.DataFrame({"id": [1, 2, 3], "value": [100, 200, 300]})

# 1. Upload to S3
s3.upload_df(df, "data-lake", "raw/input.csv", file_format="csv")

# 2. Process in Databricks
db.execute("CREATE TABLE processed AS SELECT id, value * 2 as doubled_value FROM delta.`s3://data-lake/raw/input.csv`")

# 3. Load results to Snowflake
processed_df = db.execute("SELECT * FROM processed")
sf.upload_df(processed_df, "final_results")

print("✓ Cross-platform pipeline completed")
```

### Using Context Managers

```python
from cloudconduit import SnowflakeConnector, DatabricksConnector

# Automatic connection cleanup
with SnowflakeConnector() as sf:
    # Upload data
    df = pd.DataFrame({"test": [1, 2, 3]})
    sf.upload_df(df, "temp_table")
    
    # Process data
    result = sf.execute("SELECT COUNT(*) FROM temp_table")
    print(f"Row count: {result.iloc[0, 0]}")
    
    # Cleanup
    sf.drop_table("temp_table")
    
# Connection automatically closed here

# Chain multiple operations
with SnowflakeConnector() as sf, DatabricksConnector() as db:
    # Move data between platforms
    snowflake_data = sf.execute("SELECT * FROM important_table")
    db.upload_df(snowflake_data, "main.default.imported_data")
    print("✓ Data migrated from Snowflake to Databricks")
```

## Configuration

CloudConduit v0.2.0 uses a simple 4-level priority system that **works out of the box** with minimal configuration:

**Priority Order (highest to lowest):**
1. **Function parameters** (highest priority)
2. **Environment variables**  
3. **Keychain** (credentials only, macOS)
4. **config.yaml defaults** (lowest priority)

**💡 For detailed setup and VS Code integration:** See [VSCODE_SETUP.md](VSCODE_SETUP.md)

### Environment Variables

For **production deployments** or to **override defaults**, set these environment variables:

**Credentials (Required):**
```bash
# Snowflake
export SNOWFLAKE_PASSWORD="your-password"
# OR: export SNOWFLAKE_PRIVATE_KEY_PATH="/path/to/key.p8"
# OR: export SNOWFLAKE_AUTHENTICATOR="externalbrowser"  # SSO

# Databricks  
export DATABRICKS_ACCESS_TOKEN="your-access-token"

# AWS/S3
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
```

**Configuration Overrides (Optional):**
```bash
# Override config.yaml defaults if needed
export SNOWFLAKE_ACCOUNT="production-account"
export SNOWFLAKE_WAREHOUSE="PROD_WH"
export DATABRICKS_SERVER_HOSTNAME="prod.databricks.com"
export AWS_DEFAULT_REGION="us-west-2"
```

### macOS Keychain Integration

Store credentials securely using macOS keychain for development (see Quick Start for examples):

```python
from cloudconduit import ConfigManager
cm = ConfigManager()
cm.set_credential("SNOWFLAKE_PASSWORD", "your-password")  # One-time setup
# Now connect without exposing passwords: sf = connect_snowflake()
```

## API Reference

### Core Methods (Available on all connectors)

**Data Operations:**
- `execute(query)` - Execute SQL queries, returns pandas DataFrame
- `upload_df(df, table_name, if_exists="replace")` - Upload DataFrame as table
- `download_df()` - Download table as DataFrame (S3 only)

**Table Management:**
- `copy_table(source, target)` - Duplicate tables
- `drop_table(table_name)` - Remove tables
- `list_tables()` - List available tables
- `get_table_info(table_name)` - Get table metadata

**Access Control:**
- `grant_access(table, role, permissions)` - Grant table access

### S3-Specific Methods

- `list_buckets()` - List S3 buckets
- `list_objects(bucket, prefix)` - List objects in bucket
- `copy_object(src_bucket, src_key, dst_bucket, dst_key)` - Copy S3 objects
- `create_presigned_url(bucket, key, expiration)` - Generate shareable URLs

**File Formats:** S3 supports CSV, Parquet, and JSON via `file_format` parameter.

### Utility Functions

```python
from cloudconduit import execute, upload_df, copy_table, drop_table, grant_access
# Generic functions that work with any connector instance
```

## Error Handling

CloudConduit provides informative error messages and proper exception handling:

```python
try:
    sf = connect_snowflake("username")  # Account from config/env
    result = sf.execute("SELECT * FROM non_existent_table")
except ConnectionError as e:
    print(f"Connection failed: {e}")
except RuntimeError as e:
    print(f"Query failed: {e}")
```

## Development

Clone the repository and install development dependencies:

```bash
git clone https://github.com/yourusername/cloudconduit.git
cd cloudconduit
pip install -e .[dev]
```

Run tests:
```bash
pytest
```

Format code:
```bash
black cloudconduit/
```

Type checking:
```bash
mypy cloudconduit/
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run the test suite
6. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Support

- Report issues: [GitHub Issues](https://github.com/yourusername/cloudconduit/issues)
- Documentation: [GitHub Wiki](https://github.com/yourusername/cloudconduit/wiki)