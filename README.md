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
pip install cloudconduit-1.0.0-py3-none-any.whl

# Or from source distribution
pip install cloudconduit-1.0.0.tar.gz
```

## Quick Start

### Using Individual Connectors

```python
from cloudconduit import connect_snowflake, connect_databricks, connect_s3
import pandas as pd

# Connect to Snowflake (automatically uses current system user and account from config)
sf = connect_snowflake()
result = sf.execute("SELECT CURRENT_USER(), CURRENT_ROLE()")
print(result)

# Or specify username explicitly
sf = connect_snowflake("custom-username")

# Connect to Databricks  
db = connect_databricks()
tables = db.list_tables()
print(tables)

# Connect to S3
s3 = connect_s3()
buckets = s3.list_buckets()
print(buckets)
```

### Using Unified Interface

```python
from cloudconduit import CloudConduit
import pandas as pd

# Create unified interface
cc = CloudConduit()

# Access connectors (automatically uses system user and account from config)
sf = cc.snowflake()  # Uses current system user and account from config/env
db = cc.databricks()
s3 = cc.s3()

# Upload DataFrame to Snowflake
df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
sf.upload_df(df, "my_table")

# Execute query
result = sf.execute("SELECT * FROM my_table")
print(result)
```

### Using Context Managers

```python
from cloudconduit import SnowflakeConnector

with SnowflakeConnector() as sf:  # Uses system user and account from config
    result = sf.execute("SELECT * FROM my_table")
    sf.copy_table("source_table", "backup_table")
```

## Configuration

### Automatic User Detection

CloudConduit automatically detects your system username for Snowflake connections, eliminating the need to specify usernames in most cases:

```python
from cloudconduit import connect_snowflake, SnowflakeConnector

# These both automatically use your current system username and account from config
sf1 = connect_snowflake()
sf2 = SnowflakeConnector()

# System user: "john.doe" -> Snowflake username: "john.doe"
# System user: "Jane Smith" -> Snowflake username: "jane.smith"  
# System user: "admin@company.com" -> Snowflake username: "admin"
```

**Custom Username Override:**
```python
# Still works if you need to specify a different username
sf = connect_snowflake("custom.username")
```

**Domain Suffix Support:**
```python
# Add domain suffix to system username
config = {"domain_suffix": "@company.com"}
sf = SnowflakeConnector(config=config)
# Uses: "john.doe@company.com"
```

### Automatic Configuration Loading

CloudConduit automatically loads configuration when you import the package - **no manual setup required!**

```python
# Option 1: Full package import (auto-loads config)
import cloudconduit
sf = cloudconduit.connect_snowflake()  # Uses account from config/environment

# Option 2: Lightweight config loading (for individual imports)
from cloudconduit.utils.quick_config import load_config
load_config()  # Loads config without importing full package

from cloudconduit.connectors.snowflake import SnowflakeConnector
sf = SnowflakeConnector()  # Uses loaded config

# Option 3: Direct import (environment only)
from cloudconduit.connectors.snowflake import SnowflakeConnector  
sf = SnowflakeConnector()  # Uses environment variables only
```

**💡 For VS Code users:** See [VSCODE_SETUP.md](VSCODE_SETUP.md) for permanent environment variable configuration in your IDE.

### Configuration Setup Options

**Option 1: Update config.yaml (Recommended)**
```yaml
# Edit cloudconduit/config.yaml:
snowflake:
  account: "abc123.us-east-1"     # Your account identifier
  warehouse: "COMPUTE_WH"
  database: "ANALYTICS"
```

**Option 2: Environment Variables**
```bash
export SNOWFLAKE_ACCOUNT="abc123.us-east-1"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
```

**Option 3: Manual Push (if needed)**
```python
from cloudconduit import push_config_to_env

# Manually push config to environment
push_config_to_env()
```

### Disabling Auto-Configuration

If you need to disable automatic config loading:
```bash
export CLOUDCONDUIT_DISABLE_AUTO_CONFIG=1
python your_script.py
```

### Environment Variables (Optional Overrides)

With automatic config loading, **most settings are handled automatically**. You only need environment variables for:

1. **Credentials (always required)**
2. **Overriding config.yaml defaults**
3. **Production/Docker deployments**

**Required Credentials Only:**
```bash
# Snowflake - only credentials needed (account/warehouse from config.yaml)
export SNOWFLAKE_PASSWORD="your-password"
# OR for key-pair authentication:
export SNOWFLAKE_PRIVATE_KEY_PATH="/path/to/key.p8"
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE="key-passphrase"
# OR for SSO:
export SNOWFLAKE_AUTHENTICATOR="externalbrowser"

# Databricks - only credentials needed (server/path from config.yaml)
export DATABRICKS_ACCESS_TOKEN="your-access-token"

# AWS/S3 - only credentials needed (region from config.yaml)
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_SESSION_TOKEN="your-session-token"  # Optional, for temporary credentials
```

**Optional Overrides (only if you want to override config.yaml):**
```bash
# Override config.yaml settings if needed
export SNOWFLAKE_ACCOUNT="different-account"        # Required - override config.yaml
export SNOWFLAKE_WAREHOUSE="production-warehouse"   # Required - override config.yaml
export SNOWFLAKE_DATABASE="prod-database"           # Optional - override config.yaml
export SNOWFLAKE_SCHEMA="prod-schema"               # Optional - override config.yaml
export DATABRICKS_SERVER_HOSTNAME="prod.databricks.com"  # Override config.yaml
export AWS_DEFAULT_REGION="us-west-2"               # Override config.yaml
```

**Complete Override (for production/Docker):**
```bash
# If you don't want to use config.yaml at all
export SNOWFLAKE_ACCOUNT="prod-account"      # Required
export SNOWFLAKE_WAREHOUSE="PROD_WH"         # Required  
export SNOWFLAKE_DATABASE="PROD_DB"          # Optional
export SNOWFLAKE_SCHEMA="PROD_SCHEMA"        # Optional

export DATABRICKS_SERVER_HOSTNAME="prod.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/prod-id"
export DATABRICKS_CATALOG="prod"
export DATABRICKS_SCHEMA="main"

export AWS_DEFAULT_REGION="us-west-2"
```

### macOS Keychain Integration

CloudConduit provides seamless integration with macOS keychain for secure credential storage. This is especially useful for local development where you don't want to expose credentials in environment variables.

#### Storing Credentials in Keychain

**Using Python (Simple Method):**
```python
from cloudconduit import CredentialManager

cm = CredentialManager()

# Store credentials using simple key names
cm.set_credential("SNOWFLAKE_PASSWORD", "your-snowflake-password")
cm.set_credential("DATABRICKS_TOKEN", "your-databricks-token")  
cm.set_credential("AWS_SECRET_ACCESS_KEY", "your-aws-secret")

# Keychain entries are created as:
# - Service: "cloudconduit", Account: "snowflake_password"
# - Service: "cloudconduit", Account: "databricks_token"  
# - Service: "cloudconduit", Account: "aws_secret_access_key"
```

**Using Command Line:**
```python
# Store Snowflake password
python3 -c "
from cloudconduit import CredentialManager
cm = CredentialManager()
cm.set_credential('SNOWFLAKE_PASSWORD', 'your-actual-password')
print('✓ Snowflake password stored in keychain')
"

# Store Databricks token
python3 -c "
from cloudconduit import CredentialManager
cm = CredentialManager()
cm.set_credential('DATABRICKS_TOKEN', 'your-databricks-token')
print('✓ Databricks token stored in keychain')
"
```

#### Automatic Credential Retrieval

Once stored in keychain, credentials are automatically retrieved:

```python
from cloudconduit import connect_snowflake, connect_databricks

# No need to specify account or password - automatically retrieved from config and keychain
sf = connect_snowflake("custom-username")  # Account from config, password from keychain

# Or use all defaults (recommended)
sf = connect_snowflake()  # Account from config, username from system, password from keychain

# Works with all connectors
db = connect_databricks()  # Retrieves token from environment or keychain
```

#### Keychain Management

**List stored credentials:**
```bash
# View CloudConduit keychain entries
security find-generic-password -s "cloudconduit" -a "snowflake_password"
security find-generic-password -s "cloudconduit" -a "databricks_token"
```

**Delete credentials:**
```python
from cloudconduit import CredentialManager

cm = CredentialManager()

# Remove specific credentials
cm.delete_credential("SNOWFLAKE_PASSWORD")
cm.delete_credential("DATABRICKS_TOKEN")
```

**Manual keychain management:**
```bash
# Add credential manually using macOS security command
security add-generic-password -a "snowflake_password" -s "cloudconduit" -w "your-password"

# Delete credential manually
security delete-generic-password -a "snowflake_password" -s "cloudconduit"
```

#### Security Features

- **Encrypted Storage**: All credentials are encrypted using macOS keychain encryption
- **User Authentication**: macOS may prompt for user authentication when accessing stored credentials
- **Application Isolation**: Credentials are stored per-application (cloudconduit service name)
- **Sync Support**: Keychain items sync across your Apple devices (if enabled)
- **Backup Integration**: Included in Time Machine and iCloud keychain backups

#### Credential Priority Order

CloudConduit checks credentials in this order:

1. **Environment Variables** (highest priority)
2. **macOS Keychain** (if on macOS and username provided)  
3. **Configuration Parameters** (lowest priority)

This allows environment variables to override keychain for different deployment scenarios.

#### Best Practices for Keychain Usage

**Development Setup:**
```bash
# One-time setup for development
python3 -c "
from cloudconduit import CredentialManager
cm = CredentialManager()

# Store all your development credentials
cm.set_credential('SNOWFLAKE_PASSWORD', input('Snowflake password: '))
cm.set_credential('DATABRICKS_TOKEN', input('Databricks token: '))
print('✓ Development credentials stored securely')
"
```

**Production Deployment:**
```bash
# Use environment variables in production (Docker, CI/CD, etc.)
export SNOWFLAKE_PASSWORD="$PROD_PASSWORD"
export DATABRICKS_ACCESS_TOKEN="$PROD_TOKEN"

# Keychain is ignored when env vars are present
```

**Multi-Environment Support:**
```python
import os
from cloudconduit import connect_snowflake

# Determine environment
env = os.getenv('ENVIRONMENT', 'development')

if env == 'development':
    # Use keychain in development (credentials auto-retrieved)
    sf = connect_snowflake()  # Uses keychain for password, system user for username
else:
    # Use environment variables in production  
    sf = connect_snowflake()  # Uses env vars for everything
```

## API Reference

### Snowflake Connector

```python
from cloudconduit import SnowflakeConnector

sf = SnowflakeConnector("username", config={
    "warehouse": "compute_wh", 
    "database": "mydb",
    "schema": "public"
})

# Execute queries
result = sf.execute("SELECT * FROM table")

# Upload DataFrame
sf.upload_df(df, "table_name", if_exists="replace")

# Copy table
sf.copy_table("source", "target")

# Grant access
sf.grant_access("table", "role", "SELECT,INSERT")

# List tables
tables = sf.list_tables("schema_name")

# Get table info
info = sf.get_table_info("table_name")
```

### Databricks Connector

```python
from cloudconduit import DatabricksConnector

db = DatabricksConnector(config={
    "catalog": "main",
    "schema": "default"
})

# Execute queries
result = db.execute("SELECT * FROM table")

# Upload DataFrame (creates table)
db.upload_df(df, "catalog.schema.table", if_exists="replace")

# List tables
tables = db.list_tables("catalog", "schema")

# List schemas
schemas = db.list_schemas("catalog")
```

### S3 Connector

```python
from cloudconduit import S3Connector

s3 = S3Connector()

# Upload DataFrame as CSV/Parquet/JSON
s3.upload_df(df, "bucket", "path/file.csv", file_format="csv")

# Download as DataFrame
df = s3.download_df("bucket", "path/file.csv", file_format="csv")

# List buckets
buckets = s3.list_buckets()

# List objects
objects = s3.list_objects("bucket", prefix="path/")

# Copy objects
s3.copy_object("src_bucket", "src_key", "dst_bucket", "dst_key")

# Generate presigned URL
url = s3.create_presigned_url("bucket", "key", expiration=3600)
```

## Utility Functions

```python
from cloudconduit import execute, upload_df, copy_table, drop_table, grant_access

# Works with any database connector
result = execute(connector, "SELECT * FROM table")

# Upload DataFrame to any connector
upload_df(connector, df, "destination")

# Copy/drop tables
copy_table(connector, "source", "target")
drop_table(connector, "table_name")

# Grant access
grant_access(connector, "table", "user", "SELECT")
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