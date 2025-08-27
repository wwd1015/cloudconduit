# Configuration

CloudConduit supports multiple ways to configure credentials and connection settings.

## Environment Variables

The recommended way to configure CloudConduit is through environment variables.

### Snowflake

```bash
# Password authentication
export SNOWFLAKE_PASSWORD="your-password"

# OR Key-pair authentication
export SNOWFLAKE_PRIVATE_KEY_PATH="/path/to/your/private_key.p8"
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE="your-passphrase"

# Connection settings
export SNOWFLAKE_WAREHOUSE="your-warehouse"
export SNOWFLAKE_DATABASE="your-database"
export SNOWFLAKE_SCHEMA="your-schema"
export SNOWFLAKE_ROLE="your-role"

# SSO authentication
export SNOWFLAKE_AUTHENTICATOR="externalbrowser"
```

### Databricks

```bash
export DATABRICKS_SERVER_HOSTNAME="your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/your-warehouse-id"
export DATABRICKS_ACCESS_TOKEN="your-access-token"

# Optional settings
export DATABRICKS_CATALOG="your-catalog"
export DATABRICKS_SCHEMA="your-schema"
```

### AWS/S3

```bash
export AWS_ACCESS_KEY_ID="your-access-key-id"
export AWS_SECRET_ACCESS_KEY="your-secret-access-key"
export AWS_DEFAULT_REGION="us-east-1"

# Optional for temporary credentials
export AWS_SESSION_TOKEN="your-session-token"
```

## macOS Keychain Integration

On macOS, CloudConduit can securely store and retrieve credentials from the system keychain.

### Storing Credentials

```python
from cloudconduit import CredentialManager

cm = CredentialManager()

# Store a password in keychain
cm.set_credential("password", "your-secret-password", "your-username")
```

### Retrieving Credentials

```python
# Credentials are automatically retrieved when creating connectors
from cloudconduit import connect_snowflake

# This will automatically check keychain for stored password
sf = connect_snowflake("your-account", "your-username")
```

### Managing Keychain Credentials

#### Storing Snowflake Password

```bash
# Using Python
python3 -c "
from cloudconduit import CredentialManager
cm = CredentialManager()
cm.set_credential('password', 'your-snowflake-password', 'your-snowflake-username')
print('Password stored in keychain')
"
```

#### Storing Multiple Credentials

```python
from cloudconduit import CredentialManager

cm = CredentialManager()

# Store different types of credentials
cm.set_credential("snowflake_password", "sf-password", "sf-user")
cm.set_credential("databricks_token", "db-token", "db-user") 
cm.set_credential("aws_secret", "aws-secret", "aws-user")
```

#### Deleting Credentials

```python
from cloudconduit import CredentialManager

cm = CredentialManager()

# Delete a specific credential
cm.delete_credential("your-username")
```

### Keychain Security Features

- **Encrypted Storage**: All credentials are encrypted using macOS keychain encryption
- **User Authentication**: macOS may prompt for user authentication when accessing stored credentials
- **Per-Application**: Credentials are stored per-application (CloudConduit) for security isolation
- **Backup Integration**: Keychain credentials are included in macOS backups and sync across devices

### Keychain vs Environment Variables

CloudConduit follows this priority order for credential lookup:

1. **Environment Variables** (highest priority)
2. **macOS Keychain** (if on macOS and username provided)
3. **Configuration Parameters** (lowest priority)

This means environment variables will always override keychain credentials, providing flexibility for different deployment scenarios.

## Configuration Files

### .env Files

CloudConduit supports `.env` files for local development:

```bash
# .env
SNOWFLAKE_ACCOUNT=your-account
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=MY_DB
SNOWFLAKE_SCHEMA=PUBLIC

DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abc123
DATABRICKS_ACCESS_TOKEN=your-token

AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=us-east-1
```

### Configuration Objects

You can also pass configuration directly to connectors:

```python
from cloudconduit import SnowflakeConnector, DatabricksConnector, S3Connector

# Snowflake with explicit config
sf_config = {
    "password": "your-password",
    "warehouse": "COMPUTE_WH",
    "database": "MY_DB",
    "schema": "PUBLIC",
    "role": "DATA_ANALYST"
}
sf = SnowflakeConnector("account", "username", sf_config)

# Databricks with explicit config
db_config = {
    "server_hostname": "your-workspace.cloud.databricks.com",
    "http_path": "/sql/1.0/warehouses/abc123",
    "access_token": "your-token",
    "catalog": "main",
    "schema": "default"
}
db = DatabricksConnector(db_config)

# S3 with explicit config
s3_config = {
    "aws_access_key_id": "your-access-key",
    "aws_secret_access_key": "your-secret-key", 
    "region_name": "us-west-2"
}
s3 = S3Connector(s3_config)
```

## Best Practices

### Security

1. **Never hardcode credentials** in your source code
2. **Use environment variables** for production deployments
3. **Use keychain** for local development on macOS
4. **Rotate credentials** regularly
5. **Use least-privilege** access principles

### Development vs Production

#### Development (macOS)
```python
# Store once in keychain
from cloudconduit import CredentialManager
cm = CredentialManager()
cm.set_credential("password", "dev-password", "dev-user")

# Use without explicit credentials
sf = connect_snowflake("dev-account", "dev-user")
```

#### Production (Linux/Docker)
```bash
# Set environment variables
export SNOWFLAKE_PASSWORD="$SECRET_PASSWORD"
export SNOWFLAKE_WAREHOUSE="PROD_WH"

# Use in application
sf = connect_snowflake("prod-account", "prod-user")
```

### Configuration Validation

```python
from cloudconduit import CredentialManager

cm = CredentialManager()

# Check if credentials are available
sf_creds = cm.get_snowflake_credentials("account", "username")
if not sf_creds.get("password") and not sf_creds.get("private_key_path"):
    raise ValueError("No Snowflake authentication method configured")

db_creds = cm.get_databricks_credentials()
required_db_params = ["server_hostname", "http_path", "access_token"]
missing_params = [p for p in required_db_params if not db_creds.get(p)]
if missing_params:
    raise ValueError(f"Missing Databricks parameters: {missing_params}")
```