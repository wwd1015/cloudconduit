# VS Code Environment Setup for CloudConduit

This guide shows how to permanently configure environment variables in VS Code for CloudConduit development.

## Method 1: VS Code Settings (Recommended)

### 1. Create/Edit VS Code Settings

**Option A: Workspace Settings (Project-specific)**
```bash
# Create .vscode/settings.json in your project root
mkdir -p .vscode
```

Create `.vscode/settings.json`:
```json
{
    "python.terminal.activateEnvironment": true,
    "python.envFile": "${workspaceFolder}/.env",
    "terminal.integrated.env.osx": {
        "SNOWFLAKE_ACCOUNT": "your-account-id",
        "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
        "SNOWFLAKE_DATABASE": "ANALYTICS",
        "SNOWFLAKE_SCHEMA": "PUBLIC",
        "DATABRICKS_SERVER_HOSTNAME": "your-workspace.cloud.databricks.com",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/your-warehouse-id",
        "DATABRICKS_CATALOG": "main",
        "DATABRICKS_SCHEMA": "default",
        "AWS_DEFAULT_REGION": "us-east-1"
    },
    "terminal.integrated.env.linux": {
        "SNOWFLAKE_ACCOUNT": "your-account-id",
        "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
        "SNOWFLAKE_DATABASE": "ANALYTICS",
        "SNOWFLAKE_SCHEMA": "PUBLIC",
        "DATABRICKS_SERVER_HOSTNAME": "your-workspace.cloud.databricks.com",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/your-warehouse-id",
        "DATABRICKS_CATALOG": "main",
        "DATABRICKS_SCHEMA": "default",
        "AWS_DEFAULT_REGION": "us-east-1"
    },
    "terminal.integrated.env.windows": {
        "SNOWFLAKE_ACCOUNT": "your-account-id",
        "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
        "SNOWFLAKE_DATABASE": "ANALYTICS",
        "SNOWFLAKE_SCHEMA": "PUBLIC",
        "DATABRICKS_SERVER_HOSTNAME": "your-workspace.cloud.databricks.com",
        "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/your-warehouse-id",
        "DATABRICKS_CATALOG": "main",
        "DATABRICKS_SCHEMA": "default",
        "AWS_DEFAULT_REGION": "us-east-1"
    }
}
```

**Option B: Global Settings (All projects)**
- Press `Ctrl+Shift+P` (or `Cmd+Shift+P` on Mac)
- Type "Preferences: Open Settings (JSON)"
- Add the same `terminal.integrated.env.*` configuration

### 2. Create .env File (Alternative)

Create `.env` in your project root:
```bash
# CloudConduit Configuration
# Required
SNOWFLAKE_ACCOUNT=your-account-id
SNOWFLAKE_WAREHOUSE=COMPUTE_WH

# Optional  
SNOWFLAKE_DATABASE=ANALYTICS
SNOWFLAKE_SCHEMA=PUBLIC

DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
DATABRICKS_CATALOG=main
DATABRICKS_SCHEMA=default

AWS_DEFAULT_REGION=us-east-1

# Optional: Disable auto-config if needed
# CLOUDCONDUIT_DISABLE_AUTO_CONFIG=1
```

Add to `.gitignore`:
```bash
# Add to your .gitignore file
.env
.env.local
.env.*.local
```

## Method 2: Launch Configuration

Create `.vscode/launch.json`:
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: CloudConduit Development",
            "type": "debugpy",
            "request": "launch",
            "program": "${file}",
            "console": "integratedTerminal",
            "env": {
                "SNOWFLAKE_ACCOUNT": "your-account-id",
                "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
                "SNOWFLAKE_DATABASE": "ANALYTICS",
                "SNOWFLAKE_SCHEMA": "PUBLIC",
                "DATABRICKS_SERVER_HOSTNAME": "your-workspace.cloud.databricks.com",
                "DATABRICKS_HTTP_PATH": "/sql/1.0/warehouses/your-warehouse-id",
                "AWS_DEFAULT_REGION": "us-east-1"
            }
        }
    ]
}
```

## Method 3: Python Extension Settings

### 1. Configure Python Interpreter with Environment

Press `Ctrl+Shift+P` → "Python: Select Interpreter" → Configure with environment.

### 2. Set Python Path with Environment File

In VS Code Settings:
```json
{
    "python.defaultInterpreterPath": "/path/to/your/python",
    "python.envFile": "${workspaceFolder}/.env"
}
```

## Quick Usage Examples

### Option 1: Full Package Import (Auto-config)
```python
import cloudconduit  # Automatically loads config
sf = cloudconduit.connect_snowflake()  # Just works!
```

### Option 2: Lightweight Import with Manual Config
```python
# Load config without importing full package
from cloudconduit.utils.quick_config import load_config
load_config()

# Then use individual connectors
from cloudconduit.connectors.snowflake import SnowflakeConnector
sf = SnowflakeConnector()
```

### Option 3: Direct Individual Imports
```python
# Direct import - config from environment only
from cloudconduit.connectors.snowflake import SnowflakeConnector
sf = SnowflakeConnector()  # Uses environment variables
```

## VS Code Extensions (Recommended)

Install these VS Code extensions for better environment management:

1. **Python** - Microsoft's Python extension
2. **Python Docstring Generator** - For documentation
3. **GitLens** - Git integration
4. **DotENV** - Syntax highlighting for .env files

## Verification

Test your setup with this simple script:
```python
# test_setup.py
import os
print("Environment Variables:")
for key in ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_WAREHOUSE', 'DATABRICKS_SERVER_HOSTNAME']:
    print(f"  {key}: {os.getenv(key, 'NOT SET')}")

# Test CloudConduit import
try:
    import cloudconduit
    print("✓ CloudConduit imports successfully")
except Exception as e:
    print(f"✗ CloudConduit import failed: {e}")
```

## Security Best Practices

1. **Never commit credentials** to version control
2. **Use keychain for passwords/tokens**:
   ```python
   from cloudconduit import CredentialManager
   cm = CredentialManager()
   cm.set_credential("SNOWFLAKE_PASSWORD", "your-password")
   ```
3. **Add .env to .gitignore**
4. **Use different .env files** for different environments:
   - `.env.development`
   - `.env.staging`  
   - `.env.production`

## Troubleshooting

### Environment Variables Not Loading
1. Restart VS Code after changing settings
2. Check if `.env` file is in correct location
3. Verify `python.envFile` setting points to correct file
4. Test with: `python -c "import os; print(os.getenv('SNOWFLAKE_ACCOUNT'))"`

### Auto-config Not Working
1. Check if `CLOUDCONDUIT_DISABLE_AUTO_CONFIG` is set
2. Manually load config: `from cloudconduit.utils.quick_config import load_config; load_config()`

### Permission Issues (macOS)
1. VS Code may need keychain access permissions
2. Grant access when prompted or manually in System Preferences

This setup ensures your CloudConduit environment variables are automatically available in VS Code terminals and Python environments!