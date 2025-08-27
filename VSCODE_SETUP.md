# VS Code Environment Setup for CloudConduit v0.2.0

This guide shows how to set up VS Code for CloudConduit development with the new simplified configuration system.

**🎉 Good News:** With CloudConduit v0.2.0, **minimal setup is required!** The package now uses a smart configuration priority system that works out of the box.

## Configuration Priority System

CloudConduit v0.2.0 uses a simple priority system (highest to lowest):

1. **Function parameters** (highest priority)
2. **Environment variables**
3. **Keychain** (credentials only, macOS)
4. **config.yaml defaults** (lowest priority)

**For most users:** Just install and use! The defaults in `config.yaml` provide everything needed except credentials.

## Method 1: Minimal Setup (Recommended)

**Option 1: Just Use Defaults**
```python
# Works out of the box with config.yaml defaults!
import cloudconduit
sf = cloudconduit.connect_snowflake()  # Uses config.yaml defaults
```

**Option 2: Override via Environment Variables (Optional)**

Only set environment variables if you need to override the package defaults:

### VS Code Workspace Settings (Project-specific)
Create `.vscode/settings.json` in your project root:

```json
{
    "python.terminal.activateEnvironment": true,
    "python.envFile": "${workspaceFolder}/.env",
    "terminal.integrated.env.osx": {
        "SNOWFLAKE_ACCOUNT": "your-production-account",
        "SNOWFLAKE_WAREHOUSE": "LARGE_WH",
        "SNOWFLAKE_PASSWORD": "your-password"
    },
    "terminal.integrated.env.linux": {
        "SNOWFLAKE_ACCOUNT": "your-production-account",
        "SNOWFLAKE_WAREHOUSE": "LARGE_WH",
        "SNOWFLAKE_PASSWORD": "your-password"
    },
    "terminal.integrated.env.windows": {
        "SNOWFLAKE_ACCOUNT": "your-production-account",
        "SNOWFLAKE_WAREHOUSE": "LARGE_WH",
        "SNOWFLAKE_PASSWORD": "your-password"
    }
}
```

### VS Code Global Settings
Press `Ctrl+Shift+P` (or `Cmd+Shift+P` on Mac) → "Preferences: Open Settings (JSON)" → Add the same `terminal.integrated.env.*` configuration.

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

### Option 1: Simple Import (Uses config.yaml defaults)
```python
import cloudconduit
sf = cloudconduit.connect_snowflake()  # Uses config.yaml defaults + keychain/env overrides
```

### Option 2: Direct Connector Import
```python
# Direct import - uses all priority levels
from cloudconduit.connectors.snowflake import SnowflakeConnector
sf = SnowflakeConnector()  # Uses config.yaml + keychain + environment variables
```

### Option 3: Override with Function Parameters
```python
from cloudconduit import connect_snowflake
# Function parameters have highest priority
sf = connect_snowflake("custom-user", {"warehouse": "LARGE_WH"})
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

# Test CloudConduit configuration priority system
try:
    from cloudconduit import ConfigManager
    cm = ConfigManager()
    
    # Test priority system
    account = cm.get_config_value('snowflake', 'account')
    warehouse = cm.get_config_value('snowflake', 'warehouse')
    
    print(f"✓ CloudConduit configuration system working")
    print(f"  Account: {account}")
    print(f"  Warehouse: {warehouse}")
    
    import cloudconduit
    print("✓ CloudConduit imports successfully")
except Exception as e:
    print(f"✗ CloudConduit test failed: {e}")
```

## Security Best Practices

1. **Never commit credentials** to version control
2. **Use keychain for passwords/tokens** (macOS):
   ```python
   from cloudconduit import ConfigManager
   cm = ConfigManager()
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

### Configuration Priority Issues
1. Check configuration order: Function parameters > Environment variables > Keychain > config.yaml
2. Test priority system: 
   ```python
   from cloudconduit import ConfigManager
   cm = ConfigManager()
   print(cm.get_config_value('snowflake', 'account'))
   ```
3. Disable auto-config if needed: `export CLOUDCONDUIT_DISABLE_AUTO_CONFIG=1`

### Permission Issues (macOS)
1. VS Code may need keychain access permissions for credential retrieval
2. Grant access when prompted or manually in System Preferences
3. Test keychain access: `cm.get_config_value('snowflake', 'password', is_credential=True)`

This setup ensures your CloudConduit configuration system works seamlessly in VS Code with the new v0.2.0 priority system!