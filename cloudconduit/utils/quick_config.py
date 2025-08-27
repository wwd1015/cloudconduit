"""Quick configuration loader for lightweight imports."""

import os
from .config_helper import push_config_to_env


def load_config():
    """Load CloudConduit configuration without importing full package.
    
    This is a lightweight alternative to importing the full cloudconduit package
    when you only need to load configuration.
    
    Examples:
        # Lightweight config loading
        from cloudconduit.utils.quick_config import load_config
        load_config()
        
        # Or use individual connectors directly
        from cloudconduit.connectors.snowflake import SnowflakeConnector
        sf = SnowflakeConnector()
    """
    if not os.getenv("CLOUDCONDUIT_DISABLE_AUTO_CONFIG"):
        try:
            push_config_to_env()
            print("✓ CloudConduit configuration loaded")
        except Exception as e:
            print(f"⚠ Configuration loading failed: {e}")
    else:
        print("ℹ Auto-configuration disabled")


# Convenience function
ensure_config = load_config  # Alias for backwards compatibility