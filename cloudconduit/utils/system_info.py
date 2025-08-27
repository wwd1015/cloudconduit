"""System information utilities for CloudConduit."""

import os
import platform
import getpass
import socket
from typing import Optional


def get_current_user() -> str:
    """Get the current system username.
    
    This function attempts to get the username in order of preference:
    1. getpass.getuser() - Most reliable across platforms
    2. os.getlogin() - Login name from controlling terminal  
    3. Environment variables (USER, USERNAME, LOGNAME)
    4. Default fallback
    
    Returns:
        Current username as string
    """
    try:
        # Most reliable method across platforms
        return getpass.getuser()
    except Exception:
        pass
    
    try:
        # Try getting login name
        return os.getlogin()
    except Exception:
        pass
    
    # Try environment variables
    for env_var in ['USER', 'USERNAME', 'LOGNAME']:
        user = os.getenv(env_var)
        if user:
            return user
    
    # Last resort fallback
    return "unknown_user"


def get_computer_name() -> str:
    """Get the computer/hostname.
    
    Returns:
        Computer name/hostname as string
    """
    try:
        return socket.gethostname()
    except Exception:
        return "unknown_host"


def get_system_info() -> dict:
    """Get comprehensive system information.
    
    Returns:
        Dictionary with system information
    """
    return {
        "username": get_current_user(),
        "hostname": get_computer_name(),
        "platform": platform.system(),
        "platform_release": platform.release(),
        "platform_version": platform.version(),
        "architecture": platform.machine(),
        "python_version": platform.python_version(),
    }


def generate_default_user_id(service: Optional[str] = None, domain_suffix: Optional[str] = None) -> str:
    """Generate a default user ID for cloud services.
    
    This creates a user ID based on the current system user, optionally
    formatted for specific services or with domain suffixes.
    
    Args:
        service: Optional service name to include in the ID
        domain_suffix: Optional domain suffix (e.g., "@company.com")
        
    Returns:
        Generated user ID string
        
    Examples:
        >>> generate_default_user_id()
        'john.doe'
        >>> generate_default_user_id("snowflake")
        'john.doe'
        >>> generate_default_user_id("snowflake", "@company.com")
        'john.doe@company.com'
        >>> generate_default_user_id(domain_suffix="@myorg.com")
        'john.doe@myorg.com'
    """
    # Get base username
    username = get_current_user()
    
    # Clean up username (remove domain if present, normalize)
    if "@" in username:
        username = username.split("@")[0]
    
    # Convert to lowercase and replace spaces with dots
    username = username.lower().replace(" ", ".")
    
    # Add domain suffix if provided
    if domain_suffix:
        if not domain_suffix.startswith("@"):
            domain_suffix = f"@{domain_suffix}"
        username = f"{username}{domain_suffix}"
    
    return username


def get_default_snowflake_user(domain_suffix: Optional[str] = None) -> str:
    """Get default Snowflake username based on system user.
    
    Args:
        domain_suffix: Optional domain suffix for the username
        
    Returns:
        Default Snowflake username
        
    Examples:
        >>> get_default_snowflake_user()
        'john.doe'
        >>> get_default_snowflake_user("@company.com")
        'john.doe@company.com'
    """
    return generate_default_user_id("snowflake", domain_suffix)


def format_keychain_account(service: str, username: Optional[str] = None) -> str:
    """Format account name for keychain storage.
    
    Creates a consistent naming scheme for storing multiple service
    credentials in keychain.
    
    Args:
        service: Service name (e.g., 'snowflake', 'databricks')
        username: Optional username, defaults to current user
        
    Returns:
        Formatted account name for keychain
        
    Examples:
        >>> format_keychain_account("snowflake")
        'snowflake.john.doe'
        >>> format_keychain_account("snowflake", "custom.user")
        'snowflake.custom.user'
        >>> format_keychain_account("databricks")
        'databricks.john.doe'
    """
    if username is None:
        username = get_current_user()
    
    # Clean username for keychain compatibility
    username = username.lower().replace(" ", ".").replace("@", ".")
    
    return f"{service}.{username}"