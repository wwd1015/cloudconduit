# CloudConduit Deployment Guide

This document provides instructions for deploying CloudConduit to internal artifactory and GitHub Enterprise.

## Prerequisites

### Internal Artifactory
- Access to your organization's internal PyPI repository/artifactory
- Valid credentials with publish permissions
- Repository URL and authentication details

### GitHub Enterprise
- GitHub Enterprise account with appropriate permissions
- Repository created and configured
- GitHub Actions enabled

## Environment Setup

### Required Environment Variables/Secrets

Set the following in your GitHub repository secrets:

```bash
# Artifactory Configuration
ARTIFACTORY_URL=https://your-company.artifactory.com/artifactory/api/pypi/pypi-local/
ARTIFACTORY_USERNAME=your-username
ARTIFACTORY_PASSWORD=your-api-token

# GitHub Configuration (if different from default)
GITHUB_TOKEN=ghp_your_github_token
```

### Local Development Setup

```bash
# Clone the repository
git clone https://github-enterprise.your-company.com/your-org/cloudconduit.git
cd cloudconduit

# Set up development environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements-dev.txt
pip install -e .
```

## Publishing Workflow

### Automated Publishing (Recommended)

1. **Create a Release**:
   ```bash
   # Tag the release
   git tag v0.1.0
   git push origin v0.1.0
   
   # Or create via GitHub UI
   # Go to Releases -> Create new release
   # Tag: v0.1.0, Title: "Release v0.1.0"
   ```

2. **GitHub Actions will automatically**:
   - Run full test suite
   - Build the package
   - Publish to internal artifactory
   - Create release assets

### Manual Publishing

If you need to publish manually:

```bash
# 1. Build the package
python -m build

# 2. Check the package
twine check dist/*

# 3. Configure .pypirc
cat > ~/.pypirc << EOF
[distutils]
index-servers = internal

[internal]
repository = https://your-company.artifactory.com/artifactory/api/pypi/pypi-local/
username = your-username
password = your-api-token
EOF

# 4. Upload to internal artifactory
twine upload --repository internal dist/*
```

## Installation from Internal Artifactory

Once published, users can install from your internal repository:

```bash
# Install from internal artifactory
pip install cloudconduit --extra-index-url https://your-company.artifactory.com/artifactory/api/pypi/pypi-local/simple/

# Or configure pip permanently
pip config set global.extra-index-url https://your-company.artifactory.com/artifactory/api/pypi/pypi-local/simple/
pip install cloudconduit
```

### pip.conf Configuration

Create `~/.pip/pip.conf` (Linux/Mac) or `%APPDATA%\pip\pip.ini` (Windows):

```ini
[global]
extra-index-url = https://your-company.artifactory.com/artifactory/api/pypi/pypi-local/simple/
trusted-host = your-company.artifactory.com
```

## Docker Deployment

For containerized environments:

```dockerfile
FROM python:3.12-slim

# Install CloudConduit from internal artifactory
RUN pip install cloudconduit --extra-index-url https://your-company.artifactory.com/artifactory/api/pypi/pypi-local/simple/

# Your application code
COPY . /app
WORKDIR /app

# Set up credentials via environment variables
ENV SNOWFLAKE_PASSWORD=${SNOWFLAKE_PASSWORD}
ENV DATABRICKS_ACCESS_TOKEN=${DATABRICKS_ACCESS_TOKEN}
ENV AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
ENV AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

CMD ["python", "your-app.py"]
```

## Version Management

### Semantic Versioning

CloudConduit follows [Semantic Versioning](https://semver.org/):

- **MAJOR.MINOR.PATCH** (e.g., 1.2.3)
- **Major**: Breaking changes
- **Minor**: New features, backward compatible
- **Patch**: Bug fixes, backward compatible

### Release Process

1. **Update Version**:
   ```bash
   # Update version in pyproject.toml
   sed -i 's/version = "0.1.0"/version = "0.1.1"/' pyproject.toml
   
   # Update __init__.py
   sed -i 's/__version__ = "0.1.0"/__version__ = "0.1.1"/' cloudconduit/__init__.py
   ```

2. **Update Changelog**:
   - Document changes in CHANGELOG.md
   - Follow [Keep a Changelog](https://keepachangelog.com/) format

3. **Commit and Tag**:
   ```bash
   git add .
   git commit -m "Bump version to 0.1.1"
   git tag v0.1.1
   git push origin main --tags
   ```

## Monitoring and Maintenance

### Health Checks

Create a simple health check script:

```python
#!/usr/bin/env python3
"""Health check for CloudConduit installation."""

try:
    import cloudconduit
    print(f"✓ CloudConduit v{cloudconduit.__version__} imported successfully")
    
    # Test basic functionality
    from cloudconduit import CredentialManager, CloudConduit
    cm = CredentialManager()
    cc = CloudConduit()
    print("✓ Core components initialized successfully")
    
except ImportError as e:
    print(f"✗ Import failed: {e}")
    exit(1)
except Exception as e:
    print(f"✗ Initialization failed: {e}")
    exit(1)

print("✓ CloudConduit health check passed")
```

### Dependency Updates

Regular maintenance schedule:

```bash
# Check for outdated dependencies
pip list --outdated

# Update requirements
pip-compile requirements.in
pip-compile requirements-dev.in

# Test with new dependencies
python -m pytest tests/
```

## Troubleshooting

### Common Issues

1. **Artifactory Authentication Failed**:
   ```bash
   # Check credentials
   curl -u username:password https://your-company.artifactory.com/artifactory/api/system/ping
   
   # Verify repository permissions
   curl -u username:password https://your-company.artifactory.com/artifactory/api/repositories
   ```

2. **Package Not Found After Publishing**:
   ```bash
   # Check if package exists in artifactory
   curl https://your-company.artifactory.com/artifactory/api/pypi/pypi-local/simple/cloudconduit/
   
   # Clear pip cache
   pip cache purge
   ```

3. **GitHub Actions Failing**:
   - Check repository secrets are set correctly
   - Verify GitHub Actions are enabled
   - Review workflow logs for specific errors

### Support

For deployment issues:
1. Check this documentation
2. Review GitHub Actions workflow logs
3. Contact your DevOps team
4. File issues in the repository

## Security Considerations

### Credential Management

1. **Never commit credentials** to version control
2. **Use environment variables** for sensitive data
3. **Rotate credentials** regularly
4. **Use keychain** for local development only

### Access Control

1. **Limit artifactory access** to authorized users
2. **Use GitHub branch protection** rules
3. **Enable two-factor authentication**
4. **Review and audit permissions** regularly

### Dependency Security

```bash
# Check for security vulnerabilities
pip audit

# Update dependencies regularly
pip-compile --upgrade requirements.in
```

This completes the deployment guide for CloudConduit to internal artifactory and GitHub Enterprise.