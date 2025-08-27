# Quick Start Guide

This guide will help you get started with CloudConduit quickly.

## Basic Usage

### Individual Connectors

```python
from cloudconduit import connect_snowflake, connect_databricks, connect_s3
import pandas as pd

# Connect to Snowflake
sf = connect_snowflake("your-account", "your-username")
result = sf.execute("SELECT CURRENT_USER(), CURRENT_ROLE()")
print(result)

# Connect to Databricks  
db = connect_databricks()
tables = db.list_tables()
print(tables)

# Connect to S3
s3 = connect_s3()
buckets = s3.list_buckets()
print(buckets)
```

### Unified Interface

```python
from cloudconduit import CloudConduit
import pandas as pd

# Create unified interface
cc = CloudConduit()

# Access connectors
sf = cc.snowflake("account", "username")
db = cc.databricks()
s3 = cc.s3()

# Upload DataFrame to Snowflake
df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
sf.upload_df(df, "my_table")

# Execute query
result = sf.execute("SELECT * FROM my_table")
print(result)
```

### Context Managers

```python
from cloudconduit import SnowflakeConnector

with SnowflakeConnector("account", "username") as sf:
    result = sf.execute("SELECT * FROM my_table")
    sf.copy_table("source_table", "backup_table")
```

## Common Operations

### DataFrame Operations

```python
import pandas as pd

# Create sample data
df = pd.DataFrame({
    'id': [1, 2, 3, 4],
    'name': ['Alice', 'Bob', 'Charlie', 'Diana'],
    'value': [100.5, 200.0, 300.25, 400.75]
})

# Upload to Snowflake
with connect_snowflake("account", "username") as sf:
    sf.upload_df(df, "users", if_exists="replace")

# Upload to S3
with connect_s3() as s3:
    s3.upload_df(df, "my-bucket", "data/users.csv", file_format="csv")
    
# Download from S3
downloaded_df = s3.download_df("my-bucket", "data/users.csv", file_format="csv")
```

### Table Management

```python
# Copy table
sf.copy_table("source_table", "backup_table", if_exists="replace")

# Drop table
sf.drop_table("old_table")

# Grant access
sf.grant_access("my_table", "data_team_role", "SELECT,INSERT")
```

### Querying Data

```python
# Simple query
result = sf.execute("SELECT * FROM users WHERE value > 200")

# Query with parameters (use connector-specific syntax)
result = sf.execute("""
    SELECT name, value 
    FROM users 
    WHERE created_date >= '2023-01-01'
    ORDER BY value DESC
""")
```

## Error Handling

```python
from cloudconduit import connect_snowflake

try:
    sf = connect_snowflake("account", "username")
    result = sf.execute("SELECT * FROM non_existent_table")
except ConnectionError as e:
    print(f"Connection failed: {e}")
except RuntimeError as e:
    print(f"Query failed: {e}")
finally:
    sf.disconnect()
```

## Next Steps

- Learn about [credential configuration](configuration.md)
- Explore the [API reference](api_reference.rst)
- Check out more [examples](examples.md)
- Run the [test suite](testing.md)