CloudConduit Documentation
=========================

Welcome to CloudConduit's documentation! CloudConduit is a unified Python API for connecting to Snowflake, Databricks, and S3 with comprehensive credential management.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   quickstart
   installation
   configuration
   api_reference
   examples
   testing
   contributing

Features
--------

* **Unified Interface**: Single API to work with Snowflake, Databricks, and S3
* **Credential Management**: Automatic credential retrieval from macOS keychain and environment variables
* **SSO Support**: Snowflake SSO authentication support
* **DataFrame Operations**: Easy pandas DataFrame upload/download
* **Common Operations**: Execute queries, copy tables, drop tables, grant access
* **Context Managers**: Clean connection management with ``with`` statements
* **Type Hints**: Full type annotation support

Quick Start
-----------

.. code-block:: python

   from cloudconduit import connect_snowflake, connect_databricks, connect_s3
   import pandas as pd

   # Connect to services
   sf = connect_snowflake("your-account", "your-username")
   db = connect_databricks()
   s3 = connect_s3()

   # Upload DataFrame
   df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})
   sf.upload_df(df, "my_table")

   # Execute query
   result = sf.execute("SELECT * FROM my_table")

Installation
------------

.. code-block:: bash

   pip install cloudconduit

For development:

.. code-block:: bash

   pip install cloudconduit[dev]

For documentation:

.. code-block:: bash

   pip install cloudconduit[docs]

API Reference
=============

.. autosummary::
   :toctree: _autosummary
   :recursive:

   cloudconduit

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`