"""Sphinx configuration for CloudConduit documentation."""

import os
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Project information
project = 'CloudConduit'
copyright = '2024, CloudConduit Contributors'
author = 'CloudConduit Contributors'

# The full version, including alpha/beta/rc tags
try:
    from cloudconduit import __version__
    release = __version__
    version = '.'.join(__version__.split('.')[:2])
except ImportError:
    release = '0.1.0'
    version = '0.1'

# Extensions
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.doctest',
    'sphinx.ext.intersphinx',
    'sphinx.ext.napoleon',
    'sphinx.ext.viewcode',
    'sphinx_autodoc_typehints',
    'myst_parser',
]

# Source file suffixes
source_suffix = {
    '.rst': None,
    '.md': None,
}

# The master toctree document
master_doc = 'index'

# Language
language = 'en'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

# HTML theme options
html_theme = 'sphinx_rtd_theme'
html_theme_options = {
    'canonical_url': '',
    'analytics_id': '',
    'display_version': True,
    'prev_next_buttons_location': 'bottom',
    'style_external_links': False,
    'vcs_pageview_mode': '',
    'style_nav_header_background': '#2980B9',
    'collapse_navigation': False,
    'sticky_navigation': True,
    'navigation_depth': 4,
    'includehidden': True,
    'titles_only': False
}

# Static files (CSS, JavaScript, images)
html_static_path = ['_static']

# Custom CSS/JS files
html_css_files = []
html_js_files = []

# HTML context
html_context = {
    'display_github': True,
    'github_user': 'yourusername',
    'github_repo': 'cloudconduit',
    'github_version': 'main',
    'conf_py_path': '/docs/source/',
}

# Autodoc settings
autodoc_default_options = {
    'members': True,
    'member-order': 'bysource',
    'special-members': '__init__',
    'undoc-members': True,
    'exclude-members': '__weakref__'
}

# Napoleon settings (for Google/NumPy style docstrings)
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = False
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = True
napoleon_preprocess_types = False
napoleon_type_aliases = None
napoleon_attr_annotations = True

# Intersphinx mapping
intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'pandas': ('https://pandas.pydata.org/pandas-docs/stable/', None),
    'boto3': ('https://boto3.amazonaws.com/v1/documentation/api/latest/', None),
}

# Auto-generate stub files
autosummary_generate = True

# Type hints configuration
typehints_use_signature = True
typehints_use_signature_return = True
always_document_param_types = True
typehints_document_rtype = True

# MyST parser settings
myst_enable_extensions = [
    "deflist",
    "tasklist",
    "colon_fence",
    "linkify",
    "substitution",
]

# Suppress warnings
suppress_warnings = ['image.nonlocal_uri']