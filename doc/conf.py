# Ensure we get the local copy of mySQLdbConnector instead of what's on the standard path
import os
import sys
sys.path.insert(0, os.path.abspath(".."))
import mySQLdbConnector

master_doc = "index"

project = "mySQLdbConnector"
copyright = "Adrian Toader"

version = release = mySQLdbConnector.version

extensions = ["sphinx.ext.autodoc", "sphinx.ext.coverage", "sphinx.ext.viewcode"]

primary_domain = 'py'
default_role = 'py:obj'

autodoc_member_order = "bysource"
autoclass_content = "both"

coverage_skip_undoc_in_source = True
