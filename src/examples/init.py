import sys
import os

# Get the absolute path to the project root (assuming dummy_pipeline.py is in a subdirectory)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# Add the project root to sys.path if it's not already there
if project_root not in sys.path:
    sys.path.insert(0, project_root)