"""
Run this file to start the pipeline.
Usage: python run.py
"""
import os
import sys

# Set the project root
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)
sys.path.insert(0, project_root)

# Now run the pipeline
from src.pipeline.main import run_pipeline

run_pipeline(source="csv")
