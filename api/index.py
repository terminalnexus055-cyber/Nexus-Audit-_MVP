# api/index.py
import sys
import os

# Make sure the project root (one level up) is in the Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from mangum import Mangum
from backend.main import app   # now safely importable

handler = Mangum(app)
