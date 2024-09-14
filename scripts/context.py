"""
Path modification to resolve package name

add `from .context import nhs` inside test files
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import nhs
import nhs.data
