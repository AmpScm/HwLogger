"""
HomeWizard P1 Logger - Async websocket listener with REST API.

A modern async Python package for logging energy consumption from
HomeWizard P1 devices via APIv2 websocket, with weighted averages
and minimalistic REST API.
"""

__version__ = "2.0.0"
__author__ = "Your Name"

from .config import Config
from .db import Database
from .prices import PriceFetcher

__all__ = ["Config", "Database", "PriceFetcher", "__version__"]
