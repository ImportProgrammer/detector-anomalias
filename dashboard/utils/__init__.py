"""
Utilidades del dashboard
"""

from .db import get_connection, get_engine, execute_query, execute_query_dict, test_connection
from .queries import *

__all__ = [
    'get_connection',
    'get_engine', 
    'execute_query',
    'execute_query_dict',
    'test_connection'
]