"""Simple python client for extracting data from the Dune Analytics API"""

from ._extract import query, async_query

__version__ = '0.1.3'

__all__ = ['query', 'async_query']
