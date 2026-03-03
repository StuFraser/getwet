"""
cache/__init__.py — Cache backend factory.

Reads the CACHE_BACKEND environment variable and returns the appropriate
backend instance. Defaults to 'memory' if not set.

Usage:
    from cache import get_cache
    cache = get_cache()

Supported values for CACHE_BACKEND:
    memory  — in-memory dict, lost on restart (default)
"""

import logging
import os

from cache.base import BaseTileCache

logger = logging.getLogger(__name__)


def get_cache() -> BaseTileCache:
    backend = os.getenv("CACHE_BACKEND", "memory").lower().strip()

    if backend == "memory":
        from cache.memory import MemoryTileCache
        logger.info("Using memory cache backend")
        return MemoryTileCache()

    logger.warning(
        "Unknown CACHE_BACKEND value '%s' — falling back to memory cache", backend
    )
    from cache.memory import MemoryTileCache
    return MemoryTileCache()