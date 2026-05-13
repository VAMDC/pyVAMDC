"""
Centralized logging configuration for pyVAMDC.

This module provides a global logging system with configurable verbosity levels
to control error output depth, making it suitable for both interactive use and
AI agent consumption.
"""

import os
import sys
import logging
import traceback
from enum import Enum
from typing import Optional


class LogLevel(Enum):
    """
    Verbosity levels for logging output.
    
    SILENT: No output except results (errors suppressed)
    MINIMAL: One-line error summaries only
    NORMAL: Standard error messages (default)
    VERBOSE: Detailed messages with context
    DEBUG: Full tracebacks and debug information
    """
    SILENT = 0
    MINIMAL = 1
    NORMAL = 2
    VERBOSE = 3
    DEBUG = 4


# Global log level storage
_log_level_env = os.environ.get('VAMDC_LOG_LEVEL', 'NORMAL').upper()
_current_log_level = LogLevel[_log_level_env] if _log_level_env in LogLevel.__members__ else LogLevel.NORMAL


def get_log_level() -> LogLevel:
    """
    Get the current global log level.
    
    Returns:
        LogLevel: Current log level
    """
    return _current_log_level


def set_log_level(level: LogLevel):
    """
    Set the global log level programmatically.
    
    Args:
        level: LogLevel enum value to set
    """
    global _current_log_level
    _current_log_level = level


class SmartLogger:
    """
    Logger that respects verbosity levels and provides context-aware error formatting.
    
    This logger adapts its output based on the global log level, from completely silent
    to full debug tracebacks, making it suitable for both human users and AI agents.
    """
    
    def __init__(self, name: str):
        """
        Initialize the SmartLogger.
        
        Args:
            name: Logger name (typically __name__ of the module)
        """
        self.logger = logging.getLogger(name)
        self.name = name
    
    def error(self, message: str, exception: Optional[Exception] = None, show_traceback: bool = True):
        """
        Log an error with verbosity-aware formatting.
        
        Args:
            message: Error message to log
            exception: Optional exception object for context
            show_traceback: Whether to show full traceback at DEBUG level
        """
        level = get_log_level()
        
        if level == LogLevel.SILENT:
            return  # No output
        
        elif level == LogLevel.MINIMAL:
            # One-line summary only to stderr
            if exception:
                sys.stderr.write(f"Error: {message}: {type(exception).__name__}\n")
            else:
                sys.stderr.write(f"Error: {message}\n")
        
        elif level == LogLevel.NORMAL:
            # Standard error message
            self.logger.error(message)
            if exception:
                self.logger.error(f"  {type(exception).__name__}: {str(exception)}")
        
        elif level == LogLevel.VERBOSE:
            # Detailed context
            self.logger.error(f"Error in {self.name}: {message}")
            if exception:
                self.logger.error(f"  Exception type: {type(exception).__name__}")
                self.logger.error(f"  Exception message: {str(exception)}")
        
        elif level == LogLevel.DEBUG:
            # Full traceback
            self.logger.error(f"Error in {self.name}: {message}")
            if exception and show_traceback:
                self.logger.error("Traceback:")
                traceback.print_exception(type(exception), exception, exception.__traceback__)
    
    def warning(self, message: str):
        """
        Log a warning respecting verbosity level.
        
        Args:
            message: Warning message to log
        """
        level = get_log_level()
        if level not in (LogLevel.SILENT, LogLevel.MINIMAL):
            self.logger.warning(message)
    
    def info(self, message: str):
        """
        Log an info message (only shown in VERBOSE and DEBUG modes).
        
        Args:
            message: Info message to log
        """
        level = get_log_level()
        if level in (LogLevel.VERBOSE, LogLevel.DEBUG):
            self.logger.info(message)
    
    def debug(self, message: str):
        """
        Log a debug message (only shown in DEBUG mode).
        
        Args:
            message: Debug message to log
        """
        if get_log_level() == LogLevel.DEBUG:
            self.logger.debug(message)


def get_logger(name: str) -> SmartLogger:
    """
    Get a SmartLogger instance for the given module.
    
    Args:
        name: Logger name (typically __name__ of the module)
    
    Returns:
        SmartLogger: Configured logger instance
    """
    return SmartLogger(name)


def configure_python_logging():
    """
    Configure Python's logging module based on current log level.
    Should be called after set_log_level() to update logging handlers.
    """
    level = get_log_level()
    
    if level == LogLevel.DEBUG:
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    elif level in (LogLevel.VERBOSE, LogLevel.NORMAL):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    elif level == LogLevel.MINIMAL:
        logging.basicConfig(
            level=logging.WARNING,
            format='%(levelname)s - %(message)s'
        )
    else:  # SILENT
        logging.basicConfig(
            level=logging.CRITICAL,
            format='%(levelname)s - %(message)s'
        )
