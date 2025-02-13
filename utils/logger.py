import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

class ErrorFormatter:
    @staticmethod
    def format_error(error_id: str, error: str, traceback: str = None) -> str:
        """Format error message with ID and optional traceback."""
        sections = [
            "=" * 80,
            f"Error ID: {error_id}",
            f"Error: {error}"
        ]
        
        if traceback:
            sections.extend([
                "Traceback:",
                "  " + "\n  ".join(traceback.split("\n"))  # Indent traceback
            ])
            
        sections.append("=" * 80)
        return "\n".join(sections)

class CrawlerLogger:
    def __init__(self, name="crawler"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        
        # Create logs directory if it doesn't exist
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        
        # Create handlers
        # Console handler with detailed formatting
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        
        # File handler with rotation
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        log_file = log_dir / f"crawler_{today}.log"
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.DEBUG)
        
        # Create formatters
        # Simple format for console
        console_format = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] %(message)s',
            '%Y-%m-%d %H:%M:%S'
        )
        
        # Detailed format for file
        file_format = logging.Formatter(
            '[%(asctime)s] [%(levelname)s] [%(name)s] [%(filename)s:%(lineno)d] %(message)s',
            '%Y-%m-%d %H:%M:%S'
        )
        
        # Set formatters
        console_handler.setFormatter(console_format)
        file_handler.setFormatter(file_format)
        
        # Add handlers to the logger
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        
        # Prevent propagation to root logger
        self.logger.propagate = False
        
        # Log initialization
        self.info("[INIT] Logger initialized", logger_name=name)
    
    def _generate_error_id(self) -> str:
        """Generate a unique error ID with timestamp."""
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        return f"{timestamp}_{unique_id}"

    def _format_message(self, message: str, **kwargs) -> str:
        """Format message with additional context."""
        if kwargs:
            # Filter out traceback from kwargs for cleaner output
            clean_kwargs = {k: v for k, v in kwargs.items() if k != 'traceback'}
            context = " | ".join(f"{k}: {v}" for k, v in clean_kwargs.items())
            return f"{message} | {context}" if context else message
        return message

    def debug(self, message: str, **kwargs):
        """Log debug level message with context."""
        self.logger.debug(self._format_message(message, **kwargs))
    
    def info(self, message: str, **kwargs):
        """Log info level message with context."""
        self.logger.info(self._format_message(message, **kwargs))
    
    def warning(self, message: str, **kwargs):
        """Log warning level message with context."""
        self.logger.warning(self._format_message(message, **kwargs))
    
    def error(self, message: str, **kwargs):
        """Log error level message with context and stack trace."""
        error_id = kwargs.pop('error_id', self._generate_error_id())
        exc_info = kwargs.pop('exc_info', True)
        error = kwargs.get('error', '')
        traceback = kwargs.get('traceback', '')
        
        formatted_error = ErrorFormatter.format_error(error_id, error, traceback)
        # Remove traceback from kwargs to avoid duplication
        kwargs.pop('traceback', None)
        
        self.logger.error(
            self._format_message(f"{message}\n{formatted_error}", error_id=error_id, **kwargs),
            exc_info=exc_info if not traceback else False  # Only include exc_info if no traceback provided
        )
        return error_id
    
    def critical(self, message: str, **kwargs):
        """Log critical level message with context and stack trace."""
        error_id = kwargs.pop('error_id', self._generate_error_id())
        exc_info = kwargs.pop('exc_info', True)
        error = kwargs.get('error', '')
        traceback = kwargs.get('traceback', '')
        
        formatted_error = ErrorFormatter.format_error(error_id, error, traceback)
        # Remove traceback from kwargs to avoid duplication
        kwargs.pop('traceback', None)
        
        self.logger.critical(
            self._format_message(f"{message}\n{formatted_error}", error_id=error_id, **kwargs),
            exc_info=exc_info if not traceback else False  # Only include exc_info if no traceback provided
        )
        return error_id

    def log_fetch(self, url: str, status: str, time: float):
        """Log fetch operation."""
        self.info(f"[FETCH] {url}", status=status, time=f"{time:.2f}s")
    
    def log_scrape(self, url: str, time: float):
        """Log scrape operation."""
        self.info(f"[SCRAPE] {url}", time=f"{time:.2f}s")
    
    def log_extract(self, url: str, venues_count: int, time: float):
        """Log extraction operation."""
        self.info(
            f"[EXTRACT] {url}",
            venues_count=venues_count,
            time=f"{time:.2f}s"
        )
    
    def log_error(self, operation: str, url: str, error: Exception):
        """Log error with operation context and full traceback."""
        error_id = self._generate_error_id()
        self.error(
            f"[{operation}] {url}",
            error_id=error_id,
            error=str(error),
            traceback=traceback.format_exc()
        )
        return error_id
