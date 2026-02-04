import logging
import logging.handlers
from pathlib import Path
from src.utils import TOP_DIR
from colored import fg, attr
import sys


class ColoredFormatter(logging.Formatter):
    """Custom formatter with colored log levels"""

    COLORS = {
        'DEBUG': fg('cyan'),
        'INFO': fg('green'),
        'WARNING': fg('yellow'),
        'ERROR': fg('red'),
        'CRITICAL': fg('magenta')
    }
    RESET = attr('reset')

    def format(self, record):
        # Store original levelname
        levelname = record.levelname
        log_color = self.COLORS.get(levelname, self.RESET)
        # Only color the levelname, not the whole message
        record.levelname = f'{log_color}{levelname}{self.RESET}'
        formatted = super().format(record)
        # Restore original levelname to avoid side effects
        record.levelname = levelname
        return formatted


def setup_logger(name: str, log_dir: Path = None) -> logging.Logger:
    """
    Configure logger with console and file handlers.

    Args:
        name: Logger name
        log_dir: Directory to save log files (defaults to DATA_DIR)

    Returns:
        Configured logger instance
    """
    log_dir = log_dir or (TOP_DIR / 'logs')
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    fmt = '%(asctime)s: %(name)s - %(levelname)s -> %(message)s'
    datefmt = '%Y-%m-%d %H:%M:%S'

    # Console handler with colors
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = ColoredFormatter(fmt, datefmt=datefmt)
    console_handler.setFormatter(console_formatter)

    # File handler
    file_handler = logging.handlers.RotatingFileHandler(
        log_dir / f'{name}.log',
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(fmt, datefmt=datefmt)
    file_handler.setFormatter(file_formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger
