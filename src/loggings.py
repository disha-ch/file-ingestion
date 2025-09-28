"""Singleton logger used across the pipeline with optional color support."""
import logging
import os
import sys
import threading

class Colors:
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    CYAN = "\033[96m"
    RESET = "\033[0m"

def supports_color() -> bool:
    return (hasattr(sys.stdout, "isatty") and sys.stdout.isatty() and "NO_COLOR" not in os.environ)

class ColoredFormatter(logging.Formatter):
    COLOR_MAP = {
        logging.DEBUG: Colors.CYAN,
        logging.INFO: Colors.GREEN,
        logging.WARNING: Colors.YELLOW,
        logging.ERROR: Colors.RED,
    }

    def __init__(self, fmt=None, datefmt=None, use_color=True):
        super().__init__(fmt, datefmt)
        self.use_color = use_color

    def format(self, record):
        experiment_id = os.getenv("experiment_id")
        record.experiment_id = f"[{experiment_id}]" if experiment_id else ""
        if self.use_color:
            color = self.COLOR_MAP.get(record.levelno, Colors.CYAN)
            record.levelname = f"{color}{record.levelname}{Colors.RESET}"
            record.msg = f"{color}{record.msg}{Colors.RESET}"
        return super().format(record)

class SingletonLogger:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, level=logging.INFO):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize(level)
        return cls._instance

    def _initialize(self, level=logging.INFO):
        self.logger = logging.getLogger("PipelineLogger")
        self.logger.setLevel(level)
        self.logger.propagate = False
        for handler in list(self.logger.handlers):
            self.logger.removeHandler(handler)
        fmt = "%(asctime)s - %(experiment_id)s %(module)s.%(funcName)s - %(levelname)s - %(message)s"
        color_formatter = ColoredFormatter(fmt=fmt, use_color=supports_color())
        plain_formatter = logging.Formatter(fmt=fmt)
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(color_formatter)
        self.logger.addHandler(console_handler)
        file_handler = logging.FileHandler("file.log", mode="a", encoding="utf-8")
        file_handler.setFormatter(plain_formatter)
        self.logger.addHandler(file_handler)

    def get_logger(self):
        return self.logger
