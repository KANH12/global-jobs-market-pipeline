import logging
import logging.config
import yaml
from pathlib import Path
from datetime import datetime, timezone

_LOGGER_INITIALIZED = False
LOG_BASE = Path("/home/jovyan/logs")

def get_logger(name: str):
    global _LOGGER_INITIALIZED
    BASE_DIR = Path(__file__).resolve().parents[1]
    config_path = BASE_DIR / "config" / "logging.yaml"
    
    if not Path(config_path).exists():
        raise FileNotFoundError(f"Logging config not found: {config_path}")
        
    if not _LOGGER_INITIALIZED:
        with open(config_path) as f:
            config = yaml.safe_load(f)

        logging.config.dictConfig(config)
        _LOGGER_INITIALIZED = True

    return logging.getLogger(name)

def get_job_logger(job_name: str, component: str):
    
    VALID_COMPONENTS = {"api", "bronze", "silver", "gold"}
    
    if component not in VALID_COMPONENTS:
        raise ValueError(f"Invalid component: {component}")
    
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")

    log_dir = LOG_BASE / component
    log_dir.mkdir(parents=True, exist_ok=True)

    log_file = log_dir / f"{today}.log"

    logger = logging.getLogger(f"{component}.{job_name}")
    logger.setLevel(logging.INFO)
    logger.propagate = False #Avoid duplicate logs

    if logger.handlers:
        for h in logger.handlers:
            if isinstance(h, logging.FileHandler):
                return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)

    return logger
