import logging
import logging.config
from src.config.settings import LOG_CONFIG

def setup_logging():
    """Configura o logging para a aplicação."""
    logging.config.dictConfig(LOG_CONFIG) 