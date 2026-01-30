import sys
sys.path.insert(0, '.')

from src.utils.logger import get_logger
from src.utils.config import ConfigManager
from src.ingestion.load_data import DataIngestion

logger = get_logger()
config = ConfigManager('configs/data_config.yaml')
ingestion = DataIngestion(logger, config)
ingestion.execute()
