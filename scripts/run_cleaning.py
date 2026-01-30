import sys
sys.path.insert(0, '.')

from src.utils.logger import get_logger
from src.utils.config import ConfigManager
from src.processings.data_cleanup import DataCleaning

logger = get_logger()
config = ConfigManager('configs/data_config.yaml')
cleaning = DataCleaning(logger, config)
cleaning.execute()
