import sys
sys.path.insert(0, '.')

from src.utils.logger import get_logger
from src.utils.config import ConfigManager
from src.processings.data_split import DataSplitter

logger = get_logger()
config = ConfigManager('configs/data_config.yaml')
splitter = DataSplitter(logger, config)
splitter.execute()
