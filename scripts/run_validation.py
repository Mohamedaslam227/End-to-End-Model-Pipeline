import sys
sys.path.insert(0, '.')

from src.utils.logger import get_logger
from src.utils.config import ConfigManager
from src.validation.data_validation import DataValidation

logger = get_logger()
config = ConfigManager('configs/data_config.yaml')
validation = DataValidation(logger, config)
is_valid = validation.execute()
sys.exit(0 if is_valid else 1)
