"""
Complete ML Pipeline Flow

This script executes the entire pipeline:
1. Data Ingestion: Loads raw data from source to raw path.
2. Data Validation: Ensures quality of raw data using GX.
3. Data Cleaning: Processes raw data and saves to processed path.
4. Data Splitting: Splits processed data into Train/Test/Val sets.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent))

from src.utils.logger import get_logger
from src.utils.config import ConfigManager
from src.ingestion.load_data import DataIngestion
from src.validation.data_validation import DataValidation
from src.processings.data_cleanup import DataCleaning
from src.processings.data_split import DataSplitter

def main():
    # 1. Initialize Dependencies
    logger = get_logger()
    config = ConfigManager("configs/data_config.yaml")
    
    logger.info("Starting End-to-End ML Pipeline")
    
    try:
        ingestion = DataIngestion(logger, config)
        ingestion.execute()
        
        validation = DataValidation(logger, config)
        is_valid = validation.execute()
        
        if not is_valid:
            logger.error("Data validation failed. Stopping pipeline.")
            return

        cleaning = DataCleaning(logger, config)
        cleaning.execute()
        
        splitter = DataSplitter(logger, config)
        splitter.execute()
        
        logger.info("ML Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
