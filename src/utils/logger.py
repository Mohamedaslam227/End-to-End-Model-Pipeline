from loguru import logger
import sys
import time

def setup_logger():
    logger.remove(handler_id=None)
    logger.add(sys.stdout, format="{time} | {level} | {message}", level="INFO")
    logger.add(f"logs/debug/debug_pipeline.log_{time.strftime('%Y-%m-%d_%H-%M-%S')}", format="{time} | {level} | {message}", level="DEBUG")
    logger.add(f"logs/info/info_pipeline.log_{time.strftime('%Y-%m-%d_%H-%M-%S')}", format="{time} | {level} | {message}", level="INFO")
    return logger
