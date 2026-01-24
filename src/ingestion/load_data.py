import pandas as pd
from pathlib import Path
from src.utils.logger import setup_logger
from src.utils.config import load_config

logger = setup_logger()
config = load_config("configs/data_config.yaml")

def import_data(path:str,save_path:str):
    try:
        data = pd.read_csv(path)
        logger.info("Data imported successfully")
        logger.info(f"Data shape: {data.head(5)}")
        save_path=Path(save_path)
        save_path.parent.mkdir(parents=True,exist_ok=True)
        data.to_csv(save_path,index=False)
        logger.info(f"Data saved to {save_path}")
    except Exception as e:
        logger.error(f"Error importing data: {e}")

if __name__ == "__main__":
    import_data(f"csv_files/Telco_Customer_Churn.csv",config["dataset"]["data_path"])