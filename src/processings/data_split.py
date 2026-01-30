import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
from src.utils.base import DataComponent
from src.utils.config import ConfigManager
from src.utils.logger import LoggerConfig
from sklearn.model_selection import train_test_split
from src.ingestion.load_data import DataSourceFactory

class DataSplitter(DataComponent):
    """
    Splits processed data into Train, Test, and Validation sets.
    """
    def __init__(self, logger: LoggerConfig, config: ConfigManager):
        super().__init__(logger, config)
        # In your yaml data_path: data/raw and processed_data_path: data/processed
        self.raw_data_dir = self.get_data_path("dataset.data_path")
        self.processed_data_dir = self.get_data_path("dataset.processed_data_path")
    
    def execute(
        self,
        source_file_name: Optional[str] = None,
        source_type: Optional[str] = None,
        save_path: Optional[str] = None,
        read_kwargs: Optional[Dict[str, Any]] = None
    ) -> None:
        try:
            self.log_execution_start("DataSplitter")
            
            if source_file_name:
                source_file_path = self.processed_data_dir / source_file_name
            else:
                dataset_name = self.get_config("dataset.name", "data")
                source_file_path = self.processed_data_dir / dataset_name if Path(dataset_name).suffix else self.processed_data_dir / f"{dataset_name}.csv"
            
            self.train_path = Path(self.create_directory(str(self.processed_data_dir / "train")))
            self.test_path = Path(self.create_directory(str(self.processed_data_dir / "test")))
            self.val_path = Path(self.create_directory(str(self.processed_data_dir / "val")))
            
            if source_type:
                self.data_source, self.ext = DataSourceFactory.create_data_source(source_type)
            else:
                self.data_source, self.ext = DataSourceFactory.create_from_path(str(source_file_path))
            
            self.logger.info(f"Loading data from {source_file_path}...")
            kwargs = read_kwargs or {}
            data = self.data_source.read(str(source_file_path), **kwargs)
            self._log_data_info(data)
            
            train, test = train_test_split(data, test_size=0.2, random_state=42)
            train, val = train_test_split(train, test_size=0.2, random_state=42)
            
            self.logger.info(f"✓ Data split successful. Train: {len(train)}, Test: {len(test)}, Val: {len(val)}")
            
            base_name = source_file_path.stem
           
            self._save_data(train, Path(self.train_path) / f"{base_name}_train.{self.ext}")
            self._save_data(test, Path(self.test_path) / f"{base_name}_test.{self.ext}")
            self._save_data(val, Path(self.val_path) / f"{base_name}_val.{self.ext}")


            self.config_manager.update("dataset.train_data_path", str(self.train_path / f"{base_name}_train.{self.ext}"))
            self.config_manager.update("dataset.test_data_path", str(self.test_path / f"{base_name}_test.{self.ext}"))
            self.config_manager.update("dataset.val_data_path", str(self.val_path / f"{base_name}_val.{self.ext}"))
            
            self.log_execution_end("DataSplitter", success=True)
            
        except Exception as e:
            self.logger.error(f"Error in DataSplitter: {str(e)}")
            self.log_execution_end("DataSplitter", success=False)
            raise

    def _log_data_info(self, data: pd.DataFrame) -> None:
        self.logger.info(f"  Shape: {data.shape}")
        self.logger.info(f"  Memory: {data.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    def _save_data(self, data: pd.DataFrame, save_path: Path) -> None:
        save_path.parent.mkdir(parents=True, exist_ok=True)
        extension = save_path.suffix.lower()
        if extension == '.csv':
            data.to_csv(save_path, index=False)
        elif extension in ['.xlsx', '.xls']:
            data.to_excel(save_path, index=False)
        elif extension == '.parquet':
            data.to_parquet(save_path, index=False)
        else:
            data.to_csv(save_path, index=False)
        self.logger.info(f"✓ Saved: {save_path.name}")