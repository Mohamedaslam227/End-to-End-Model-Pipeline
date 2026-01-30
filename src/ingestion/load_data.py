import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any
from abc import ABC, abstractmethod
from src.utils.base import DataComponent

class DataSource(ABC):
    @abstractmethod
    def read(self, source_path: str, **kwargs) -> pd.DataFrame:
        pass

class CSVDataSource(DataSource):
    def read(self, source_path: str, **kwargs) -> pd.DataFrame:
        return pd.read_csv(source_path, **kwargs)

class ExcelDataSource(DataSource):
    def read(self, source_path: str, **kwargs) -> pd.DataFrame:
        return pd.read_excel(source_path, **kwargs)

class ParquetDataSource(DataSource):
    def read(self, source_path: str, **kwargs) -> pd.DataFrame:
        return pd.read_parquet(source_path, **kwargs)

class DataSourceFactory:
    @staticmethod
    def create_data_source(source_type: str) -> DataSource:
        sources = {
            'csv': CSVDataSource(),
            'excel': ExcelDataSource(),
            'xlsx': ExcelDataSource(),
            'xls': ExcelDataSource(),
            'parquet': ParquetDataSource(),
        }
        source_type = source_type.lower()
        if source_type not in sources:
            raise ValueError(f"Unsupported data source type: {source_type}")
        
        return sources[source_type], source_type
    
    @staticmethod
    def create_from_path(file_path: str) -> tuple[DataSource, str]:
        extension = Path(file_path).suffix[1:]
        return DataSourceFactory.create_data_source(extension)
    
    

class DataIngestion(DataComponent):
    """Handles data loading from sources using Strategy and Factory patterns."""
    def __init__(self, logger, config_manager, data_source: Optional[DataSource] = None):
        super().__init__(logger, config_manager)
        self.data_source = data_source
    
    def execute(
        self,
        source_path: Optional[str] = None,
        save_path: Optional[str] = None,
        source_type: Optional[str] = None,
        **read_kwargs
    ) -> pd.DataFrame:
        self.log_execution_start("Data Ingestion")
        try:
            if source_path is None:
                source_path = self.get_config("dataset.source_path")
            
            source_path_obj = self.validate_path(source_path, must_exist=True)
            
            if save_path is None:
                save_dir = self.get_data_path("dataset.data_path")
                dataset_name = self.get_config("dataset.name", "data")
                # Avoid double extension if dataset_name already has one
                save_path = save_dir / dataset_name if Path(dataset_name).suffix else save_dir / f"{dataset_name}.csv"
            else:
                save_path = Path(save_path)
            
            if self.data_source is None:
                if source_type:
                    self.data_source, _ = DataSourceFactory.create_data_source(source_type)
                else:
                    self.data_source, _ = DataSourceFactory.create_from_path(str(source_path_obj))
            
            self.logger.info(f"Loading data using {self.data_source.__class__.__name__}...")
            data = self.data_source.read(str(source_path_obj), **read_kwargs)
            
            self._log_data_info(data)
            self._save_data(data, save_path)
            
            self.log_execution_end("Data Ingestion", success=True)
            return data
            
        except Exception as e:
            self.logger.error(f"Error during data ingestion: {e}")
            self.log_execution_end("Data Ingestion", success=False)
            raise
    
    def _log_data_info(self, data: pd.DataFrame) -> None:
        self.logger.info(f"✓ Data loaded successfully")
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
        self.logger.info(f"✓ Data saved to: {save_path}")