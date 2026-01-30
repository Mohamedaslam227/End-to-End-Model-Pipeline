import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
from abc import ABC, abstractmethod
from src.utils.base import DataComponent

class DataProcessor(ABC):
    @abstractmethod
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        pass
    
    @abstractmethod
    def get_description(self) -> str:
        pass

class NullValueHandler(DataProcessor):
    def __init__(self, strategy: str = 'drop', fill_value: Any = None):
        self.strategy = strategy
        self.fill_value = fill_value
    
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        if self.strategy == 'drop': return df.dropna()
        if self.strategy == 'fill': return df.fillna(self.fill_value)
        if self.strategy == 'forward_fill': return df.fillna(method='ffill')
        if self.strategy == 'backward_fill': return df.fillna(method='bfill')
        raise ValueError(f"Unknown strategy: {self.strategy}")
    
    def get_description(self) -> str:
        return f"Handle nulls ({self.strategy})"

class DuplicateRemover(DataProcessor):
    def __init__(self, subset: Optional[List[str]] = None, keep: str = 'first'):
        self.subset = subset
        self.keep = keep
    
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates(subset=self.subset, keep=self.keep)
    
    def get_description(self) -> str:
        return f"Remove duplicates (keep={self.keep})"

class OutlierRemover(DataProcessor):
    def __init__(self, columns: List[str], multiplier: float = 1.5):
        self.columns = columns
        self.multiplier = multiplier
    
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        df_clean = df.copy()
        for col in self.columns:
            if col in df_clean.columns:
                Q1, Q3 = df_clean[col].quantile(0.25), df_clean[col].quantile(0.75)
                IQR = Q3 - Q1
                lower, upper = Q1 - self.multiplier * IQR, Q3 + self.multiplier * IQR
                df_clean = df_clean[(df_clean[col] >= lower) & (df_clean[col] <= upper)]
        return df_clean
    
    def get_description(self) -> str:
        return f"Outlier removal: {self.columns}"

class ProcessingPipeline:
    """Chain of Responsibility pipeline for sequential data transformations."""
    def __init__(self, logger):
        self.logger = logger
        self.processors = []
    
    def add_processor(self, processor: DataProcessor) -> 'ProcessingPipeline':
        self.processors.append(processor)
        return self
    
    def execute(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df.copy()
        for idx, proc in enumerate(self.processors, 1):
            self.logger.info(f"  Step {idx}: {proc.get_description()}")
            initial_rows = len(result)
            result = proc.process(result)
            self.logger.info(f"    Rows: {initial_rows} -> {len(result)}")
        return result

class DataCleaning(DataComponent):
    """Orchestrates the data cleaning process using a ProcessingPipeline."""
    def __init__(self, logger, config_manager, pipeline: Optional[ProcessingPipeline] = None):
        super().__init__(logger, config_manager)
        self.pipeline = pipeline
    
    def execute(self, data_path: Optional[str] = None, save_path: Optional[str] = None) -> pd.DataFrame:
        self.log_execution_start("Data Cleaning")
        try:
            dataset_name = self.get_config("dataset.name", "data")
            
            if data_path:
                data_file_path = Path(data_path)
            else:
                data_dir = self.get_data_path("dataset.data_path")
                data_file_path = data_dir / dataset_name if Path(dataset_name).suffix else data_dir / f"{dataset_name}.csv"
            
            if save_path:
                save_file_path = Path(save_path)
            else:
                processed_dir = self.get_data_path("dataset.processed_data_path")
                save_file_path = processed_dir / dataset_name if Path(dataset_name).suffix else processed_dir / f"{dataset_name}.csv"
            
            self.logger.info(f"Cleaning data from {data_file_path}...")
            df = pd.read_csv(data_file_path)
            
            if self.pipeline is None:
                self.pipeline = ProcessingPipeline(self.logger).add_processor(NullValueHandler()).add_processor(DuplicateRemover())
            
            cleaned_df = self.pipeline.execute(df)
            save_file_path.parent.mkdir(parents=True, exist_ok=True)
            cleaned_df.to_csv(save_file_path, index=False)
            
            self.logger.info(f"✓ Cleaned data saved to: {save_file_path}")
            self.logger.info(f"✓ Retention: {(len(cleaned_df)/len(df)*100):.2f}%")
            self.log_execution_end("Data Cleaning", success=True)
            return cleaned_df
        except Exception as e:
            self.logger.exception(f"Error in data cleaning: {e}")
            raise