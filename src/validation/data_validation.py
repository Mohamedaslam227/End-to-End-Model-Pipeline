import pandas as pd
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
from abc import ABC, abstractmethod
import great_expectations as gx
from great_expectations.core.expectation_suite import ExpectationSuite
from src.utils.base import DataComponent

class ValidationRule(ABC):
    @abstractmethod
    def apply(self, validator, config: Dict[str, Any]) -> None:
        pass

class ColumnExistenceRule(ValidationRule):
    def apply(self, validator, config: Dict[str, Any]) -> None:
        for column in config.get('columns', []):
            validator.expect_column_to_exist(column)

class ColumnNullabilityRule(ValidationRule):
    def apply(self, validator, config: Dict[str, Any]) -> None:
        for column in config.get('columns', []):
            validator.expect_column_values_to_not_be_null(column)

class ColumnValueSetRule(ValidationRule):
    def apply(self, validator, config: Dict[str, Any]) -> None:
        for column, value_set in config.get('rules', {}).items():
            validator.expect_column_values_to_be_in_set(column, value_set)

class ColumnRangeRule(ValidationRule):
    def apply(self, validator, config: Dict[str, Any]) -> None:
        for column, range_config in config.get('rules', {}).items():
            validator.expect_column_values_to_be_between(
                column, min_value=range_config.get('min'), max_value=range_config.get('max')
            )

class ColumnTypeRule(ValidationRule):
    def apply(self, validator, config: Dict[str, Any]) -> None:
        for column, dtype in config.get('rules', {}).items():
            validator.expect_column_values_to_be_of_type(column, dtype)

class ColumnUniquenessRule(ValidationRule):
    def apply(self, validator, config: Dict[str, Any]) -> None:
        for column in config.get('columns', []):
            validator.expect_column_values_to_be_unique(column)

class RowCountRule(ValidationRule):
    def apply(self, validator, config: Dict[str, Any]) -> None:
        validator.expect_table_row_count_to_be_between(
            min_value=config.get('min_rows', 0), max_value=config.get('max_rows', float('inf'))
        )

class ColumnCountRule(ValidationRule):
    def apply(self, validator, config: Dict[str, Any]) -> None:
        validator.expect_table_column_count_to_equal(config.get('expected_count', 0))

class ExpectationSuiteBuilder:
    def __init__(self, validator):
        self.validator = validator
        self.rules = []
    
    def add_rule(self, rule: ValidationRule, config: Dict[str, Any]) -> 'ExpectationSuiteBuilder':
        self.rules.append((rule, config))
        return self
    
    def build(self) -> None:
        for rule, config in self.rules:
            rule.apply(self.validator, config)

class DataValidation(DataComponent):
    """Encapsulates GX validation using a rule-based Builder pattern."""
    def __init__(self, logger, config_manager):
        super().__init__(logger, config_manager)
        self.context = None
        self.df = None
    
    def execute(self, data_path: Optional[str] = None) -> bool:
        self.log_execution_start("Data Validation")
        try:
            if data_path:
                data_file_path = Path(data_path)
            else:
                data_dir = self.get_data_path("dataset.data_path")
                dataset_name = self.get_config("dataset.name", "data")
                data_file_path = data_dir / dataset_name if Path(dataset_name).suffix else data_dir / f"{dataset_name}.csv"

            self.df = self._load_and_prepare_data(data_file_path)
            self._setup_gx()
            
            validator = self._create_validator()
            self._apply_default_validation(validator)
            
            result = validator.validate()
            is_valid = self._process_results(result)
            
            self.log_execution_end("Data Validation", success=is_valid)
            return is_valid
        except Exception as e:
            self.logger.exception(f"Error during validation: {e}")
            return False
    
    def _load_and_prepare_data(self, path: Path) -> pd.DataFrame:
        df = pd.read_csv(path)
        if 'TotalCharges' in df.columns:
            df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
        return df
    
    def _setup_gx(self) -> None:
        self.context = gx.get_context()
    
    def _create_validator(self):
        ds_name = f"ds_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        datasource = self.context.data_sources.add_pandas(name=ds_name)
        asset = datasource.add_dataframe_asset(name="asset")
        batch_def = asset.add_batch_definition_whole_dataframe("batch_def")
        batch = batch_def.get_batch(batch_parameters={"dataframe": self.df})
        suite = self.context.suites.add(ExpectationSuite(name=f"suite_{ds_name}"))
        return self.context.get_validator(batch=batch, expectation_suite=suite)
    
    def _apply_default_validation(self, validator) -> None:
        builder = ExpectationSuiteBuilder(validator)
        builder.add_rule(RowCountRule(), {'min_rows': 1000, 'max_rows': 10000})
        builder.add_rule(ColumnCountRule(), {'expected_count': 21})
        
        cols = ["customerID", "gender", "SeniorCitizen", "Partner", "Dependents", "tenure", "PhoneService", "MultipleLines", "InternetService", "OnlineSecurity", "OnlineBackup", "DeviceProtection", "TechSupport", "StreamingTV", "StreamingMovies", "Contract", "PaperlessBilling", "PaymentMethod", "MonthlyCharges", "TotalCharges", "Churn"]
        builder.add_rule(ColumnExistenceRule(), {'columns': cols})
        builder.add_rule(ColumnNullabilityRule(), {'columns': [c for c in cols if c != 'TotalCharges']})
        
        builder.add_rule(ColumnValueSetRule(), {'rules': {
            'gender': ["Male", "Female"], 'SeniorCitizen': [0, 1], 'Partner': ["Yes", "No"], 'Dependents': ["Yes", "No"], 'PhoneService': ["Yes", "No"], 'MultipleLines': ["Yes", "No", "No phone service"], 'InternetService': ["DSL", "Fiber optic", "No"], 'OnlineSecurity': ["Yes", "No", "No internet service"], 'OnlineBackup': ["Yes", "No", "No internet service"], 'DeviceProtection': ["Yes", "No", "No internet service"], 'TechSupport': ["Yes", "No", "No internet service"], 'StreamingTV': ["Yes", "No", "No internet service"], 'StreamingMovies': ["Yes", "No", "No internet service"], 'Contract': ["Month-to-month", "One year", "Two year"], 'PaperlessBilling': ["Yes", "No"], 'PaymentMethod': ["Electronic check", "Mailed check", "Bank transfer (automatic)", "Credit card (automatic)"], 'Churn': ["Yes", "No"]
        }})
        
        builder.add_rule(ColumnRangeRule(), {'rules': {
            'tenure': {'min': 0, 'max': 72}, 'MonthlyCharges': {'min': 0, 'max': 200}, 'TotalCharges': {'min': 0, 'max': 10000}
        }})
        builder.add_rule(ColumnTypeRule(), {'rules': {'TotalCharges': 'float'}})
        builder.add_rule(ColumnUniquenessRule(), {'columns': ['customerID']})
        builder.build()
    
    def _process_results(self, result) -> bool:
        if result.success:
            self.logger.info("✓ Validation PASSED")
            return True
        self.logger.error("✗ Validation FAILED")
        for idx, res in enumerate([r for r in result.results if not r.success][:10], 1):
            self.logger.error(f"  {idx}. {res.expectation_config.expectation_type} - Column: {res.expectation_config.kwargs.get('column')}")
        return False