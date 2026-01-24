from pathlib import Path
import pandas as pd
import great_expectations as gx

from src.utils.logger import setup_logger
from src.utils.config import load_config
from datetime import datetime
config = load_config("configs/data_config.yaml")
logger = setup_logger()

data_path = Path(config["dataset"]["data_path"])


def data_validation() -> bool:
    try:
        logger.info("Data validation started")
        
        # Load the data
        df = pd.read_csv(data_path)
        logger.info(f"Loaded data with {len(df)} rows and {len(df.columns)} columns")
        df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
        logger.info("Converted TotalCharges to numeric type")
        
        # Get context - gx.get_context() returns EphemeralDataContext by default
        context = gx.get_context()
        logger.info(f"Created context: {type(context).__name__}")
        
        # Add Pandas datasource - NOTE: use 'data_sources' with underscore
        datasource_name = "churn_datasource"
        datasource = context.data_sources.add_pandas(name=datasource_name)
        logger.info(f"Created datasource: {datasource_name}")
        
        # Add dataframe asset
        asset_name = "churn_asset"
        data_asset = datasource.add_dataframe_asset(name=asset_name)
        logger.info(f"Created data asset: {asset_name}")
        
        # Add batch definition
        batch_definition = data_asset.add_batch_definition_whole_dataframe("batch_def")
        logger.info("Created batch definition")
        
        # Get batch with dataframe
        batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
        logger.info("Created batch from dataframe")
        
        # Create expectation suite
        suite_name = f"churn_validation_suite_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Delete existing suite if it exists
        try:
            context.suites.delete(name=suite_name)
            logger.info(f"Deleted existing suite: {suite_name}")
        except Exception:
            logger.info(f"No existing suite to delete: {suite_name}")
        
        # Create new suite
        suite = context.suites.add(gx.core.expectation_suite.ExpectationSuite(name=suite_name))
        logger.info(f"Created expectation suite: {suite_name}")
        
        # Get validator
        validator = context.get_validator(
            batch=batch,
            expectation_suite=suite
        )
        
        logger.info("Adding expectations...")
        
        # Table-level expectations
        validator.expect_table_row_count_to_be_between(
            min_value=1000,
            max_value=10000
        )
        
        validator.expect_table_column_count_to_equal(21)
        
        # Required columns
        required_columns = [
            "customerID", "gender", "SeniorCitizen", "Partner", "Dependents",
            "tenure", "PhoneService", "MultipleLines", "InternetService",
            "OnlineSecurity", "OnlineBackup", "DeviceProtection", "TechSupport",
            "StreamingTV", "StreamingMovies", "Contract",
            "PaperlessBilling", "PaymentMethod",
            "MonthlyCharges", "TotalCharges", "Churn"
        ]
        
        # Check column existence
        for col in required_columns:
            validator.expect_column_to_exist(col)
        
        # Not null expectations (excluding TotalCharges which may have nulls)
        not_null_columns = [
            "customerID", "gender", "SeniorCitizen", "Partner", "Dependents",
            "tenure", "PhoneService", "MultipleLines", "InternetService",
            "OnlineSecurity", "OnlineBackup", "DeviceProtection", "TechSupport",
            "StreamingTV", "StreamingMovies", "Contract",
            "PaperlessBilling", "PaymentMethod", "MonthlyCharges", "Churn"
        ]
        
        for col in not_null_columns:
            validator.expect_column_values_to_not_be_null(col)
        
        # Categorical value expectations
        validator.expect_column_values_to_be_in_set(
            "gender", ["Male", "Female"]
        )
        
        validator.expect_column_values_to_be_in_set(
            "SeniorCitizen", [0, 1]
        )
        
        validator.expect_column_values_to_be_in_set(
            "Partner", ["Yes", "No"]
        )
        
        validator.expect_column_values_to_be_in_set(
            "Dependents", ["Yes", "No"]
        )
        
        validator.expect_column_values_to_be_in_set(
            "PhoneService", ["Yes", "No"]
        )
        
        validator.expect_column_values_to_be_in_set(
            "MultipleLines", ["Yes", "No", "No phone service"]
        )
        
        validator.expect_column_values_to_be_in_set(
            "InternetService", ["DSL", "Fiber optic", "No"]
        )
        
        validator.expect_column_values_to_be_in_set(
            "OnlineSecurity", ["Yes", "No", "No internet service"]
        )
        
        validator.expect_column_values_to_be_in_set(
            "OnlineBackup", ["Yes", "No", "No internet service"]
        )
        
        validator.expect_column_values_to_be_in_set(
            "DeviceProtection", ["Yes", "No", "No internet service"]
        )
        
        validator.expect_column_values_to_be_in_set(
            "TechSupport", ["Yes", "No", "No internet service"]
        )
        
        validator.expect_column_values_to_be_in_set(
            "StreamingTV", ["Yes", "No", "No internet service"]
        )
        
        validator.expect_column_values_to_be_in_set(
            "StreamingMovies", ["Yes", "No", "No internet service"]
        )
        
        validator.expect_column_values_to_be_in_set(
            "Contract", ["Month-to-month", "One year", "Two year"]
        )
        
        validator.expect_column_values_to_be_in_set(
            "PaperlessBilling", ["Yes", "No"]
        )
        
        validator.expect_column_values_to_be_in_set(
            "PaymentMethod", [
                "Electronic check",
                "Mailed check",
                "Bank transfer (automatic)",
                "Credit card (automatic)"
            ]
        )
        
        validator.expect_column_values_to_be_in_set(
            "Churn", ["Yes", "No"]
        )
        
        # Numeric range expectations
        validator.expect_column_values_to_be_between(
            "tenure", min_value=0, max_value=72
        )
        
        validator.expect_column_values_to_be_between(
            "MonthlyCharges", min_value=0, max_value=200
        )
        
        validator.expect_column_values_to_be_of_type("TotalCharges", "float")
        
        validator.expect_column_values_to_be_between(
            "TotalCharges", min_value=0, max_value=10000
        )
        
        # customerID should be unique
        validator.expect_column_values_to_be_unique("customerID")
        
        # Update the suite instead of saving (which tries to add)
        context.suites.add_or_update(validator.expectation_suite)
        logger.info("Expectation suite updated")
        
        # Run validation directly
        logger.info("Running validation...")
        validation_result = validator.validate()
        
        # Check results
        if validation_result.success:
            logger.info("✓ Data validation PASSED")
            
            # Get statistics
            results_list = validation_result.results
            
            total = len(results_list)
            successful = sum(1 for r in results_list if r.success)
            
            logger.info(f"  Expectations evaluated: {total}")
            logger.info(f"  Successful: {successful}")
            logger.info(f"  Success rate: {(successful/total*100):.2f}%")
            
            return True
        else:
            logger.error("✗ Data validation FAILED")
            
            # Log failed expectations
            results_list = validation_result.results
            failed_expectations = [r for r in results_list if not r.success]
            
            logger.error(f"  Total failed expectations: {len(failed_expectations)}")
            
            for idx, result in enumerate(failed_expectations[:10], 1):
                exp_type = result.expectation_config.expectation_type
                kwargs = result.expectation_config.kwargs
                column = kwargs.get("column", "N/A")
                
                logger.error(f"  {idx}. {exp_type}")
                logger.error(f"     Column: {column}")
                
                if hasattr(result.result, "observed_value"):
                    logger.error(f"     Observed: {result.result.observed_value}")
            
            if len(failed_expectations) > 10:
                logger.error(f"  ... and {len(failed_expectations) - 10} more failures")
            
            return False
    
    except FileNotFoundError:
        logger.exception(f"Data file not found: {data_path}")
        return False
    except pd.errors.EmptyDataError:
        logger.exception(f"Data file is empty: {data_path}")
        return False
    except Exception as e:
        logger.exception(f"Error during data validation: {e}")
        return False


if __name__ == "__main__":
    success = data_validation()
    exit(0 if success else 1)