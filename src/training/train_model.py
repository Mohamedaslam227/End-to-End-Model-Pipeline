from src.utils.base import DataComponent
from src.utils.config import ConfigManager
from src.utils.logger import LoggerConfig
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
from sklearn.pipeline import Pipeline
import mlflow
import joblib
import os
from pathlib import Path

class TrainModel(DataComponent):
    def __init__(self,logger:LoggerConfig,config:ConfigManager):
        super().__init__(logger,config)

    def save_model(self, model):
        save_dir = self.get_config("model.save_dir")
        model_name = self.get_config("model.model_name")
        full_path = Path(save_dir) / f"{model_name}.pkl"
        
        # Ensure directory exists
        self.create_directory(save_dir)
        
        joblib.dump(model, full_path)
        self.logger.info(f"Model saved locally at {full_path}")

    def load_model(self):
        save_dir = self.get_config("model.save_dir")
        model_name = self.get_config("model.model_name")
        full_path = Path(save_dir) / f"{model_name}.pkl"

        if full_path.exists():
            self.logger.info(f"Loading model from {full_path}")
            return joblib.load(full_path)
        else:
            self.logger.warning(f"No model found at {full_path}")
            return None

    def execute(self,pipeline:Pipeline, train_data:pd.DataFrame, target_train:pd.Series, test_data:pd.DataFrame,target_test:pd.Series,val_data:pd.DataFrame,target_val:pd.Series):
        self.log_execution_start("Model Training")
        try:
            mlflow.sklearn.autolog()
            with mlflow.start_run(run_name=f"rf_telco_churn"):
                for epoch in range(self.get_config("model.epochs",default=10)):
                    pipeline.fit(train_data,target_train)
                    mlflow.log_metric("epoch",epoch)
                    mlflow.log_metric(f"{epoch}_validation_accuracy",accuracy_score(target_val,pipeline.predict(val_data)))
                    mlflow.log_metric(f"{epoch}_validation_precision",precision_score(target_val,pipeline.predict(val_data)))
                    mlflow.log_metric(f"{epoch}_validation_recall",recall_score(target_val,pipeline.predict(val_data)))
                    mlflow.log_metric(f"{epoch}_validation_f1",f1_score(target_val,pipeline.predict(val_data)))
                    # mlflow.log_metric(f"{epoch}_validation_confusion_matrix",confusion_matrix(target_val,pipeline.predict(val_data)))
                
                
                mlflow.log_metric("test_accuracy",accuracy_score(target_test,pipeline.predict(test_data)))
                mlflow.log_metric("test_precision",precision_score(target_test,pipeline.predict(test_data)))
                mlflow.log_metric("test_recall",recall_score(target_test,pipeline.predict(test_data)))
                mlflow.log_metric("test_f1",f1_score(target_test,pipeline.predict(test_data)))
                # Confusion matrix cannot be logged as a metric (it requires single scalar value).
                # mlflow.log_metric("test_confusion_matrix",confusion_matrix(target_test,pipeline.predict(test_data)))
                
                # Save locally first to ensure persistence
                self.save_model(pipeline)

                # Attempt to log to MLflow
                try:
                    mlflow.sklearn.log_model(
                        sk_model=pipeline, 
                        artifact_path="model",
                        # registered_model_name=self.get_config("model.model_name") # specific configuration required for registry
                    )
                except Exception as e:
                    self.logger.warning(f"Failed to log model to MLflow: {e}")
            
            self.log_execution_end("Model Training")
            return pipeline

        except Exception as e:
            self.logger.exception(f"Error during Model Training: {e}")
            return None
    
