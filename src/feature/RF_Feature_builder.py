from src.utils.base import DataComponent
from src.utils.config import ConfigManager
from src.utils.logger import LoggerConfig
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.processing import OneHotEncoder



class RF_FeatureBuilder(DataComponent):
    def __init__(self, logger, config: ConfigManager):
        super().__init__(logger, config)
    
    def execute(self):
        self.log_execution_start("RF_FeatureBuilder")
        try:
            train_data = self.validate_path(self.get_config("dataset.train_data_path"), must_exist=True)
            test_data = self.validate_path(self.get_config("dataset.test_data_path"), must_exist=True)
            val_data = self.validate_path(self.get_config("dataset.val_data_path"), must_exist=True)

            train_data = pd.read_csv(train_data)
            self.logger.info(f"✓ Train data loaded successfully")
            self.logger.info(f"  Shape: {train_data.shape}")
            test_data = pd.read_csv(test_data)
            self.logger.info(f"✓ Test data loaded successfully")
            self.logger.info(f"  Shape: {test_data.shape}")
            val_data = pd.read_csv(val_data)
            self.logger.info(f"✓ Validation data loaded successfully")
            self.logger.info(f"  Shape: {val_data.shape}")
            training_data = train_data.drop_columns(columns=['customerID', self.get_config("dataset.target_column")])
            target_train = train_data[self.get_config("dataset.target_column")].map({"Yes":1,"No":0})
            target_test = test_data[self.get_config("dataset.target_column")].map({"Yes":1,"No":0})
            target_val = val_data[self.get_config("dataset.target_column")].map({"Yes":1,"No":0})
            test_data = test_data.drop_columns(columns=['customerID', self.get_config("dataset.target_column")])
            val_data = val_data.drop_columns(columns=['customerID', self.get_config("dataset.target_column")])
            train_data = train_data[self.get_config("dataset.target_column")].map({"Yes":1, "No":0})
            numeric_features = train_data.select_dtypes(include=['int64', 'float64']).columns.tolist()
            categorical_features = train_data.select_dtypes(include=['object']).columns.tolist()
            binary_features =  [
                "Partner",
                "Dependents",
                "PhoneService",
                "PaperlessBilling"
            ]
            categorical_features = [set(categorical_features) - set(binary_features)]
            binary_transformer = OneHotEncoder(
                drop='if_binary',
                handle_unknown='ignore'
            )
            categorical_transformer = OneHotEncoder(
                handle_unknown='ignore',
                sparse_output=False
            )
            preprocessor = ColumnTransformer(
                transformers=[
                    ('num', 'passthrough', numeric_features),
                    ('binary', binary_transformer, binary_features),
                    ('categorical', categorical_transformer, categorical_features)
                ],
                remainder='passthrough'
            )

            pipeline = Pipeline(
                steps=[
                    ('preprocessor', preprocessor),
                    ('model', RandomForestClassifier(**self.get_config("model.model_params")))
                ]
            )

            return pipeline,training_data,target_train,test_data,target_test,val_data,target_val
            

            
        except Exception as e:
            self.logger.exception(f"Error during RF_FeatureBuilder: {e}")
            return False

        self.log_execution_end("RF_FeatureBuilder")
