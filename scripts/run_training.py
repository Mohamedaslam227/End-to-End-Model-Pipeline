import sys
sys.path.insert(0, '.')

from src.utils.logger import get_logger
from src.utils.config import ConfigManager
from src.feature.RF_Feature_builder import RF_FeatureBuilder
from src.training.train_model import TrainModel

logger = get_logger()
config = ConfigManager('configs/data_config.yaml')

if config.get("model.model_type") == "RandomForestClassifier":
    rf_feature_builder = RF_FeatureBuilder(logger,config)
    pipeline,training_data,target_train,test_data,target_test,val_data,target_val = rf_feature_builder.execute()
    train_model = TrainModel(logger,config)
    pipeline = train_model.execute(pipeline,training_data,target_train,test_data,target_test,val_data,target_val)
