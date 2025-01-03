import sys
import os
import numpy as np
import pandas as pd
from pymongo.mongo_client import MongoClient
from zipfile import Path
from src.constant import *
from src.exception import CostumException
from src.logger import logging
from src.utils import main_utils
from dataclasses import dataclass


@dataclass
class DataIngetionConfig:
    artifact_folder:str = os.path.join(artifact_folder)

class DataIngetion:
    def __init__(self):
        self.data_ingetion_config = DataIngetionConfig()
        self.utils = MainUtils()


    def export_collection_as_dataFrame(self, collection_name,db_name):
        try:
            mongo_client = MongoClient(MONGO_DB_URL)

            collection_name = mongo_client[db_name][collection_name]
            df = pd.DataFrame(list(collection.find()))

            if "_id" in df.columns.to_list():
                df = df.drop(columns=['_id'], axis=1)
            df.replace({"na:np.nan"}, inplace=True)

            return df
        except Exception as e:
            raise CustomException(e, error_details)
        
    def export_data_into_feature_store_file_path(self)->pd.DataFrame:
        try:
            loggign.info(f"Exporting data from mongodb")
            raw_file_path= self.data_ingestion_confi.artifact_folder
            
            os.makedir(raw_file_path, exist_ok=True)

            sensor_data =self.export_collection_as_dataFrame(
                collection_name=
                db_name= MONGO_DATABASE_NAME
            )

            logging.info(f"saving exported data into feature store :{raw_file_path}")

            feature_store_file_path = os.path.json(raw_file_path,'wafer_fault.csv')

            sensor_data.to_csv(feature_store_file_path, index=False)

            return feature_store_file_path
        except Exception as e:
            raise CustomException(e, sys)


    def initiate_data_ingetion(self) -> Path:
        logging.info("Entered initiated data ingestion method of data ingestion class")

        try:
            featuer_store_file_path = self.export_data_into_feature_store_file_path()

            logging.info("got the data from mongodb")

            logging.info("exited intiate data ingestion method of data ingestion class")

            return featuer_store_file_path
        except Exception as e:
            raise CustomException (e, sys) from e
