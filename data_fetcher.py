from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from imu_preprocessor import get_row_wise_stat
import pandas as pd
from flask import jsonify
from store import local_store


class DataFetcher:

    def __init__(self, uri, db):
        self._MONGO_URI = uri
        self._DATABASE = db
        self.client = self.__init_db()

    def __init_db(self) -> MongoClient:
        try:
            client = MongoClient(self._MONGO_URI)
            if client.server_info()['ok'] == 1:
                print("[+] Connection established...")
                return client

        except ServerSelectionTimeoutError as err:
            print(f"[-] TimeoutError: {err}")
            return self.__init_db()

    def __check_connection(self, key: str):
        try:
            if self.client.server_info()['ok'] == 1:
                pass
            else:
                local_store[key] = jsonify(message='There seems to be an issue on our side'), 400
                return
        except (KeyError, ServerSelectionTimeoutError):
            local_store[key] = jsonify(message='There seems to be an issue on our side'), 400
            return

    def sensor_query(self, device, query, param, key):
        self.__check_connection(key)
        _collections = self.client[self._DATABASE].list_collection_names()

        collection = [col for col in _collections if (
                device in col) and (col.endswith("senloc"))]

        collection_data = self.client[self._DATABASE][collection[0]]
        output = list(collection_data.find(query, {'_id': 0, 'srvtime': 1, 'value.lat': 1, 'value.lon': 1,
                                                   f'value.{param}': 1}))
        if len(output) != 0:
            df = pd.json_normalize(output)
            df.columns = [name.replace('value.', '') for name in list(df.columns.tolist())]
            return df
        else:
            local_store[key] = jsonify(message='Requested parameter is not available in the requested device'), 400
            return

    def imu_query(self, device, query, param, key):
        self.__check_connection(key)
        _collections = self.client[self._DATABASE].list_collection_names()

        collection = [col for col in _collections if (
                device in col) and (col.endswith("accloc"))]

        collection_data = self.client[self._DATABASE][collection[0]]
        output = list(collection_data.find(query, {'_id': 0, 'srvtime': 1, 'value.LatAcc': 1, 'value.LonAcc': 1,
                                                   f'value.{param.replace("_Mean", "")}': 1}))
        if len(output) != 0:
            df = pd.json_normalize(output)
            df.columns = [name.replace('value.', '').replace('Acc', '').lower() for name in list(df.columns.tolist())]
            df["lat"] = pd.to_numeric(df["lat"])
            df["lon"] = pd.to_numeric(df["lon"])
            df[param] = get_row_wise_stat(col=param.replace("_Mean", "").lower(), data=df)
            return df
        else:
            local_store[key] = jsonify(message='Requested parameter is not available in the requested device'), 400
            return
