from google.cloud import bigquery
import os

class BigQuery:
    def __init__(self, key_path) -> None:
        try:
            self.client = bigquery.Client.from_service_account_json(json_credentials_path=key_path)
        except Exception as e:
            raise BaseException(f"Não foi possível instanciar o bigquery {e}")


    def execute_query(self, query_string):
        return self.client.query(query_string).result()

        