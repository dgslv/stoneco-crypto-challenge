import pandas as pd
from uuid import uuid4
from datetime import datetime, timedelta
from connectors.postgres import Postgres
from connectors.bigquery import BigQuery
from .core import Job
import os
from logger import logger

PATH = os.getcwd()
STORAGE_PATH = f"{PATH}/src/etls/storage/criptos"
BLOCK_SIZE=1000
    
class StoneCryptoJob(Job):
    """
        Implementações relacionadas ao desafio

    """
    def __init__(self) -> None:
        key_path = f"{os.getcwd()}/{os.getenv('GCP_CREDENTIALS_PATH')}"
        self.postgres = Postgres()
        self.client = BigQuery(key_path=key_path)
        pass
    
    def extract(self, lower_limit, upper_limit, current_iteration=0):
        """
            Retira os dados do bigquery
        """
        query_table = 'bigquery-public-data.crypto_ethereum.tokens'
        query_limit = int(os.getenv('CRIPTO_ETL_RECORD_LIMIT', '100'))
        
        logger.info(f"Buscando dados no intervalo de datas {lower_limit} - {upper_limit}")
        query_string = os.getenv('CRIPTO_ETL_QUERY', """
            select
                *
            from {}
            where block_timestamp between '{}' and '{}'
            order by block_timestamp, address
            limit {}
            offset {}
        """).format(query_table, 
                    lower_limit,
                    upper_limit,
                    query_limit, 
                    current_iteration * query_limit
            )
        
        logger.info(query_string)
        return self.client.execute_query(query_string)
    
    def preprocess(self, df):
        """Preprocesses the data before sending them to destination

        Args:
            data (dd.DataFrame): data returned from a source like BigQuery

        Returns:
            dd.DataFrame: processed data
        """
        df['block_timestamp'] = pd.to_datetime(df['block_timestamp'], format='%Y-%m-%d %H:%M:%S.%f UTC').astype(int)
        logger.info("Preprocessando dataset...")
        def values_mapper(record):
            try:
                record['block'] = str(record['block_timestamp']) + str(record['block_hash']) + str(record['block_number'])
                return record
            except Exception as e:
                raise e
                
        return df.apply(values_mapper, axis=1)
    
    def store_file(self, data: pd.DataFrame, floor_dt: datetime) -> bool:
        file_uuid = uuid4()
        
        try:
            file_name = f'{floor_dt.strftime("%Y-%m-%d-%H-%M-%S")}_{file_uuid}'
            file_name = f'{STORAGE_PATH}/{file_name}.csv'
            logger.info(f"Salvando arquivo {file_name}.csv")
            data.to_csv(file_name, index=False, header=False)
            return file_name
        except Exception as e:
            logger.error("Erro ao tentar salvar arquivo em store_file.")
            raise e
 
    def delete_file(self, file_path):
        try:
            logger.info(f"Deletando arquivo {file_path}...")
            os.remove(file_path)
        except Exception as _e:
            logger.error("Erro ao tentar deletar arquivo em store_file.")
            raise _e
        return 1

    def store_values_from_file(self, file_path) -> None:
        try:
            cursor = self.postgres.get_cursor()
            logger.info("Inserindo records...")
            with open(f"{file_path}", "r") as f:
                with cursor.copy("COPY stone_schemas.tokens FROM STDIN WITH (FORMAT CSV)") as copy:
                    while data := f.read(BLOCK_SIZE):
                        copy.write(data)
            cursor.close()
            self.postgres.commit()
            logger.info("Inserção de records finalizada.")
            return 1
        except Exception as e:
            raise e
        
    def execute(self, date = (datetime.now() - timedelta(days=1)).date()):
        current_iteration = 0
        
        previous_day = date - timedelta(days=1)

        lower_limit = datetime.combine(previous_day, datetime.min.time())
        upper_limit = datetime.combine(previous_day, datetime.max.time())
        while True:
            logger.info(f"Iteração atual: {current_iteration}")
            
            data = self.preprocess(
                self.extract(
                    lower_limit=lower_limit,
                    upper_limit=upper_limit,
                    current_iteration=current_iteration                    
                    ).to_dataframe()
            )
            
            file_path = self.store_file(data,  date)
            
            self.store_values_from_file(file_path)
            self.delete_file(file_path)

            
            if data.shape[0] < 100 or data.shape[0] == 0:
                logger.info("caiu aqui")
                return 1
            else:
                current_iteration += 1        