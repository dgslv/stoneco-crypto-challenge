import psycopg
import os

class Postgres:
    def __init__(self) -> None:
        params = {
            'dbname': os.getenv('POSTGRES_DB'),
            'user': os.getenv('POSTGRES_USER'),
            'password': os.getenv('POSTGRES_PASS'),
            'host': os.getenv('POSTGRES_HOST'),  
            'port': os.getenv('POSTGRES_PORT')  
        }
        
        try:
            self.connection = psycopg.connect(**params)
        except Exception as error:
            raise error
            
    def get_cursor(self) -> psycopg.ClientCursor:
        return self.connection.cursor()
    
    def commit(self) -> None:
        self.connection.commit()
        
   
    
    
        
        