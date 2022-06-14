import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import io, urllib
import sqlalchemy as sa


class SqlConnector():
    # Main parameters:
    def __init__(self, conn_string, schema, query) -> None:
        self.conn_string = conn_string
        self.schema = schema
        self.query = query

    # Connection engine defination:
    def engine(self):
        connection_uri = f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(self.conn_string)}"
        engine = sa.create_engine(connection_uri, fast_executemany=True)
        return engine

    # Run the query
    def connector(self):
        engine = self.engine()
        query = str(self.query)

        #check the first item in the query string, if it is "Select" than read the data from sql to pandas dataframe, other case just run the query
        try:
            check = query.split(" ")
            if check[0].lower() == 'select':
                table_df = pd.read_sql(query, engine)
                print(f'Number of rows: {len(table_df)}')
                return table_df
            else:
                with engine.connect() as con:
                    result = con.execute(query)
                return str(f"Rows affected: {result.rowcount}")
        except:
            return("Something went wrong!")

    # Append dataframe to the SQL database
    def uploader(self, df):
        #jsut for test:
        try:
            engine = self.engine()
            df.to_sql("PersonTest", engine, if_exists="append", schema = self.schema, index=False, method = 'multi')
            return str(f"Done, it is working ! Rows affected: {len(df)}")
        except:
            return("Something went wrong in the query!")
        