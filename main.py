from sql_parser import ParsedInsert, ParsedSelect, Parser

from catalog_handler import JSONFileHandler
from constants import CATALOG_LOCATION, CONN_STRING

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Result

from db import PostgresExecutor

import pandas as pd

from uuid import uuid4

class Engine:

    def __init__(self) -> None:
        self.parser = Parser()

    # engine recieves a dataclass and processes an operation

    def run(self, sql):
        dclass = self.parser.process_statement(sql)
        self.process_dataclass(dclass)


    def process_dataclass(self, statement_dataclass):
        
        match statement_dataclass:
            
            case ParsedInsert() as insert:
                
                # Read the latest metadata avialable for this table

                insert.target_table

                # insert the data into the table

                # Call the metadata and check the last state
                # write the data
                # updat the metadata
                print('its an insert statement!')

                # Check if the file does not exist -> then create one
                # save the file with a unique name

                created_file_name = str(uuid4())

                # create a pandas dataframe from the provided values
                df_to_save = pd.DataFrame.from_records(insert.insert_values, columns=insert.insert_columns)


                db = PostgresExecutor(CONN_STRING)

                

                print ('created_file_name ',  created_file_name)

                # Create a new manifest file entry in the manifest_files table
                sql_new_manifest_entry = "INSERT INTO manifest_files (manifest_file_id, datafile_location) VALUES (%s, %s)"
                db.execute_query(sql_new_manifest_entry, (created_file_name, f"{created_file_name}.parquet",))

                # Get the last snapshot of the table
                result = db.execute_query("SELECT manifest_list_id FROM tables ORDER BY last_updated_at DESC LIMIT 1")

                last_manifest_list_id = result[0][0]
                print(last_manifest_list_id)

                # Select all the previous manifests + add the new entry 
                new_manifest_list_id = str(uuid4())

                copy_to_new_manifests_sql = """
                INSERT INTO manifest_lists
                (manifest_list_id, manifest_file_id)

                SELECT
                    %s as manifest_list_id,
                    manifest_file_id
                FROM manifest_lists
                WHERE manifest_list_id = %s
                """
                db.execute_query(copy_to_new_manifests_sql, (new_manifest_list_id, last_manifest_list_id,))

                
                insert_new_row = """
                INSERT INTO manifest_lists ( manifest_list_id, manifest_file_id) VALUES (%s, %s)
                """

                db.execute_query(insert_new_row, (new_manifest_list_id, created_file_name,))


                print('OK!!!')


                    
                # Get all of the manifest files under a manifest id
                

                # write all the existing manifest files + the new file under a new 
                # manifest list id
                    
                # update the existing manifest list if







            case ParsedSelect() as select:
                
                print('not yet implemented!')

if __name__ == "__main__":

    sql_insert = "INSERT INTO phone_book (name, number) VALUES ('John Doe', '555-1212'), ('Peter Doe', '555-2323');"
    
    


    engine = Engine()
    engine.run(sql_insert)

