import re
import requests
import json
import base64  
import snowflake.connector 
from datetime import datetime
import logging
from snowflake import telemetry

from snowflake.snowpark.table import WhenMatchedClause, WhenNotMatchedClause
from typing import Dict
import _snowflake
from snowflake.snowpark.functions import column
from snowflake.snowpark import Session
from snowflake.snowpark.types import (
    StructType,
    StructField,
    VariantType,
    IntegerType,
    StringType,
    DateType, 
    BooleanType,
    FloatType,
    TimestampType
)

import logging
ingestionPY_logger = logging.getLogger('snowflake.snowpark.session')


# from snowflake_"iLevel Connector".common import save_vals


#*#*# Functional Functions

def escape_name(name: str): #**#*# we can add "if statements" here and manually code what the names for the tables in snwoflake should be 
    return name.replace("/", "_").replace("-", "_").replace(" ", "_").upper()  


# def sink_table_name(destination_db: str, schema: str, resource_id: str):
#     return destination_db + "." + schema + "." + escape_name(resource_id) 

def sink_table_name(destination_db: str, schema: str, resource_id: str):
    return f'"{destination_db}"."{schema}"."{escape_name(resource_id)}"'



def get_next_page_link(response: requests.Response): 
    r = json.loads(response.text)
    next_link = r.get('links', {}).get('next')
    
    if next_link is not None:
        return next_link
    else:
        return None


def check_data_type(value):

    # if value is None or value == '' or value == 'null' or value == ' ':
    #     return VariantType()

    # if isinstance(value, dict):
    #     return VariantType()
        
    # if isinstance(value, bool):
    #     return BooleanType()


    # if isinstance(value, str):

    #     cleaned_value = value.strip('" ')
        
    #     try:
    #         int(cleaned_value)
    #         return IntegerType()
    #     except ValueError:
    #         pass

    #     try:
    #         float(cleaned_value)
    #         return FloatType()
    #     except ValueError:
    #         pass

    #     if value.isdigit():
    #         return StringType()
            
    #     date_formats = [
    #         "%Y-%m-%dT%H:%M:%S.%fZ",
    #         "%Y-%m-%dT%H:%M:%SZ", 
    #         "%Y-%m-%dT%H:%M:%S",
    #         "%Y-%m-%d %H:%M:%S.%f",
    #         "%Y-%m-%d %H:%M:%S",
    #         "%Y-%m-%d %H:%M",
    #         "%Y-%m-%d %H",
    #         "%Y-%m-%d",
    #         "%m/%d/%Y",
    #         "%d/%m/%Y",
    #         "%Y-%m",
    #         "%Y"
    #     ]

    #     for date_format in date_formats:
    #         try:
    #             datetime.strptime(value, date_format)

    #             return TimestampType()
    #         except ValueError:
    #             continue

    #     return StringType()


    # if isinstance(value, int):
    #     return IntegerType()
    # if isinstance(value, float):
    #     return FloatType()


    return VariantType()


#*#*#* Store client id and client secret in a table/secrets 

def create_secrets(session: Session, client_id: str, client_secret: str):
    app_name = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
        
    # session.sql(f"""CREATE OR REPLACE NETWORK RULE ILEVEL_RULE MODE = EGRESS TYPE = HOST_PORT VALUE_LIST= ('api.ilevelsolutions.com:443') """).collect()
    # session.sql(f"""USE DATABASE ILEVEL_PYTHON_CONNECTOR_INSTANCE ; CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ILEVEL_INTEGRATION ALLOWED_NETWORK_RULES = (ILEVEL_RULE) allowed_authentication_secrets = all ENABLED = TRUE  """).collect()
    session.sql(f"""CREATE or replace SECRET {app_name}.PUBLIC.CLIENT_ID TYPE= GENERIC_STRING SECRET_STRING= "{client_id}" """).collect()
    session.sql(f"""CREATE or replace SECRET {app_name}.PUBLIC.CLIENT_SECRET TYPE= GENERIC_STRING SECRET_STRING= "{client_secret}" """).collect()

    # ingestionPY_logger.info("Custom_ILEVEL_CONNECTOR - ingestionPY_logger - create_secrets(): Client ID and Client Secret Created")

# NEWWWW ADDDED # NEWWWW ADDDED  # NEWWWW ADDDED 

    # session.sql("""
    #     CREATE OR REPLACE NETWORK RULE ILEVEL_RULE   
    #     MODE = EGRESS
    #     TYPE = HOST_PORT
    #     VALUE_LIST=('api.ilevelsolutions.com:443');  

    #     CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ILEVEL_INTEGRATION  
    #     ALLOWED_NETWORK_RULES = (ILEVEL_RULE)
    #     allowed_authentication_secrets = all
    #     ENABLED = TRUE;
    # """).collect()
    
# NEWWWW ADDDED  # NEWWWW ADDDED 
    # The error indicates invalid syntax in the ALTER PROCEDURE statement
    # 1. Missing closing parenthesis after reference('external_access_reference')
    # 2. Need comma between EXTERNAL_ACCESS_INTEGRATIONS and SECRETS
    # 3. Need to properly quote database/schema identifiers
    session.sql(f"""ALTER PROCEDURE PUBLIC.VALIDATE_TOKEN() SET 
        EXTERNAL_ACCESS_INTEGRATIONS = (reference('external_access_reference'))
        SECRETS = ('id' = "{app_name}"."PUBLIC"."CLIENT_ID", 
                  'secret' = "{app_name}"."PUBLIC"."CLIENT_SECRET")""").collect()
    # ingestionPY_logger.info("Custom_ILEVEL_CONNECTOR - ingestionPY_logger - create_secrets(): validate_token() function altered for external access integration ") #*# if the external acess integration is not working then the logger will not log the info event ß





#*# Store identifiers for whether client secret and id exist 

def store_config_state(session: Session):
    session.sql(f"""
        INSERT INTO public.config_state_table (key, value)
        VALUES 
            ('client_id', 'Y'),
            ('client_secret', 'Y');
    """).collect()

def validate_token(session: Session) -> str:

    client_id = _snowflake.get_generic_secret_string("id")
    client_secret = _snowflake.get_generic_secret_string("secret")

    url = "https://api.ilevelsolutions.com/v1/token"
    
    credentials = f"{client_id}:{client_secret}"
    encoded_cred = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    payload = 'grant_type=client_credentials'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Basic {encoded_cred}'
    }
    
    response = requests.request("POST", url, headers=headers, data=payload)
    token = json.loads(response.text).get("access_token")

    # ingestionPY_logger.info("Custom_ILEVEL_CONNECTOR - ingestionPY_logger - validate_token(): validate_token() function executed")

    return token







def provision(session: Session, destination_db: str, schema: str):
    destination_database = destination_db
    schema = schema 
    app_name = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]

    # statements1 = [
    #     f"ALTER PROCEDURE PUBLIC.INGEST_DATA(STRING, STRING,  STRING, STRING) SET "
    #     f"EXTERNAL_ACCESS_INTEGRATIONS = (ilevel_integration) "
    #     f"SECRETS = ('id' = "iLevel Connector"."PUBLIC"."CLIENT_ID" , 'secret' = iLevel Connector.PUBLIC.CLIENT_SECRET)",
    #   ]  # Updated from github_integration to ilevel_integration
        # f"CREATE DATABASE IF NOT EXISTS {destination_database}", 
        # f"USE DATABASE {destination_database}", 
        # f"CREATE SCHEMA IF NOT EXISTS {destination_database}.{schema}",
    #     f"GRANT USAGE ON DATABASE {destination_database} TO APPLICATION ROLE ilevel_app_user", #*#* this is not hte ingetion problem , runs without
    #     f"GRANT USAGE ON SCHEMA {destination_database}.{schema} TO APPLICATION ROLE ilevel_app_user", 
    
    session.sql(f"""ALTER PROCEDURE PUBLIC.INGEST_DATA(STRING, STRING,  STRING, STRING) SET 
        EXTERNAL_ACCESS_INTEGRATIONS = (reference('external_access_reference'))
        SECRETS = ('id' = {app_name}.PUBLIC.CLIENT_ID, 
                  'secret' = {app_name}.PUBLIC.CLIENT_SECRET) """).collect()

    # ingestionPY_logger.info("Custom_ILEVEL_CONNECTOR - ingestionPY_logger - provision(): provision() function executed")
    # for statement in statements1:
    #     session.sql(statement).collect() 



#*#*#* Data Inestion Functions

def fetch_single_batch(session: Session, destination_db: str, schema: str ,resource_name: str, table_name: str , page_link: str = None): #*#*#* NEWWWW Note - this version of the fetch data fuction deals with cases where the datatype fo a column may have changed 



#*#*#**#*#*#*# CAN BE SWITCHED OUT FOR THIS #*#*#*#**#*#*#**#*#*#*##*
# import requests
# import json
# from requests_oauthlib import OAuth2Session
# from oauthlib.oauth2 import BackendApplicationClient

# def get_ilevel_token():
#     # The token endpoint
#     token_url = "https://api.ilevelsolutions.com/v1/token"
    
#     # iLevel credentials (you *might* need to decode if the client_id is truly base64-encoded user info,
#     # but typically iLevel would give you a plain string as the 'client_id').
#     client_id = 'MjU5OnN3YWxqaUBheHhzeXNjb25zdWx0aW5nLmNvbQ=='  # or possibly a plain string
#     client_secret = 'Od(OPWe:dl1!6UocW8Y8ue0(9!H={8@a'
    
#     # Create a client for the "client_credentials" flow
#     client = BackendApplicationClient(client_id=client_id)
#     oauth = OAuth2Session(client=client)
    
#     # Fetch the token
#     token_dict = oauth.fetch_token(
#         token_url=token_url,
#         client_id=client_id,         # Pass in your client_id
#         client_secret=client_secret, # Pass in your client_secret
#         # If you need scopes, do: scope=["some_scope"]
#         # If you must explicitly set grant_type, you can pass extra parameters:
#         # include_client_id=True, 
#         # grant_type='client_credentials',
#     )
    
#     # token_dict is typically a dict like {"access_token": "...", "token_type": "Bearer", ...}
#     access_token = token_dict.get('access_token')
#     return access_token


    try:

        client_id = _snowflake.get_generic_secret_string("id")
        client_secret = _snowflake.get_generic_secret_string("secret")

        # Generate token
        url_token = "https://api.ilevelsolutions.com/v1/token"
        credentials = f"{client_id}:{client_secret}"
        encoded_cred = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')                                                                                 
        payload = 'grant_type=client_credentials'
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': f'Basic {encoded_cred}'
        }
        response = requests.request("POST", url_token, headers=headers, data=payload) 
        token = json.loads(response.text)
        token_string = token.get('access_token') 

    except Exception as e:
        ingestionPY_logger.error(f"Custom_ILEVEL_CONNECTOR - ingestionPY_logger - fetch_single_batch(): Error occurred in token creation: {e}")

    token_creation_time = datetime.now()
    count = 0 
    
    all_data = []

    start_time_ingestion = datetime.now()

    try:
    

        while True:


            current_time = datetime.now()
            time_difference = (current_time - token_creation_time).total_seconds()


            if time_difference > 3400:  # 3400 seconds = ~57 minutes
                count += 1

                token_creation_time = datetime.now()
                response = requests.request("POST", url_token, headers=headers, data=payload) 

                token = json.loads(response.text)
                token_string = token.get('access_token') 




            if resource_name == "assets" or resource_name == "funds":
                url = page_link or f"https://api.ilevelsolutions.com/v1/entities/{resource_name}?page[size]=998&page[number]=1"

            elif resource_name == "periodicData" or resource_name == "cashTransactions" or resource_name == "securities" or resource_name == "deals" or resource_name == "fxRates" : 
                url = page_link or f"https://api.ilevelsolutions.com/v1/{resource_name}?page[size]=998&page[number]=1"  #*#* *Note* everythin after 'cashTransactions' has been newly added and has not been tested 

            elif resource_name == "valuationDataItems" or resource_name == "benchmarks" or resource_name == "currencies" or resource_name == "cashTransactionTypes" or resource_name == "entityGroupCategories" :
                url =  f"https://api.ilevelsolutions.com/v1/{resource_name}"  #these resouces need their own if statement because it does not accept additional paremeters such as page[number]


            response = requests.get(url, headers={"Authorization": f"Bearer {token_string}"})
            json_format = json.loads(response.text)
            resource_list = json_format.get("data")

    #*#**#*#*#*#**##*#*#*#*# Flattened List Workflow  #**#*#*#*#*#*#**#*#*#*#*#*#*#*#*#*

            transformed_data_list = []

            for i in resource_list:
                # Flatten the attributes into the main dictionary
                if 'attributes' in i:
                    i.update(i.pop('attributes'))

                # Handle the relationships field
                if 'relationships' in i:
                    relationships = i.pop('relationships')
                    for key, value in relationships.items(): #*#*#* SEE cashTransactionTypes api call output to see hwo thsi works 
                        data = value.get('data')

                        # If 'data' is a list, handle multiple entries
                        if isinstance(data, list) and data:
                            data_dict = {} #*#* this dictionary will contain key value pairs, the key si the data_type adn the value is a lsit of all the id values 
                            for entry in data: #*#*# 'entry' here woudl be a dictionart, this is looping over all the dictionarie sin 'data' 
                                data_type = entry.get('type')
                                data_id = int(entry.get('id'))

                                if data_type and data_id: #*#* necessary so that if it is null then there is no entry taht is created 
                                    # Accumulate IDs for the same type in a list under the same key
                                    relationship_key = f"relationships_{key}_{data_type}_id"
                                    if relationship_key not in data_dict:
                                        data_dict[relationship_key] = []
                                    data_dict[relationship_key].append(data_id) #*#*# here the append() fucntion will append values into the list that is associated with the key so it looks and adds calues to the lsit 

                            i.update(data_dict)

                        # If 'data' is a dictionary (single entry), handle it directly
                        elif isinstance(data, dict) and data: #*#*#* see periodicData for example of api call 
                            data_type = data.get('type')
                            data_id = data.get('id')
                            if data_type and data_id:
                                i[f"relationships_{key}_{data_type}_id"] = data_id

                transformed_data_list.append(i)

            all_data.extend(transformed_data_list)

            page_link = json_format.get("links", {}).get("next")

            if not page_link:
                
                break
            
    except Exception as e:
        ingestionPY_logger.error(f"Custom_ILEVEL_CONNECTOR - ingestionPY_logger - fetch_single_batch(): Error occurred in all_data fetching: {e}")

        #*#* metadata logging 
    end_time_ingestion = datetime.now()

    total_time_ingestion = end_time_ingestion - start_time_ingestion

        #*#* metadata logging 

    start_time_flattening_and_delta_logic = datetime.now()

    try:
        raw_df = session.create_dataframe(
            [{"raw": r} for r in all_data], 
            schema=StructType([StructField("raw", VariantType())])
        )
    except Exception as e:
        ingestionPY_logger.error(f"Custom_ILEVEL_CONNECTOR - ingestionPY_logger - fetch_single_batch(): Error occurred in raw_df creation: {e}")


#**#*#**#*#*#*#**#*#*#*#* Flattening Table Workflow #**#*#*#*#*#*#**#*#*#*#*#*#*#*#*#*
    try:
        all_keys = set()  

        for record in all_data:
            all_keys.update(record.keys())

        relationships_keys = sorted([k for k in all_keys if k.startswith("relationships_")])
        other_keys = sorted([k for k in all_keys if not k.startswith("relationships_")])

        ordered_keys = other_keys + relationships_keys

        columns = {}  

        for key in ordered_keys:
            for p in range(min(100, len(all_data))):
                value = all_data[p].get(key)

                if value is not None and value != 'null':
                    columns[key] = check_data_type(value)
                    break  

            if key not in columns:
                columns[key] = VariantType()

        flattened_df = raw_df.select(
            *[column("raw")[k].cast(t).alias(k) for k, t in columns.items()]
        )
        
    except Exception as e:
        ingestionPY_logger.error(f"Custom_ILEVEL_CONNECTOR - ingestionPY_logger - fetch_single_batch(): Error occurred in flattening workflow: {e}")

    telemetry.add_event("Custom_ILEVEL_CONNECTOR - telemetry - fetch_single_batch() - ", {"Resource Name": resource_name, "Row Count (from raw_df)": raw_df.count(),  "Total time to create all_data list": total_time_ingestion, "Token refreshed count": count, "flattened_df col count": len(columns)})


#**#*#**#*#*#*#**#*#*#*#* Flattening Table Workflow #**#*#*#*#*#*#**#*#*#*#*#*#*#*#*#*

    if table_name is None or table_name == "" or table_name == 'None':

        table_name  = sink_table_name(destination_db, schema, resource_name)

    else:

        table_name = sink_table_name(destination_db, schema, table_name)

    session.create_dataframe([], schema=StructType([StructField(k, t) for k, t in columns.items()])).write.mode("ignore").save_as_table(table_name)
    # session.sql(f"GRANT ALL PRIVILEGES ON TABLE {table_name} TO APPLICATION ROLE ilevel_app_user").collect() ## allows to query but still cant delete this table unless it is done from within the app 

    session.sql(f" GRANT ALL PRIVILEGES ON TABLE {table_name} TO APPLICATION ROLE ilevel_app_user").collect() ## not sure this serves a purpose anymore 
    # session.sql(f"ALTER TABLE {table_name} SET DATA_RETENTION_TIME_IN_DAYS = 30 ").collect() ## DOES NOT WORK



#*#*#*#*#*#*#*#*#*#* Table Change/Delta Algorithms (order matters here!)

    target = session.table(table_name)

######## First check for columns with mismatched data types and drop them, THEY WILL BE ADDED BACK LATER, WE MUST DO IT THIS WAY BECASUE OF THE LIMITATIONS OF SNOWFLAKE DONT ALLOW ALL COLUMN TYPE CHANGES

    try:

        target_schema = target.schema
        flattened_schema = flattened_df.schema
        
        columns_to_drop = []
        for field in flattened_schema.fields:
            col_name = field.name
            if col_name in target.columns:
                target_type = str(target_schema[col_name].datatype).split('(')[0]
                new_type = str(field.datatype).split('(')[0]
                
                if target_type != new_type:
                    columns_to_drop.append(col_name)
                    session.sql(f"ALTER TABLE {table_name} DROP COLUMN {col_name}").collect()

        #### Delete Columns in Target, Not in Source 

        for i in target.columns :
            if i not in flattened_df.columns:

                session.sql(f"ALTER TABLE {table_name} DROP COLUMN {i}").collect()  

        ### In Between Step - create new df w/o new columns in flattened_df
        new_columns = set(flattened_df.columns) - set(target.columns) 

        flattened_df_temp = flattened_df.drop(new_columns)


        #### Merge logic (Delta Logic) #*#* ***ADD*** if there is anew column that is in flattened_df taht is nto in the target, you will run into an error - SOLUTION - you need ot crate a copy of flattened_df w/o taht column, then do the merge statements and then add the column later
                                    # FIRST PROBLEM ***ADD** suppose there is a column in the target that is not in flattened_df, then the column needs to be DELETED in the target first before any other operation can be carried out 
        #if there is a new column, the merge statement will not merge the vlaues in the new column, it only merges on matching columns 
        target.merge(
            flattened_df_temp, #*#*#* NEW has been changed to 'flattened_df_temp' 
            target["id"] == flattened_df_temp["id"],
            [ # 'update' statement updated cells, not rows -> 'instert' statement inserts row 
                WhenMatchedClause().update({c: flattened_df_temp[c] for c in flattened_df_temp.columns}), #*# this crates ONE dictionary with values {column_name : actual_column_values , ...} note - the seperation of each key-value pair by a comma is handled
                WhenNotMatchedClause().insert({c: flattened_df_temp[c] for c in flattened_df_temp.columns}),
            ]
        )

        #### Deletion Logic

        data_for_deletion = target.join(flattened_df,'id','leftanti')
        target.delete(target['id'].isin(data_for_deletion.select('id'))) # target['id'].isin(data_for_deletion.select('id') - this part of the code return the ENTIRE 'id' column will boolean statements in each value in the column

        #### Schema Drift Logic  #*#*#**# NEW  **ADD** There needs to be a line before this that deletes columns in the target that are not in the flattened_df ,OR ELSE the subtraction wouldnt include the relevant columns

        columns_to_add = new_columns.union(set(columns_to_drop))

        if columns_to_add:
            new_columns_df = flattened_df.select(['id'] + list(columns_to_add))
            updated_target_df = target.join(new_columns_df, 'id', 'left') #returns a df
            updated_target_df.write.mode("overwrite").save_as_table(table_name)
        
        # session.sql(f"GRANT SELECT ON TABLE {table_name} TO APPLICATION ROLE ilevel_app_user").collect()

        #*#* metadata logging 
        end_time_flattening_and_delta_logic = datetime.now()
        total_time_flattening_and_delta_logic = end_time_flattening_and_delta_logic - start_time_flattening_and_delta_logic

        ingestionPY_logger.info(f"Custom_ILEVEL_CONNECTOR - ingestionPY_logger - fetch_single_batch(): Total time to complete flattening and delta logic for resource {resource_name}: {total_time_flattening_and_delta_logic}")
        #*#*metadata logging 

        session.sql(f"GRANT ALL PRIVILEGES ON TABLE {table_name} TO APPLICATION ROLE ilevel_app_user").collect() ## allows to query but still cant delete this table unless it is done from within the app 

    except Exception as e:
        ingestionPY_logger.error(f"Custom_ILEVEL_CONNECTOR - ingestionPY_logger - fetch_single_batch(): Error occurred in delta logic: {e}")
        

    return ''




def ingest_data(session: Session, resource_id: str, database: str, schema: str ,table_name: str) -> str:

    fetch_single_batch(session, destination_db = database, schema = schema, resource_name = resource_id, table_name = table_name )

    return ''


#*#*# Task Creation 

def setting_task(session: Session, resource_id: str, database: str, schema: str, schedule: str, table_name: str, warehouse: str , execute_immediately: bool = True) -> str:  
    
            
    if table_name is None or table_name == "" or table_name == 'None':
        task_name = f"TASKS.INGEST_{escape_name(resource_id).upper()}"
    else: 
        task_name = f"TASKS.INGEST_{escape_name(table_name).upper()}"
    
    statements = [
        f"CREATE OR REPLACE TASK {task_name} "
        f"SCHEDULE = '{schedule}' "   
        f"WAREHOUSE = '{warehouse}' "
        f"AS CALL PUBLIC.INGEST_DATA('{resource_id}' , '{database}' ,'{schema}', '{table_name}')",   
        f"ALTER TASK {task_name} RESUME"
    ]

    # Add immediate execution if requested
    if execute_immediately:
        statements.append(f"EXECUTE TASK {task_name}")
    
    for cmd in statements:
        session.sql(cmd).collect()

    return ""

