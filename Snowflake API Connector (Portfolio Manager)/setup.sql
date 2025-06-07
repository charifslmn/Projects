create or alter versioned schema public; --versioned schema are only for native apps, allows you to track versions of the schema  
create schema if not exists state;
create schema if not exists tasks;

create schema if not exists ingested_data;

create application role if not exists ilevel_app_user;
grant usage on schema public to application role ilevel_app_user;
grant usage on schema tasks to application role ilevel_app_user; --####*#**#*#

grant usage on schema ingested_data to application role ilevel_app_user;



create table if not exists public.config_state_table(  -- "Yes" and "No" values, will be used to blur out tab in streamlit app if the value exists
    key string,
    value VARCHAR(44)
);

-- grant all privileges on table public.config_state_table to application role ilevel_app_user; --####*#**#*#



create or replace procedure public.validate_token()
returns boolean
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python', 'requests', 'streamlit','snowflake-telemetry-python')
imports = ('/snowflake_ilevel_connector.zip')
handler='snowflake_ilevel_connector.ingestion.validate_token';
grant usage on procedure public.validate_token() to application role ilevel_app_user;


create or replace procedure public.store_config_state()
returns string
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python', 'requests', 'streamlit','snowflake-telemetry-python')
imports = ('/snowflake_ilevel_connector.zip')
handler='snowflake_ilevel_connector.ingestion.store_config_state';
grant usage on procedure public.store_config_state() to application role ilevel_app_user;


create or replace procedure public.setting_task(resource_id string, database string, schema string, schedule string, table_name string,warehouse string, execute_immediately boolean)
returns string
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python', 'requests', 'streamlit','snowflake-telemetry-python')
imports = ('/snowflake_ilevel_connector.zip')
handler='snowflake_ilevel_connector.ingestion.setting_task';
grant usage on procedure public.setting_task(string, string, string, string, string,string, boolean ) to application role ilevel_app_user;


create or replace procedure public.create_secrets(client_id string, client_secret string)
returns string
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python', 'requests', 'streamlit','snowflake-telemetry-python')
imports = ('/snowflake_ilevel_connector.zip')
handler='snowflake_ilevel_connector.ingestion.create_secrets';
grant usage on procedure public.create_secrets(string, string) to application role ilevel_app_user;


create or replace procedure public.ingest_data(resource_id string, database string, schema string, table_name string)
returns string
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python', 'requests', 'streamlit','snowflake-telemetry-python')
imports = ('/snowflake_ilevel_connector.zip')
handler='snowflake_ilevel_connector.ingestion.ingest_data';
grant usage on procedure public.ingest_data(string, string, string, string) to application role ilevel_app_user; --#*#**#*#*#

create or replace procedure public.provision(destination_db string,schema string)
returns string
language python
runtime_version = '3.8'
packages = ('snowflake-snowpark-python', 'requests', 'streamlit','snowflake-telemetry-python')
imports = ('/snowflake_ilevel_connector.zip')
handler='snowflake_ilevel_connector.ingestion.provision';
grant usage on procedure public.provision(string, string) to application role ilevel_app_user; -- #*#*#*


CREATE or replace PROCEDURE PUBLIC.REGISTER_REFERENCE(ref_name STRING, operation STRING, ref_or_alias STRING) --#*#*# code for warehouse_refrence - see manifest.yml file 
    RETURNS STRING
    LANGUAGE SQL
AS $$
    BEGIN
        CASE (operation)
            WHEN 'ADD' THEN
                SELECT SYSTEM$SET_REFERENCE(:ref_name, :ref_or_alias);
            WHEN 'REMOVE' THEN
                SELECT SYSTEM$REMOVE_REFERENCE(:ref_name);
            WHEN 'CLEAR' THEN
                SELECT SYSTEM$REMOVE_REFERENCE(:ref_name);
            ELSE RETURN 'unknown operation: ' || operation;
        END CASE;
        RETURN NULL;
    END;
$$;

GRANT USAGE ON PROCEDURE PUBLIC.REGISTER_REFERENCE(STRING, STRING, STRING)
  TO APPLICATION ROLE ilevel_app_user;


 -- #*#*#**#*#*# NEWWWW

-- Configuration callback for the `EXTERNAL_ACCESS_REFERENCE` defined in the manifest.yml
-- The procedure returns a json format object containing information about the EAI to be created
-- and shows the same information in a popup-window in the UI.
-- Allows secrets since the iLevel API requires client credentials authentication
CREATE or replace procedure public.get_configuration(ref_name STRING)
RETURNS STRING
LANGUAGE SQL
AS 
$$
BEGIN
  CASE (UPPER(ref_name))
      WHEN 'EXTERNAL_ACCESS_REFERENCE' THEN
          RETURN OBJECT_CONSTRUCT(
              'type', 'CONFIGURATION',
              'payload', OBJECT_CONSTRUCT(
                  -- Changed from api.coincap.io to api.ilevelsolutions.com:443 based on iLevel API endpoint seen in ingestion.py
                  'host_ports', ARRAY_CONSTRUCT('api.ilevelsolutions.com:443'),
                  -- Changed from 'NONE' to 'ALL' since iLevel requires client_id and client_secret authentication
                  -- This matches the ALLOWED_AUTHENTICATION_SECRETS setting in install.sql
                  'allowed_secrets', 'ALL')
          )::STRING;
      ELSE
          RETURN '';
  END CASE;
END;	
$$;

-- Grants usage to ilevel_app_user role which is used throughout the application
GRANT USAGE ON PROCEDURE public.get_configuration(STRING) TO APPLICATION ROLE ilevel_app_user;


 -- #*#*#**#*#*# NEWWWW




create or replace streamlit public."Configuration" from '/'
main_file = 'streamlit_app.py';

grant usage on streamlit public."Configuration" to application role ilevel_app_user;



