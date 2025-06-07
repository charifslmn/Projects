from snowflake.snowpark import Session
import streamlit as st
import base64
import pandas as pd
from snowflake.snowpark.exceptions import SnowparkSQLException


class APP_FUNCTIONS:
    def __init__(self, session: Session):
        self._session = session
        self.app_name = self._session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]

    def reset_credentials(self) -> None:

        self._session.sql(" DELETE FROM public.config_state_table WHERE key IN ('client_id', 'client_secret') ").collect()
        self._session.sql(""" DROP SECRET IF EXISTS PUBLIC.CLIENT_ID """).collect()
        self._session.sql(""" DROP SECRET IF EXISTS PUBLIC.CLIENT_SECRET """).collect()

    def create_secrets_st(self):
        # fdfdfd ### USED TO TEST ERROR LOGGING, TEST PASSED 
        self._session.sql(f"call public.create_secrets('{st.session_state.id_key}', '{st.session_state.secret_key}')").collect()
        self._session.sql(f"call public.store_config_state()").collect()
        st.success("Credentials stored successfully")
        st.experimental_rerun()

    def validate_credentials(self):
        result = self._session.sql("call public.validate_token()").collect()
        if result[0][0] is None:
            st.error("Invalid credentials, please reset credentials and try again")
        else:
            st.success("Credentials validated successfully")

    def validate_credentials_general(self):
        result = self._session.sql("call public.validate_token()").collect()
        return result[0][0]
    
    def is_valid_table_name(self, table_name: str) -> bool:

        invalid_chars = set(table_name) - (set("_") | set('$') | set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"))
        if invalid_chars:
            return False

        if table_name == ' ':
            return False
        
        if table_name == '':
            return False
        
        if table_name[0] == '$' or table_name[0].isdigit():
            return False

        if ' ' in table_name:
            return False

        return True
    
    def delete_tasks(self, tasks_to_delete):
        for task in tasks_to_delete:
            try:
                self._session.sql(f""" DROP TASK IF EXISTS {self.app_name}.TASKS.{task} """).collect()
            except Exception as e:
                st.error(f"Error deleting task {task}: {e}")
                

    def render_image(self, filepath: str):
        mime_type = filepath.split('.')[-1:][0].lower() 
        with open(filepath, "rb") as f:
            content_bytes = f.read()
            content_b64encoded = base64.b64encode(content_bytes).decode()
            image_string = f'data:image/{mime_type};base64,{content_b64encoded}'

            return st.image(image_string, use_column_width="auto")
        
    # @st.cache_data
    def get_schemas_for_database(_self, database_name):
        try:
            if database_name == _self.app_name:
                schema_names = ["INGESTED_DATA"]
            else:
                schemas = _self._session.sql(f"SHOW SCHEMAS IN DATABASE {database_name}").collect()
                schema_names = [schema['name'] for schema in schemas if schema['name'] != 'INFORMATION_SCHEMA']
                    
            return schema_names
        except Exception as e:
            st.error(f"Error fetching schemas: {e}")

    # @st.cache_data
    def get_tasks(_self):
        tasks = _self._session.sql(f""" SHOW TASKS IN APPLICATION {_self.app_name} """).collect()
        tasks_df = pd.DataFrame(tasks)
        
        if tasks_df.empty:
            return pd.DataFrame()  
            
        tasks_df.columns = [col.lower() for col in tasks_df.columns]
        selected_columns = ['name','schedule', 'created_on', 'state','database_name', 'schema_name','owner_role_type' , 'last_suspended_reason','id']
        tasks_df = tasks_df[selected_columns]
        tasks_df.columns = [col.upper() for col in tasks_df.columns]
        
        return tasks_df


    # @st.cache_data
    def get_accessible_databases(_self):
        databases = _self._session.sql("SHOW DATABASES").collect()  # only collects databases that have been granted to the application
        
        db_names = [db['name'] for db in databases if db['kind'] == 'STANDARD']
        
        # Manually add the app database
        db_names.append(_self.app_name)
        
        return db_names

    # @st.cache_data
    def get_tables(_self):
        all_table_names = []
        # Get all accessible databases
        databases = _self.get_accessible_databases()
        
        # Query tables in each accessible database
        for database in databases:
            tables = _self._session.sql(f"""
                SELECT * 
                FROM "{database}".information_schema.tables 
                WHERE table_schema NOT IN ('INFORMATION_SCHEMA')
                AND table_owner = '{_self.app_name}'
                AND table_name != 'CONFIG_STATE_TABLE'
            """).collect()
            
            # Extract fully qualified table names from result
            db_table_names = [table['TABLE_CATALOG'] + '.' + table['TABLE_SCHEMA'] + '.' + table['TABLE_NAME'] for table in tables]

            all_table_names.extend(db_table_names)
        
        return all_table_names


    # When show_current_only=False, this will show all tasks in SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY that are associated with 
    # the ILEVEL_PYTHON_CONNECTOR_INSTANCE database. It does not show tasks from other databases/applications in the Snowflake account.
    # @st.cache_data
    def get_task_history(_self, selected_tasks=None, show_current_only=False):
        if show_current_only:
            # Here we match the task IDs between current tasks and historical tasks
            # First get current task IDs from TASKS schema
            current_tasks = _self.get_tasks()
            # Access id column using DataFrame syntax since current_tasks is a DataFrame
            task_ids = current_tasks['ID'].tolist()
            
            # Get history only for current tasks by matching ROOT_TASK_ID with current task IDs
            query = f"""
                SELECT th.*
                FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY th
                WHERE th.DATABASE_NAME = '{_self.app_name}'
                AND th.ROOT_TASK_ID IN ({','.join(f"'{id}'" for id in task_ids)})"""
                
            if selected_tasks:
                task_names_str = "'" + "','".join(selected_tasks) + "'"
                query += f" AND th.NAME IN ({task_names_str})"
        else:
            # Get all task history
            query = f"""
                SELECT *
                FROM SNOWFLAKE.ACCOUNT_USAGE.TASK_HISTORY 
                WHERE DATABASE_NAME = '{_self.app_name}'
            """
            
            if selected_tasks:
                task_names_str = "'" + "','".join(selected_tasks) + "'"
                query += f" AND NAME IN ({task_names_str})"
        
        query += " ORDER BY SCHEDULED_TIME DESC"
        
        task_history = _self._session.sql(query).collect()
        return task_history

    def manage_task_status(self, task_name: str, action: str):
        
        if action == "pause":
            self._session.sql(f"""ALTER TASK {self.app_name}.TASKS.{task_name} SUSPEND """).collect()
        elif action == "resume":
            self._session.sql(f"""ALTER TASK {self.app_name}.TASKS.{task_name} RESUME """).collect()
            


    def delete_tables(self, tables_to_delete):
        for table in tables_to_delete:
            try:
                qualified_name = '.'.join([f'"{part}"' for part in table.split('.')])
                self._session.sql(f"""DROP TABLE IF EXISTS {qualified_name} """).collect()

            except Exception as e:
                st.error(f"Error deleting table {table}: {e}")

    ### Scheduling Functions ###

    # Function to generate a CRON expression combining all selected hours and days
    def generate_custom_cron_expression(self, weekdays=None, monthdays=None, hours_of_day=None) -> str:
        if not hours_of_day:
            return ""

        # Format hours and days as comma-separated values
        cron_hours = ','.join(map(str, sorted(hours_of_day)))
        cron_minutes = "0"  # Fixed at 0 minutes to run at the top of each selected hour
        
        if weekdays:  # Weekly schedule
            weekday_nums = ','.join(str(day[1] % 7) for day in weekdays)  #*#*#* modular arithmetic is not needed here 
            cron_expression = f"{cron_minutes} {cron_hours} * * {weekday_nums}"
        elif monthdays:  # Monthly schedule
            monthdays_str = ','.join(map(str, sorted(monthdays)))
            cron_expression = f"{cron_minutes} {cron_hours} {monthdays_str} * *"
        else:
            cron_expression = ""
        
        return cron_expression


    # Helper function to convert basic schedules into cron expressions
    def generate_basic_cron_expression(self, time_unit: str, schedule_value: int) -> str:
        if time_unit == "Minute":
            return f"*/{schedule_value} * * * *"  # Run every N minutes (e.g. */5 means every 5 minutes)
        elif time_unit == "Hour":
            return f"0 */{schedule_value} * * *"  # Run at minute 0 of every N hours (e.g. 0 */2 means 12am, 2am, 4am etc)
        elif time_unit == "Day":
            return f"0 0 */{schedule_value} * *"  # Run at 12:00 AM (00:00) every N days
        elif time_unit == "Week":
            return f"0 0 * * */{schedule_value}"  # Run at 12:00 AM (00:00) every N weeks on Sunday
        elif time_unit == "Month":
            return f"0 0 1 */{schedule_value} *"  # Run at 12:00 AM (00:00) on the 1st of every N months
        else:
            return ""
        

        
    def ingest_all_data(self):

        resource_name_mapping = { "Assets": "assets","Funds": "funds","Periodic Data": "periodicData","Cash Transactions": "cashTransactions", "Securities": "securities","Currencies": "currencies","Deals": "deals","Cash Transaction Types": "cashTransactionTypes",
        "FX Rates": "fxRates","Entity Group Categories": "entityGroupCategories", "Benchmarks": "benchmarks", "Valuation Data Items": "valuationDataItems"
    }
        resource_name_mapping_reverse = {v: k for k, v in resource_name_mapping.items()}

        selected_friendly_names = st.session_state.get('resource_names', [])
        
        selected_resources = [resource_name_mapping[name] for name in selected_friendly_names]

        for resource in selected_resources:
            destination_database = st.session_state.get('destination_database')
            schema = st.session_state.get('schema_key')
            cron_expression = st.session_state.get(f'schedule_{resource}')
            execute_immediately = st.session_state.get(f'execute_timing_{resource}') == "Execute Immediately"
            table_name = st.session_state.get(f"custom_tbl_name_{resource}")
            warehouse = st.session_state.get(f"warehouse_{resource}")
            timezone = st.session_state.get(f"timezone_{resource}")

            if cron_expression:
                try:
                    self._session.sql(f"""
                        call public.setting_task('{resource}', '{destination_database}', '{schema}', 'USING CRON {cron_expression} {timezone}', '{table_name or None}', '{warehouse}', {execute_immediately})
                    """).collect()
                    st.success(f"Task has been created for {resource_name_mapping_reverse[resource]} with schedule: {cron_expression}")
                    
                except SnowparkSQLException as e:
                    if "Invalid schedule was specified" in str(e):
                        st.error(f"Invalid CRON schedule specified for {resource}. Please enter a valid CRON expression.")
                    else:
                        st.error(str(e))

    def get_warehouse(self):
        warehouse = self._session.sql("SHOW WAREHOUSES").collect()
        wh_names = [wh['name'] for wh in warehouse]
        return wh_names

    def run_task_now(self, task_name: str):

        self._session.sql(f""" EXECUTE TASK {self.app_name}.TASKS.{task_name} """).collect()

    def generate_cron_expression(minute, hour, day_of_month, month, day_of_week):
        """
        Converts user inputs into a cron expression.
        """
        return f"{minute} {hour} {day_of_month} {month} {day_of_week}"

