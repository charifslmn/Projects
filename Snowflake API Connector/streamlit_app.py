import streamlit as st
from snowflake.snowpark.context import get_active_session
import snowflake.permissions as permissions
import _snowflake
import pandas as pd
import re
import requests
import json
import base64  
import logging
from snowflake.snowpark.exceptions import SnowparkSQLException
from common import APP_FUNCTIONS  # Import the class
from cron_descriptor import get_description




# Configure session logging
appPY_logger = logging.getLogger('snowflake.snowpark.session')



st.set_page_config(layout="wide")



if not permissions.get_reference_associations("external_access_reference"): #**#*# NEWWWW
    permissions.request_reference("external_access_reference")




if permissions.get_missing_account_privileges(["CREATE DATABASE", "EXECUTE TASK", "IMPORTED PRIVILEGES ON SNOWFLAKE DB"]):
    permissions.request_account_privileges(["CREATE DATABASE", "EXECUTE TASK", "IMPORTED PRIVILEGES ON SNOWFLAKE DB"]) ##this is why we have the register_single_refernce in the set up files

# if not permissions.is_application_authorized_for_telemetry_event_sharing() and permissions.is_application_all_mandatory_telemetry_event_definitions_enabled():
#      permissions.request_event_sharing() # Failed to enable event sharing: 093320 (0A000): AUTHORIZE_TELEMETRY_EVENT_SHARING can only be set/unset if the application is created in a different account from the application package.



#toronto index is 74
timezone_options = ["Europe/Andorra", "Asia/Dubai", "Asia/Kabul", "Europe/Tirane", "Asia/Yerevan", "Antarctica/Casey", "Antarctica/Davis", "Antarctica/DumontDUrville", "Antarctica/Mawson", "Antarctica/Palmer", "Antarctica/Rothera", "Antarctica/Syowa", "Antarctica/Troll", "Antarctica/Vostok", "America/Argentina/Buenos_Aires", "America/Argentina/Cordoba", "America/Argentina/Salta", "America/Argentina/Jujuy", "America/Argentina/Tucuman", "America/Argentina/Catamarca", "America/Argentina/La_Rioja", "America/Argentina/San_Juan", "America/Argentina/Mendoza", "America/Argentina/San_Luis", "America/Argentina/Rio_Gallegos", "America/Argentina/Ushuaia", "Pacific/Pago_Pago", "Europe/Vienna", "Australia/Lord_Howe", "Antarctica/Macquarie", "Australia/Hobart", "Australia/Melbourne", "Australia/Sydney", "Australia/Broken_Hill", "Australia/Brisbane", "Australia/Lindeman", "Australia/Adelaide", "Australia/Darwin", "Australia/Perth", "Australia/Eucla", "Asia/Baku", "America/Barbados", "Asia/Dhaka", "Europe/Brussels", "Europe/Sofia", "Atlantic/Bermuda", "Asia/Brunei", "America/La_Paz", "America/Noronha", "America/Belem", "America/Fortaleza", "America/Recife", "America/Araguaina", "America/Maceio", "America/Bahia", "America/Sao_Paulo", "America/Campo_Grande", "America/Cuiaba", "America/Santarem", "America/Porto_Velho", "America/Boa_Vista", "America/Manaus", "America/Eirunepe", "America/Rio_Branco", "America/Nassau", "Asia/Thimphu", "Europe/Minsk", "America/Belize", "America/St_Johns", "America/Halifax", "America/Glace_Bay", "America/Moncton", "America/Goose_Bay", "America/Blanc-Sablon", "America/Toronto", "America/Nipigon", "America/Thunder_Bay", "America/Iqaluit", "America/Pangnirtung", "America/Atikokan", "America/Winnipeg", "America/Rainy_River", "America/Resolute", "America/Rankin_Inlet", "America/Regina", "America/Swift_Current", "America/Edmonton", "America/Cambridge_Bay", "America/Yellowknife", "America/Inuvik", "America/Creston", "America/Dawson_Creek", "America/Fort_Nelson", "America/Whitehorse", "America/Dawson", "America/Vancouver", "Indian/Cocos", "Europe/Zurich", "Africa/Abidjan", "Pacific/Rarotonga", "America/Santiago", "America/Punta_Arenas", "Pacific/Easter", "Asia/Shanghai", "Asia/Urumqi", "America/Bogota", "America/Costa_Rica", "America/Havana", "Atlantic/Cape_Verde", "America/Curacao", "Indian/Christmas", "Asia/Nicosia", "Asia/Famagusta", "Europe/Prague", "Europe/Berlin", "Europe/Copenhagen", "America/Santo_Domingo", "Africa/Algiers", "America/Guayaquil", "Pacific/Galapagos", "Europe/Tallinn", "Africa/Cairo", "Africa/El_Aaiun", "Europe/Madrid", "Africa/Ceuta", "Atlantic/Canary", "Europe/Helsinki", "Pacific/Fiji", "Atlantic/Stanley", "Pacific/Chuuk", "Pacific/Pohnpei", "Pacific/Kosrae", "Atlantic/Faroe", "Europe/Paris", "Europe/London", "Asia/Tbilisi", "America/Cayenne", "Africa/Accra", "Europe/Gibraltar", "America/Nuuk", "America/Danmarkshavn", "America/Scoresbysund", "America/Thule", "Europe/Athens", "Atlantic/South_Georgia", "America/Guatemala", "Pacific/Guam", "Africa/Bissau", "America/Guyana", "Asia/Hong_Kong", "America/Tegucigalpa", "America/Port-au-Prince", "Europe/Budapest", "Asia/Jakarta", "Asia/Pontianak", "Asia/Makassar", "Asia/Jayapura", "Europe/Dublin", "Asia/Jerusalem", "Asia/Kolkata", "Indian/Chagos", "Asia/Baghdad", "Asia/Tehran", "Atlantic/Reykjavik", "Europe/Rome", "America/Jamaica", "Asia/Amman", "Asia/Tokyo", "Africa/Nairobi", "Asia/Bishkek", "Pacific/Tarawa", "Pacific/Enderbury", "Pacific/Kiritimati", "Asia/Pyongyang", "Asia/Seoul", "Asia/Almaty", "Asia/Qyzylorda", "Asia/Qostanay", "Asia/Aqtobe", "Asia/Aqtau", "Asia/Atyrau", "Asia/Oral", "Asia/Beirut", "Asia/Colombo", "Africa/Monrovia", "Europe/Vilnius", "Europe/Luxembourg", "Europe/Riga", "Africa/Tripoli", "Africa/Casablanca", "Europe/Monaco", "Europe/Chisinau", "Pacific/Majuro", "Pacific/Kwajalein", "Asia/Yangon", "Asia/Ulaanbaatar", "Asia/Hovd", "Asia/Choibalsan", "Asia/Macau", "America/Martinique", "Europe/Malta", "Indian/Mauritius", "Indian/Maldives", "America/Mexico_City", "America/Cancun", "America/Merida", "America/Monterrey", "America/Matamoros", "America/Mazatlan", "America/Chihuahua", "America/Ojinaga", "America/Hermosillo", "America/Tijuana", "America/Bahia_Banderas", "Asia/Kuala_Lumpur", "Asia/Kuching", "Africa/Maputo", "Africa/Windhoek", "Pacific/Noumea", "Pacific/Norfolk", "Africa/Lagos", "America/Managua", "Europe/Amsterdam", "Europe/Oslo", "Asia/Kathmandu", "Pacific/Nauru", "Pacific/Niue", "Pacific/Auckland", "Pacific/Chatham", "America/Panama", "America/Lima", "Pacific/Tahiti", "Pacific/Marquesas", "Pacific/Gambier", "Pacific/Port_Moresby", "Pacific/Bougainville", "Asia/Manila", "Asia/Karachi", "Europe/Warsaw", "America/Miquelon", "Pacific/Pitcairn", "America/Puerto_Rico", "Asia/Gaza", "Asia/Hebron", "Europe/Lisbon", "Atlantic/Madeira", "Atlantic/Azores", "Pacific/Palau", "America/Asuncion", "Asia/Qatar", "Indian/Reunion", "Europe/Bucharest", "Europe/Belgrade", "Europe/Kaliningrad", "Europe/Moscow", "Europe/Simferopol", "Europe/Kirov", "Europe/Volgograd", "Europe/Astrakhan", "Europe/Saratov", "Europe/Ulyanovsk", "Europe/Samara", "Asia/Yekaterinburg", "Asia/Omsk", "Asia/Novosibirsk", "Asia/Barnaul", "Asia/Tomsk", "Asia/Novokuznetsk", "Asia/Krasnoyarsk", "Asia/Irkutsk", "Asia/Chita", "Asia/Yakutsk", "Asia/Khandyga", "Asia/Vladivostok", "Asia/Ust-Nera", "Asia/Magadan", "Asia/Sakhalin", "Asia/Srednekolymsk", "Asia/Kamchatka", "Asia/Anadyr", "Asia/Riyadh", "Pacific/Guadalcanal", "Indian/Mahe", "Africa/Khartoum", "Europe/Stockholm", "Asia/Singapore", "America/Paramaribo", "Africa/Juba", "Africa/Sao_Tome", "America/El_Salvador", "Asia/Damascus", "America/Grand_Turk", "Africa/Ndjamena", "Indian/Kerguelen", "Asia/Bangkok", "Asia/Dushanbe", "Pacific/Fakaofo", "Asia/Dili", "Asia/Ashgabat", "Africa/Tunis", "Pacific/Tongatapu", "Europe/Istanbul", "America/Port_of_Spain", "Pacific/Funafuti", "Asia/Taipei", "Europe/Kiev", "Europe/Uzhgorod", "Europe/Zaporozhye", "Pacific/Wake", "America/New_York", "America/Detroit", "America/Kentucky/Louisville", "America/Kentucky/Monticello", "America/Indiana/Indianapolis", "America/Indiana/Vincennes", "America/Indiana/Winamac", "America/Indiana/Marengo", "America/Indiana/Petersburg", "America/Indiana/Vevay", "America/Chicago", "America/Indiana/Tell_City", "America/Indiana/Knox", "America/Menominee", "America/North_Dakota/Center", "America/North_Dakota/New_Salem", "America/North_Dakota/Beulah", "America/Denver", "America/Boise", "America/Phoenix", "America/Los_Angeles", "America/Anchorage", "America/Juneau", "America/Sitka", "America/Metlakatla", "America/Yakutat", "America/Nome", "America/Adak", "Pacific/Honolulu", "America/Montevideo", "Asia/Samarkand", "Asia/Tashkent", "America/Caracas", "Asia/Ho_Chi_Minh", "Pacific/Efate", "Pacific/Wallis", "Pacific/Apia", "Africa/Johannesburg"]


session = get_active_session()
app_func = APP_FUNCTIONS(session)  # Create instance of the class

# session.sql("ALTER PROCEDURE PUBLIC.CREATE_SECRETS(STRING, STRING) SET EXTERNAL_ACCESS_INTEGRATIONS = (ilevel_integration)").collect() #*#**#*#*#*

# ingestionPY_logger.info("Custom_ILEVEL_CONNECTOR - ingestionPY_logger - create_secrets(): Client ID and Client Secret Created")

def provision():

    session = get_active_session()
    session.sql(f"call public.provision('{st.session_state.destination_database}','{st.session_state.schema_key}')").collect()


        

resource_name_mapping = { "Assets": "assets","Funds": "funds","Periodic Data": "periodicData","Cash Transactions": "cashTransactions", "Securities": "securities","Currencies": "currencies","Deals": "deals","Cash Transaction Types": "cashTransactionTypes",
        "FX Rates": "fxRates","Entity Group Categories": "entityGroupCategories", "Benchmarks": "benchmarks", "Valuation Data Items": "valuationDataItems"
    }


# UI

# CSS to style only the sidebar buttons 
sidebar_style = """
    <style>
    [data-testid="stSidebar"] .stButton button {
        width: 100%;
        text-align: center;
    }
    </style>
"""
st.markdown(sidebar_style, unsafe_allow_html=True)

hide_img_fs = '''
<style>
button[title="View fullscreen"]{
    visibility: hidden;}
</style>
'''
st.markdown(hide_img_fs, unsafe_allow_html=True)






def main():

    session = get_active_session()
    app_func = APP_FUNCTIONS(session)  # Create instance of the class

    try:
        client_id_state = session.sql("SELECT value FROM public.config_state_table WHERE key = 'client_id' ").collect()
        client_secret_state = session.sql("SELECT value FROM public.config_state_table WHERE key = 'client_secret' ").collect()
    except Exception as e:
        appPY_logger.error(f" Custom_ILEVEL_CONNECTOR - streamlit_appPY_logger - client_id_state & client_secret_state fetch via session.sql(): {str(e)}")

    try:
        db_names = app_func.get_accessible_databases()  # for database selection
    except Exception as e:
        appPY_logger.error(f" Custom_ILEVEL_CONNECTOR - streamlit_appPY_logger - app_func.get_accessible_databases(): {str(e)}")
        

    with st.sidebar:

        try:
            app_func.render_image("logo.png")
        except Exception as e:
            appPY_logger.error(f" Custom_ILEVEL_CONNECTOR - streamlit_appPY_logger - app_func.render_image(): {str(e)}")

    st.session_state.client_id_state_st = bool(client_id_state)
    st.session_state.client_secret_state_st = bool(client_secret_state)

    # if not permissions.get_reference_associations("warehouse_reference"): 
    #     permissions.request_reference("warehouse_reference")

    if "active_page" not in st.session_state:
        st.session_state["active_page"] = "Overview and Setup"  

    if st.sidebar.button("Overview and Setup"):
        st.session_state["active_page"] = "Overview and Setup"

    # Sidebar buttons for navigation
    if st.sidebar.button("Store Client Credentials"):
        st.session_state["active_page"] = "Store Client Credentials"

    if st.sidebar.button("Data Ingestion"):
        st.session_state["active_page"] = "Data Ingestion"

    if st.sidebar.button("Tasks"):
        st.session_state["active_page"] = "Tasks"

    if st.sidebar.button("Tables"):
        st.session_state["active_page"] = "Tables"

    # Display the appropriate form based on active page
    if st.session_state["active_page"] == "Overview and Setup":

        # if st.button("test", key = "test"): ## error here , DNE or not authorized 
        #     session.sql( "ALTER TABLE ilevel_connector.public.assets SET DATA_RETENTION_TIME_IN_DAYS = 30 " ).collect()

        st.title("iLevel Connector")

        
        st.markdown("""

        ##### Overview
                    
        The iLevel Connector is a native Snowflake application that enables seamless integration between iLevel and your Snowflake environment. This connector is designed to ingest data from your iLevel environment into your Snowflake environment via scheduled tasks. 
                    
        - **Store Client Credentials**: Use the **Store Client Credentials** tab to securely input your iLevel API credentials (client ID and secret).
        - **Configure Data Ingestion**: Navigate to the **Data Ingestion** tab to select resources and configure ingestion schedules. For each selected resouce you choose to ingest, the data ingestion merges the data from the selected resouce into a table in Snowflake. Upon the first ingestion, the table will be created. The following ingestions will merge the data to the table so that only the latest version.
        - **Monitor and Manage Tasks**: Use the **Tasks** tab to view, pause, resume, or delete ingestion tasks.
        - **Manage Tables**: Access the **Tables** tab to view and manage created tables.
        
        ---   
                    
        ##### Access Controls 
                    
        - The **application role** for this app is named **ilevel_app_user**
        - The **application role** can only be granted to another role (not directly to a user) to allow them to access the application. 
        - For detailed documentation, visit the [Snowflake Access Controls Documentation](https://docs.snowflake.com/en/sql-reference/sql/grant-application-role).

        ---
                    
        ##### Enable Logs and Traces
        
        - This application uses custom logging and tracing to monitor the app functionality and improve performance. The logs and traces only collect metadata about the ingestion process and do not store any sensitive data. For example, the logs and traces will show the number of rows ingested, the time taken, and the status of the ingestion process.
        - To enable the providers of the application to access logs and traces emitted from the application, this functionalty must be enabled by the user. The steps are as follows:
            - First, the user must set up an event table in their snowflake account. This process is explained in the snowflake documentation here: [Set up an event table](https://other-docs.snowflake.com/en/native-apps/consumer-enable-logging#set-up-an-event-table)
            - Next, the user must enable event sharing for the iLevel Connector application. This process is explained in the snowflake documentation here: [Enable event sharing for an app](https://other-docs.snowflake.com/en/native-apps/consumer-enable-logging#enable-event-sharing-for-an-app)
                    
        ##### Need Help?
        For support, questions, or further assistance, please contact **support@avaxcc.com**.

    """)



    elif st.session_state["active_page"] == "Store Client Credentials":
        st.header("Store Client Credentials")


        with st.expander("**Instructions**", expanded = False):
                st.markdown("""
                Any iLEVEL user account can be granted access to the iLEVEL API. Follow these steps to generate client credentials:

                ##### Steps  
                - Log into iLEVEL and Navigate to the 'Users' tab
                - Select the specific user requiring API access
                - Navigate to the 'API Credentials' tab
                - Select 'Generate' to create client_id and client_secret
                ---
                ##### Important Notes
                - The client_secret will only be displayed ONCE at generation
                - Save the client_secret immediately when generated
                - If client_secret is lost, delete and regenerate new credentials
                - Credentials can be generated and deleted as needed
                            
                """)

        with st.form("connector_form3"):
            client_id = st.text_input("Client ID", key="id_key", disabled=st.session_state.client_id_state_st, type="password")
            client_secret = st.text_input("Client Secret", key="secret_key", disabled=st.session_state.client_secret_state_st, type="password")
            submitted = st.form_submit_button("Store Credentials")
            if submitted:
                if not client_id or not client_secret:
                    st.error("Please enter both Client ID and Client Secret")
                else:
                    try:
                        app_func.create_secrets_st()
                    except SnowparkSQLException as e:
                        # This will catch any SQL-related errors from Snowpark operations
                        if "Insufficient privileges to operate on integration 'ILEVEL_INTEGRATION'" in str(e):
                            st.error(""" Insufficient privileges to operate on integration 'ILEVEL_INTEGRATION'. Please check instructions in the "Overview and Setup" tab to grant the application the required privileges.""")
                        elif("Integration 'ILEVEL_INTEGRATION' does not exist or not authorized" in str(e)):
                            st.error(""" Integration 'ILEVEL_INTEGRATION' does not exist or not authorized. Please check instructions in the "Overview and Setup" tab to create the integration and grant the application the required privileges.""")
                        else:
                            st.error(f"Failed to store credentials: {str(e)}")
                    except Exception as e:
                        st.error(f"An unexpected error occurred: {str(e)}")
                        appPY_logger.error(f"Custom_ILEVEL_CONNECTOR - streamlit_appPY_logger - create_secrets_st(): {str(e)}")

        if st.session_state.client_id_state_st or st.session_state.client_secret_state_st:

            col1, col2 = st.columns([1, 0.3])

            with col1:
                
                if st.button("Validate Credentials", type="primary"):
                    app_func.validate_credentials() 
                    
            with col2:

                if st.button("Reset Credentials"):
                    try:
                        app_func.reset_credentials()  # Use the class method instead
                        st.session_state.client_id_state_st = False
                        st.session_state.client_secret_state_st = False
                        st.session_state["continue_clicked"] = False
                        # st.success("Credentials have been reset. You can now re-enter the client ID and secret.") #*#*# this does not show up when the button is clicked 
                        st.experimental_rerun()
                    except Exception as e:
                        appPY_logger.error(f"Custom_ILEVEL_CONNECTOR - streamlit_appPY_logger - app_func.reset_credentials() : {str(e)}")
       

    elif st.session_state["active_page"] == "Data Ingestion":
        
        col1, col2 = st.columns([1, 0.2])  # Adjust column widths as needed

        with col1:
            st.header("Data Ingestion")

        with col2:
            refresh_clicked = st.button("⟳ Refresh",type="primary")
            if refresh_clicked:
                st.session_state["refresh_clicked"] = True
                st.session_state["resource_names"] = []  # Clear the multiselect when refresh is clicked
                st.experimental_rerun()

        with st.expander ("**Instructions**" , expanded = False) : 
            st.markdown("""
                        
        - Data can be ingested into the **INGESTED_DATA** schema within the application (note that the application can function as a database).
        - Alternatively, you can ingest data into a custom database and schema of your choosing as explained below. 
        - Once the application has the required permissions to acess a database of your choosing, your selected custom databases and schemas will appear below.
        --- 
                        
        To allow the application to access a specific database and schema, execute the following commands in a SQL worksheet:

        ```
        GRANT ALL PRIVILEGES ON DATABASE <YOUR_DATABASE> TO APPLICATION <APP_NAME>;
        GRANT ALL PRIVILEGES ON SCHEMA <YOUR_DATABASE>.<YOUR_SCHEMA> TO APPLICATION <APP_NAME>;
        ```

        Replace `<YOUR_DATABASE>` and `<YOUR_SCHEMA>` with the names of the database and schema you want to grant access to.

        After granting the required permissions, click the **Refresh** button to reload the application. The list of databases will update to include the custom databases you have selected.
            
                        """)

        # Check if privileges are already granted 
        # privileges_needed = permissions.get_missing_account_privileges([ "CREATE DATABASE", "EXECUTE TASK", "IMPORTED PRIVILEGES ON SNOWFLAKE DB" ])

        # # Button will be blue (primary) and disabled if no privileges are needed
        # if st.button( "Request Account Privileges", type="primary",
        #     disabled=not privileges_needed ):
        #     request_account_privileges_fn()
        try:
            db_names = app_func.get_accessible_databases()
        except Exception as e: 
            appPY_logger.error(f"Custom_ILEVEL_CONNECTOR - streamlit_appPY_logger - app_func.get_accessible_databases() : {str(e)}")
            
        if not db_names:
            return st.warning("See instructions")

        with st.expander("**Choose Database and Schema**", expanded = True):
            # Store previous values to detect changes
            prev_db = st.session_state.get("prev_database")
            prev_schema = st.session_state.get("prev_schema")
            
            selected_database = st.selectbox("Select Database", options=db_names, key="destination_database")
            
            try:
                schemas = app_func.get_schemas_for_database(selected_database)
            except Exception as e:
                appPY_logger.error(f"Custom_ILEVEL_CONNECTOR - streamlit_appPY_logger - app_func.get_schemas_for_database() : {str(e)}")
            
            selected_schema = st.selectbox("Select Schema", options=schemas, key="schema_key", disabled=not selected_database)

            # Check if database or schema changed
            if selected_database != prev_db or selected_schema != prev_schema:
                if "continue_clicked" in st.session_state:
                    del st.session_state["continue_clicked"]
                st.session_state["prev_database"] = selected_database
                st.session_state["prev_schema"] = selected_schema

            if st.button("Continue ⮕"):

                if not st.session_state.client_id_state_st or not st.session_state.client_secret_state_st:
                    st.error("Please store client credentials first before continuing")
                elif app_func.validate_credentials_general() is None:
                     st.error("Client credentials are invalid. Please enter valid client credentials to continue.")
                else:
                    st.session_state["continue_clicked"] = True
                    try:
                        provision()
                    except Exception as e:
                        appPY_logger.error(f"Custom_ILEVEL_CONNECTOR - streamlit_appPY_logger - provision() : {str(e)}")


        if st.session_state.get("continue_clicked", False):   # st.session_state.get("continue_clicked", False) checks if "continue_clicked" exists in session state         # If it doesn't exist, returns False as the default value instead of raising KeyError

            resource_names = st.multiselect( "**Select Resources for Data Ingestion**",
                options=[ "Assets", "Funds", "Periodic Data", "Cash Transactions", "Securities", "Currencies", "Deals", "Cash Transaction Types", "FX Rates", "Entity Group Categories", "Benchmarks", "Valuation Data Items"],
                key="resource_names")
            
            st.info(""" 
                    To specify a warehouse to run the task under, you must grant the iLevel application permission to use the warehouse by executing the following command in a SQL worksheet: 
                    ```
                    GRANT USAGE ON WAREHOUSE <WAREHOUSE> TO APPLICATION <APP_NAME>;
                    ``` 
                    Afterwards, click the Refresh button above  
                    """)

            
            for name, resource in [(name, resource_name_mapping[name]) for name in resource_names]: 
                # Expander for each resource's schedule
                with st.expander(f"**Schedule Task for {name}**", expanded = True):
                    # Scheduling type choice
                    schedule_type = st.radio(f"Choose Schedule Type", ["Basic Schedule", "Custom CRON", "Advanced Schedule"], key=f"schedule_type_{resource}")

                    st.markdown("---")

                    if schedule_type == "Basic Schedule":

                        col1, col2 = st.columns([.4,1])
                        with col1:
                            time_unit = st.radio(
                                f"Select Time Unit Interval to Run Task",
                                options=["Minute", "Hour", "Day", "Week", "Month"],
                                key=f"radio_time_unit_{resource}"
                            )
                        with col2:
                            if time_unit:
                                schedule_value = st.slider(f"Run Task Per Specified {time_unit}(s)", 1, 100, key=f"slider_{resource}_{time_unit}", help = "For the 'Day' time is chosen then the task will run at 12:00 AM (00:00) every N days, if 'Week' is chosen then the task will run at 12:00 AM (00:00) every N weeks on Sunday, if 'Month' is chosen then the task will run at 12:00 AM (00:00) on the 1st of every N months")
                                cron_expression = app_func.generate_basic_cron_expression(time_unit, schedule_value)
                                st.session_state[f'schedule_{resource}'] = cron_expression

                    elif schedule_type == "Custom CRON": 
                        
                        col1, col2, col3, col4, col5 = st.columns(5)

                        # User input for cron components
                        minute = col1.text_input("Minute (0-59)", value="0", key=f"minute_{resource}")
                        hour = col2.text_input("Hour (0-23)", value="1", key=f"hour_{resource}")
                        day_of_month = col3.text_input("Day of Month (1-31, *)", value="*", key=f"dom_{resource}")
                        month = col4.text_input("Month (1-12, *)", value="*", key=f"month_{resource}")
                        day_of_week = col5.text_input("Day of Week (0-6, SUN-SAT, *)", value="*", key=f"dow_{resource}")



                        # Generate cron expression and store in session state
                        cron_expression = f"{minute} {hour} {day_of_month} {month} {day_of_week}"
                        st.session_state[f'schedule_{resource}'] = cron_expression

                        # Generate human-readable description
                        human_readable_cron = get_description(cron_expression)

                        # Display results
                        st.markdown(f"Cron Expression: {cron_expression}")
                        st.markdown(f"Description: {human_readable_cron}")
                        
                    
                    else: #*#*#* # Advanced Scheduling

                        custom_schedule_type = st.radio("Schedule Type", ["Weekly", "Monthly"], key=f"custom_type_{resource}")

                        col1, col2 = st.columns([1,1])

                        with col1:
                            hours_of_day = st.multiselect(
                                "Select Hour(s) of Day",
                                options=[f"{hour:02d}:00" for hour in range(24)],
                                key=f"hours_{resource}"
                            )
                            hours_of_day = [int(time.split(":")[0]) for time in hours_of_day]

                        with col2:

                            if custom_schedule_type == "Weekly":
                                weekdays = st.multiselect(
                                    "Select Days of the Week",
                                    options=[("Sunday", 0), ("Monday", 1), ("Tuesday", 2), ("Wednesday", 3), 
                                        ("Thursday", 4), ("Friday", 5), ("Saturday", 6)],
                                    format_func=lambda x: x[0],
                                    key=f"weekdays_{resource}"
                                )
                                cron_expression = app_func.generate_custom_cron_expression(weekdays=weekdays, hours_of_day=hours_of_day)
                            else:
                                monthdays = st.multiselect(
                                    "Select Days of the Month",
                                    options=list(range(1, 32)),
                                    key=f"monthdays_{resource}"
                                )
                                cron_expression = app_func.generate_custom_cron_expression(monthdays=monthdays, hours_of_day=hours_of_day)
                            st.session_state[f'schedule_{resource}'] = cron_expression

                    col1, col2, col3, col4 = st.columns([1,1,1,1])

                    with col1:
                        timezone = st.selectbox("Select Timezone", options= timezone_options, index = 74 , key=f"timezone_{resource}")
                    with col2:
                        warehouse = app_func.get_warehouse()
                        warehouse_choice = st.selectbox("Select Warehouse", options=warehouse, key=f"warehouse_{resource}")
                    with col3:

                        choice_name = st.radio(
                            "Table Name", options=["Default","Custom"], key=f"table_name_choice_{resource}"  # Added unique key here
                                )
                        if choice_name == "Custom":
                            st.text_input("Input Table Name" , key = f"custom_tbl_name_{resource}")
                        else:
                            st.session_state[f"custom_tbl_name_{resource}"] = 'None' 
                    with col4:
                        choice = st.radio("First Run Timing", ##note : the radio button is placed outside the conditionals for the scheduling types (Basic, Custom CRON, Advanced). This means it is executed regardless of which scheduling option is selected.
                            options=["Execute Immediately", "Execute as per Schedule"],
                            key=f"execute_timing_{resource}"
                        )


            # Single button to ingest all data for the selected resources
            if st.button("Ingest All Data",type = "primary"):

                if not st.session_state.client_id_state_st or not st.session_state.client_secret_state_st:
                    st.error("Please store client credentials first before continuing")

                if not app_func.get_warehouse():
                    st.error("Please grant usage on a warehouse to run the task under (see information above)")

                elif any(not app_func.is_valid_table_name(st.session_state.get(f'custom_tbl_name_{resource}')) for resource in [resource_name_mapping[name] for name in resource_names]):                        
                    st.error("Invalid table name. Snowflake table names must start with a letter, can contain letters, numbers (0 to 9), underscores, and a $ (but not as the first character). Table names cannot contain spaces or other special characters.")
                                
                else:
                    try:
                        app_func.ingest_all_data() 
                    except Exception as e:
                        appPY_logger.error(f"Custom_ILEVEL_CONNECTOR - streamlit_appPY_logger - app_func.ingest_all_data() : {str(e)}")


                    
    elif st.session_state["active_page"] == "Tasks":
        st.header("Tasks")
         
        # Tabs for task management and history
        tab1, tab2 = st.tabs(["Manage Tasks", "Task History"])
        
        with tab1:
            try:
                tasks = app_func.get_tasks()
            except Exception as e:
                appPY_logger.error(f"Custom_ILEVEL_CONNECTOR - streamlit_appPY_logger - app_func.get_tasks() : {str(e)}")

            if tasks.empty:
                st.write("No currently active tasks")
            else:
                st.dataframe(tasks, use_container_width=True)
            
                # Task management expanders
                with st.expander("**Manage Task Status**", expanded = True):
                    # Only show task management UI if there are tasks
                    for task_name in tasks['NAME'].tolist():
                        # Get task's current state
                        task_state = tasks[tasks['NAME'] == task_name]['STATE'].iloc[0]
                        
                        col1, col2, col3,col4 = st.columns([2, .7, .7, .7])
                        with col1:
                            st.write(task_name)
                        with col2:
                            if st.button("⏸️ Pause", key=f"pause_{task_name}",
                                disabled=(task_state == "suspended")):
                                app_func.manage_task_status(task_name, "pause")
                                st.experimental_rerun()

                        with col3:
                            if st.button("▶️ Resume", key=f"resume_{task_name}",
                                disabled=(task_state == "started")):
                                app_func.manage_task_status(task_name, "resume")
                                st.experimental_rerun()
                        with col4:
                            if st.button("🔄 Run Now", key=f"run_now_{task_name}",type="primary"):
                                app_func.run_task_now(task_name)
                                st.experimental_rerun()

                with st.expander("**Manage Tasks**", expanded = True):
                    task_names = tasks['NAME'].tolist()
                    tasks_to_delete = st.multiselect(
                        "Select Tasks to Delete",
                        options=task_names
                    )
                    
                    # Initialize confirm_delete_tasks in session state
                    if "confirm_delete_tasks" not in st.session_state:
                        st.session_state["confirm_delete_tasks"] = False
                            
                    if tasks_to_delete:
                        if st.button("Delete Selected Tasks"):
                            st.session_state["confirm_delete_tasks"] = True
                        
                    if st.session_state["confirm_delete_tasks"]:
                        # st.markdown("""
                        #     <style>
                        #     button[kind="delete_tasks_confirm"], button[kind="cancel_tasks_confirm"] {
                        #         border: 2px solid #0052cc;
                        #     }
                        #     </style>
                        # """, unsafe_allow_html=True)
                        
                        st.warning(f"Are you sure you want to delete the following task(s): {', '.join(tasks_to_delete)}?")
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("Delete", key="delete_tasks_confirm", type="primary"):
                                try:
                                    app_func.delete_tasks(tasks_to_delete)
                                except Exception as e:
                                    appPY_logger.error(f"Custom_ILEVEL_CONNECTOR - streamlit_appPY_logger - app_func.delete_tasks() : {str(e)}")
                                st.session_state["confirm_delete_tasks"] = False
                                st.experimental_rerun()
                        with col2:
                            if st.button("Cancel", key="cancel_tasks_confirm", type="primary"):
                                st.session_state["confirm_delete_tasks"] = False
                                st.experimental_rerun()
        with tab2:
            # Initialize session state for view type if not exists
            if 'task_view_type' not in st.session_state:
                st.session_state.task_view_type = "Show Only Currently Available Tasks"
            
            # Radio button that uses and updates session state
            view_type = st.radio(
                "Select View Type",
                ["Show Only Currently Available Tasks", "Show All Tasks Historically"],
                key='task_view_type',
                help="_Show Only Currently Available Tasks_ displays history for tasks that currently exist in the system (for recently created tasks they can take a few minutes to appear). _Show All Tasks Historically_ displays complete history including deleted tasks."
            )
            
            if st.session_state.task_view_type == "Show Only Currently Available Tasks":
                if not tasks.empty:
                    task_names = tasks['NAME'].tolist()
                    selected_tasks = st.multiselect(
                        "Select Tasks to View History",
                        options=task_names,)
                    
                    try:
                        task_history = app_func.get_task_history(selected_tasks, show_current_only=True)
                    except Exception as e:
                        appPY_logger.error(f"Custom_ILEVEL_CONNECTOR - streamlit_appPY_logger - app_func.get_task_history() : {str(e)}")
                    
                    st.dataframe(task_history)
                else:
                    st.write("No tasks available to show history")
            else:
                try:
                    task_history = app_func.get_task_history(show_current_only=False)
                except Exception as e:
                    appPY_logger.error(f"Custom_ILEVEL_CONNECTOR - streamlit_appPY_logger - app_func.get_task_history() : {str(e)}")
                if not task_history:
                    st.write("No historical task data found")
                else:
                    st.dataframe(task_history)
                    
    elif st.session_state["active_page"] == "Tables":
        st.header("Tables")
        # Add manage tables expander
        with st.expander("**Manage Tables**", expanded = True):
            try:
                table_names = app_func.get_tables()
            except Exception as e:
                appPY_logger.error(f"Custom_ILEVEL_CONNECTOR - streamlit_appPY_logger - app_func.get_tables() : {str(e)}")
            if not table_names:
                st.write("No tables found")
            else:
                tables_to_delete = st.multiselect(
                    "Select Tables to Delete",
                    options=table_names
                )
                
                if tables_to_delete:
                    if "confirm_delete" not in st.session_state:
                        st.session_state.confirm_delete = False
                        
                    if st.button("Delete Selected Tables"):
                        st.session_state.confirm_delete = True

                    if st.session_state.confirm_delete:                            # background-color: #0052cc;

                        # st.markdown("""
                        #     <style>
                        #     div[data-testid="stHorizontalBlock"] div.stButton > button {
                        #         border: 2px solid #0052cc;
                        #     }
                        #     </style>
                        # """, unsafe_allow_html=True)
                        
                        st.warning(f"Are you sure you want to delete the following table(s): {', '.join(tables_to_delete)}?")
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("Delete", key="delete_confirm",type="primary"):
                                try:
                                    app_func.delete_tables(tables_to_delete)
                                except Exception as e:
                                    appPY_logger.error(f"Custom_ILEVEL_CONNECTOR - streamlit_appPY_logger - app_func.delete_tables() : {str(e)}")
                                st.session_state.confirm_delete = False
                                st.experimental_rerun()
                        with col2:
                            if st.button("Cancel", key="cancel_confirm",type="primary"):
                                st.session_state.confirm_delete = False
                                st.experimental_rerun()

if __name__ == "__main__":
    main()
