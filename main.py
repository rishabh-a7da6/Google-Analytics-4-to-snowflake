# Importing necessary libraries and modules
import os
import json
import pandas as pd
import snowflake.connector as snow

from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import RunReportRequest
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from snowflake.connector.pandas_tools import write_pandas

# Setting env variables that is required for authentication
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service-account-key.json'
client = BetaAnalyticsDataClient()

# Setting Property ID of GA4
propertyId = ''

# Metrics and Dimensions
dims = ['country', 'screenResolution']
metrics = ['activeUsers']

def query_report(dimensions:list, 
                 metrics:list, 
                 start_date:str, 
                 end_date:str, 
                 row_limit:int=10000, 
                 keep_empty_rows:bool=False, 
                 quota_usage:bool=False):
        """
    Queries a report with specified dimensions and metrics within a given date range.

    Parameters:
        dimensions (list): A list of categorical attributes (e.g., age, country, city).
        metrics (list): A list of numeric attributes (e.g., views, user count, active users).
        start_date (str): The start date in 'YYYY-MM-DD' format (e.g., '2023-01-01').
        end_date (str): The end date in 'YYYY-MM-DD' format (e.g., '2023-12-31').
        row_limit (int, optional): The maximum number of rows to retrieve. Defaults to 10000.
        keep_empty_rows (bool, optional): Whether to include empty rows in the report. Defaults to False.
        quota_usage (bool, optional): Whether to return the property quota usage. Defaults to False.

    Returns:
        dict: A dictionary containing the report data with the following keys:
            - 'quota' (optional): The property quota usage.
            - 'headers': The headers of the report columns.
            - 'rows': The rows of the report, each represented as a list of values.

    Raises:
        Exception: If an error occurs during the report generation.

    Note:
        - The dimensions and metrics should correspond to valid attribute names in the report's property.
        - The start_date and end_date should be valid dates in the 'YYYY-MM-DD' format.
    """
        try:
            dimension_list = [Dimension(name=d) for d in dimensions]
            metrics_list = [Metric(name=m) for m in metrics]
            
            report_request = RunReportRequest(
                property=f'properties/{propertyId}',
                dimensions=dimension_list,
                metrics=metrics_list,
                limit=row_limit,
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                keep_empty_rows=keep_empty_rows,
                return_property_quota=quota_usage
            )
            response = client.run_report(report_request)
     
            output = {}
            if 'property_quota' in response:
                output['quota'] = response.property_quota

            # construct the dataset
            headers = [header.name for header in response.dimension_headers] + [header.name for header in response.metric_headers]
            rows = []
            for row in response.rows:
                rows.append(
                    [dimension_value.value for dimension_value in row.dimension_values] + \
                    [metric_value.value for metric_value in row.metric_values])            
            output['headers'] = headers
            output['rows'] = rows
            return output
        except Exception as e:
            print(e)

# Running and getting report data
response = query_report(
    dims,
    metrics,
    "2023-05-07",
    "2023-05-10"
)

# Converting data to pandas dataframe
dataframe = pd.DataFrame(data = response['rows'], columns = response['headers'])

# Snowflake Parameters
database = ''
schema = ''
table = ''

# Snowflake connection
def snowflake_connection(file_path):
    """
    Establishes a connection to Snowflake using the credentials read from a JSON file.

    Parameters:
        file_path (str): The path to the JSON file containing the Snowflake credentials.

    Returns:
        snowflake.connector.connection.SnowflakeConnection: The Snowflake connection object.
    """
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    user = data['user']
    password = data['password']
    account = data['account']
    role = data['role']
    warehouse = data['warehouse']
    database = data['database']
    schema = data['schema']

    conn = snow.connect(
        user=user,
        password=password,
        account=account,
        role=role,
        warehouse=warehouse,
        database=database.upper(),
        schema=schema.upper()
    )

    return conn

# Transfering pandas dataframe to snowflake
success, nchunks, nrows, out = write_pandas(snowflake_connection('snowflake.json'),
                                            dataframe,
                                            table.upper(),
                                            database.upper(),
                                            schema.upper(),
                                            auto_create_table = True
                                           )

# Printing number of transfered rows
print(f"Number of rows transferred : {nrows}")