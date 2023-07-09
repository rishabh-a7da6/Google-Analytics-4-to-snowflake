# Necessary Imports
import os
import pytz
import json
import math
import datetime

from snowflake.snowpark import Session
from snowflake.snowpark.functions import lit
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import to_date

from google.analytics.data_v1beta.types import Metric
from google.analytics.data_v1beta.types import DateRange
from google.analytics.data_v1beta.types import Dimension
from google.analytics.data_v1beta.types import RunReportRequest
from google.analytics.data_v1beta import BetaAnalyticsDataClient

# Reading Snowflake Credentials file
with open('snowflake.json', 'r') as d:
    connection_parameters = json.load(d)
session = Session.builder.configs(connection_parameters).create()

# Creating Connection from GA4
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'service-account-key.json'
client = BetaAnalyticsDataClient()

def dataframeToSnowflake(df, 
                         database:str, 
                         schema:str, 
                         table:str) -> int:
    """
    Writes a Snowpark DataFrame to Snowflake database.

    Args:
        df (session.DataFrame): The Snwopark DataFrame to be written.
        database (str): The name of the Snowflake database.
        schema (str): The name of the schema within the database.
        table (str): The name of the table to write the data into.

    Returns:
        int: The number of rows written to Snowflake.
    """
    df.write.mode("append").save_as_table([database, schema, table], table_type="")
    return df.count()

def createSnowparkDataframe(responseDictionary, dims, mets):
    """
    Converts a response dictionary to a Snowpark DataFrame.

    Args:
        responseDictionary (dict): The response dictionary containing the data.
        dims (list): The list of dimensions.
        mets (list): The list of metrics.

    Returns:
        session.DataFrame: A Snowpark DataFrame containing the extracted data.
    """
    output = {}
    headers = dims + mets
    rows = []

    for row in responseDictionary['rows']:
        rows.append(
            [x.value for x in row['dimension_values']] + \
            [y.value for y in row['metric_values']])
                
    output['headers'] = headers
    output['rows'] = rows
    
    return session.createDataFrame(output['rows'], schema = output['headers'])

def getReportInSnowflake(propertyId,
                         dimensions, 
                         metrics, 
                         startDate, 
                         endDate,
                         database,
                         schema,
                         table,
                         keep_empty_rows:bool=False,
                         quota_usage:bool=False,
                         limit:int = 250000, 
                         offset:int = 0):
    """
    Retrieves a report from Google Analytics Data API and transfers it to Snowflake database.

    Args:
        propertyId (str): The ID of the Google Analytics property.
        dimensions (list): A list of dimensions to include in the report.
        metrics (list): A list of metrics to include in the report.
        startDate (str): The start date of the report in the format 'YYYY-MM-DD'.
        endDate (str): The end date of the report in the format 'YYYY-MM-DD'.
        database (str): The name of the Snowflake database.
        schema (str): The name of the schema within the database.
        table (str): The name of the table to write the data into.
        keep_empty_rows (bool, optional): Whether to keep empty rows in the report. Defaults to False.
        quota_usage (bool, optional): Whether to include quota usage information in the report. Defaults to False.
        limit (int, optional): The maximum number of rows to retrieve per API call. Defaults to 250000.
        offset (int, optional): The offset for pagination. Defaults to 0. Can be used to debug pagination.

    Returns:
        int: The total number of rows transferred to Snowflake.

    Raises:
        KeyError: If the required keys are not present in the response.

    Example:
        total_rows = getReportInSnowflake(propertyId='12345678',
                                          dimensions=['ga:date', 'ga:source'],
                                          metrics=['ga:sessions', 'ga:users'],
                                          startDate='2023-01-01',
                                          endDate='2023-01-31',
                                          database='my_database',
                                          schema='my_schema',
                                          table='my_table')
    """
    
    totalRows : int = 0
    apiCallsCounter : int = 0
    flag : int = 0
    
    dimension_list = [Dimension(name=d) for d in dimensions]
    metrics_list = [Metric(name=m) for m in metrics]

    current_date = datetime.datetime.strptime(startDate, '%Y-%m-%d')
    end = datetime.datetime.strptime(endDate, '%Y-%m-%d')

    while current_date <= end:
        current_start_date = current_date.strftime('%Y-%m-%d')
        current_end_date = current_date.strftime('%Y-%m-%d')
        offset = 0
        flag = 0

        while True:

            report_request = RunReportRequest(
                property=f'properties/{propertyId}',
                dimensions=dimension_list,
                metrics=metrics_list,
                limit=limit,
                date_ranges=[DateRange(start_date=current_start_date, end_date=current_end_date)],
                keep_empty_rows=keep_empty_rows,
                return_property_quota=quota_usage,
                offset=offset
            )

            # actual data pull
            response = client.run_report(report_request)
            
            apiRows = response.row_count
            # print(current_start_date, current_end_date, apiRows)

            if 'rows' in response:

                response_dict = {
                    'dimension_headers' : response.dimension_headers,
                    'metric_headers' : response.metric_headers,
                    'rows' : [],
                    'row_count' : response.row_count,
                    'metadata' : response.metadata,
                    'kind' : response.kind
                }
                for row in response.rows:
                    row_dict = {
                        'dimension_values': row.dimension_values,
                        'metric_values': row.metric_values
                    }
                    response_dict['rows'].append(row_dict)

                response = None

                metricHeaders = [value.name for value in response_dict['metric_headers']]
                dimensionHeaders = [value.name for value in response_dict['dimension_headers']]

                dataframe = createSnowparkDataframe(responseDictionary=response_dict, 
                                                    dims= dimensionHeaders,
                                                    mets=metricHeaders)

                # convert all metrics to double
                for key in metricHeaders:
                    dataframe = dataframe.withColumn(key, dataframe[key].cast('double'))
                
                
                # adding loadtimestamp in dataframe
                current_timestamp = datetime.datetime.now(pytz.timezone('UTC')).strftime('%Y-%m-%d %H:%M:%S')
                dataframe = dataframe.withColumn('LOADTIMESTAMP', lit(current_timestamp))

                # typecasting date and loadtimestamp column
                dataframe = dataframe.withColumn("DATE", to_date(col("DATE"), 'YYYYMMDD'))
                dataframe = dataframe.withColumn('LOADTIMESTAMP', dataframe["LOADTIMESTAMP"].cast('timestamp'))

                # Transfer dataframe to snowflake
                rows = dataframeToSnowflake(dataframe, database, schema, table)

                totalRows = totalRows + rows
                apiCallsCounter += 1

                offset += limit
                flag += 1

                if flag >= math.ceil(apiRows / limit):
                    break

            else:
                break

        current_date += datetime.timedelta(days=1)

    return totalRows

        