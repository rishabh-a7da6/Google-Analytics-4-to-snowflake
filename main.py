# Importing local module
import ga4

# CURRENT DIMS AND METS FOR PAGES
propertyId = ''

# Snowflake Parameters for actual data
database = ''
schema = ''
table = ''

# Metrics and Dimensions
dims = ['country', 'pagePath', 'pageTitle', 'date', 'city', 'region']
metrics = ['activeUsers', 'screenPageviews']

start_date = "2023-05-07"
end_date = "2023-05-10"

if __name__ == '__main__':
    rows = ga4.getReportInSnowflake(propertyId,
                                    dims, metrics,
                                    start_date, end_date,
                                    database, schema, table)

    print(rows)
