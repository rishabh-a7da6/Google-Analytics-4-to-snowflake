# Google Analytics(GA4) to Snowflake Data Warehouse

This project demonstrates the process of transferring Google Analytics data to Snowflake using the Google Analytics Data API(GA4) and Snowflake Connector for Python.

## Prerequisites

Before running the code, ensure that you have the following prerequisites set up:

1. **Google Analytics API Credentials**: Obtain the credentials file (`service-account-key.json`) for accessing the Google Analytics Data API. You can follow the [Google Analytics Data API documentation](https://developers.google.com/analytics/devguides/reporting/core/v4/quickstart/service-py#1_enable_the_api) to set up the credentials.

2. **Snowflake Credentials**: Prepare a JSON file (`snowflake.json`) containing the Snowflake credentials required to establish a connection. JSON file should present in same directory as main python file. The JSON file should include the following fields:
   - `user`: The Snowflake username
   - `password`: The Snowflake password
   - `account`: The Snowflake account name
   - `role`: The Snowflake role name
   - `warehouse`: The Snowflake warehouse name
   - `database`: The Snowflake database name
   - `schema`: The Snowflake schema name

3. **Python Dependencies**: Either create a new virtual environment using `conda` or use standard `pip` to install dependencies:

   - **Pip**: Install the dependencies listed in the `requirements.txt` file using the command:

     ```bash
     pip install -r requirements.txt
     ```

   - **Conda**: Create a new virtual environment using the provided `conda-env.yaml` file. Run the following command:

     ```bash
     conda env create -f conda-env.yaml
     conda activate ga4-env
     ```

   The above steps will create a virtual environment named `ga4-env` with the necessary dependencies installed.

   NOTE: Barebones of all prerequisites can be found in code repository itself.

## Usage

1. Modify the `propertyId`, `dims` and `metrics` lists in the `main.py` file to specify the property ID and desired dimensions and metrics for the Google Analytics report. List of all possible metrics and dimensions can be found [here](https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema)

2. Update the `start_date` and `end_date` variables in the `main.py` file to set the date range for the report.

3. Run the `main.py` script using the command `python main.py`.

4. The script will query the Google Analytics Data API for the specified dimensions and metrics within the given date range. The retrieved data will be saved to a Snowflake table.

5. After the script completes, it will print the number of rows transferred to the Snowflake table.

## Troubleshooting

- If you encounter any issues or errors during the process, please ensure that you have correctly set up the prerequisites and provided valid credentials.

- If you need assistance, please open an issue on the [GitHub repository](https://github.com/rishabh-a7da6/GA4-to-snowflake) for this project.

## License

This project is licensed under the [MIT License](LICENSE).

