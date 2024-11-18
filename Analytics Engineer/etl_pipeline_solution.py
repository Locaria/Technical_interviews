import ftplib
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

## no useful comments
## -- No Docstring, no explanation of what the code does, no inline comments

## no proper error handling // no logging
## -- No try-except block, no logging, no error handling
## -- If there's an error, the pipeline will fail without any error message

## no edge-case handling 
## -- No handling for edge cases where 'value' might not be present or not an integer
## -- If the input data is not as expected, the pipeline will fail without any error message

## No global constants for FTP credentials and Google Sheets credentials
## -- creds, paths etc are hardcoded in the functions

## bad naming (columns, variables, functions) (i.e. Calculated, clean_data(), Transformed) are not descriptive
## -- not following best practices for variable naming
## -- makes the code harder to read and understand

## Transformation possible issues:
## - Drops duplicates without specifying which subset; might lead to data loss
## - to_numeric() without error handling. (will break with any non-numeric data)
## - df.fillna(0, inplace=True) (fills NaNs with 0 without context - either not thought through or badly documented)
## - df[Transformed] = df[Calculated] / 3.1415 bad naming, no explanation, no context, pi hardcoded (and rounded)
## - df["NewColumn"] = 0; for loop to fill the column (inefficient, could be done with vectorized operations)
## -> (could be done with df['NewColumn'] = df['Column2'] ** 2)

## Inefficient data upload to Google Sheets
## -- Converting DataFrame to list row-by-row for upload
## -- Better would be to use batch update or at least convert the DataFrame to a list of lists

def get_data(ftp_host, file_name):
    ftp = ftplib.FTP(ftp_host)
    ftp.login(user="loc_user501", passwd="23480dfhS..HH/d")
    data = []
    ftp.retrlines(f'RETR {file_name}', lambda x: data.append(x))
    ftp.quit()
    # converts to df
    df = pd.DataFrame([line.split(',') for line in data])
    return df

def clean_data(df):
    
    df.columns = df.columns.str.strip() # Strip whitespace from headers 
    
    df.drop_duplicates(inplace=True)

    df['Column2'] = pd.to_numeric(df['Column2']) 
    
    df.fillna(0, inplace=True)
    
    new_col = df['Column2'] * 2  
    df['Calculated'] = new_col
    df['Transformed'] = df['Calculated'] / 3.1415  

    df['NewColumn'] = 0  # Initialize column with default value
    for i in range(len(df)):
        df.at[i, 'NewColumn'] = df.at[i, 'Column2'] ** 2
    
    return df

# Writes data to Google Sheets
def push_to_google_sheets(sheet_name, data):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open(sheet_name).sheet1
    
    data_to_upload = data.values.tolist()
    for row in data_to_upload:
        sheet.append_row(row)  
    
    print("Data pushed to Google Sheets!")  

# Main function to run the entire process
def main():
    ftp_host = 'ftp.example.com'
    file_name = 'data.csv'
    sheet_name = 'Sheet1'
    
    try:
        df = get_data(ftp_host, file_name)
        cleaned_df = clean_data(df)
        push_to_google_sheets(sheet_name, cleaned_df)
    except Exception:
        print("Something went wrong!") 

if __name__ == "__main__":
    main()








########################################################################################################################
########################################################################################################################
########################################################################################################################
########################################################################################################################
## BELOW A POSSIBLE IMPROVED SOLUTION

import ftplib
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
from typing import List
from math import pi

# Set up logging to write to a file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("etl_process.log"),  # File to store logs
        logging.StreamHandler()  # Also log to console
    ]
)

# Global constants for FTP and Google Sheets credentials
FTP_USERNAME = "loc_user501"
FTP_PASSWORD = "secure_password"  # This should ideally be loaded from environment variables for security

def fetch_data_from_ftp(ftp_server: str, file_name: str) -> pd.DataFrame:
    """
    Connects to the FTP server, retrieves the specified file, and loads it into a DataFrame.
    Args:
        ftp_server (str): The hostname of the FTP server.
        file_name (str): The name of the file to retrieve.
    Returns:
        pd.DataFrame: A DataFrame containing the retrieved data.
    """
    try:
        with ftplib.FTP(ftp_server) as ftp:
            ftp.login(user=FTP_USERNAME, passwd=FTP_PASSWORD)
            raw_data: List[str] = []
            ftp.retrlines(f'RETR {file_name}', lambda line: raw_data.append(line))
        
        # Create a DataFrame from the retrieved data with appropriate column names (adjust as needed)
        data_frame = pd.DataFrame([row.split(',') for row in raw_data], columns=['ID', 'Value', 'Category'])  # Adjust columns as needed
        logging.info("Data successfully retrieved from FTP and loaded into DataFrame.")
        return data_frame
    except ftplib.all_errors as ftp_error:
        logging.error(f"Error occurred while retrieving data from FTP: {ftp_error}")
        raise

def process_and_clean_data(data_frame: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and processes the DataFrame with standard transformations.
    Args:
        data_frame (pd.DataFrame): The input DataFrame to be cleaned and transformed.
    """
    # Trim whitespace from column names
    data_frame.columns = data_frame.columns.str.strip()

    # Remove duplicate rows based on specific columns to prevent data loss
    data_frame.drop_duplicates(subset=['ID', 'Value'], inplace=True)

    # Convert the 'Value' column to numeric, setting non-convertible values to NaN
    data_frame['Value'] = pd.to_numeric(data_frame['Value'], errors='coerce')

    # Fill NaN values with the median for numerical stability
    data_frame.fillna(data_frame.median(), inplace=True)

    # Perform vectorized operations for new calculated columns
    data_frame['Value_Double'] = data_frame['Value'] * 2
    data_frame['Value_Ratio'] = data_frame['Value_Double'] / pi  # Using the precise value of pi from math library
    data_frame['Value_Squared'] = data_frame['Value'] ** 2  # Efficient, vectorized transformation

    logging.info("Data cleaning and transformations completed.")
    return data_frame

def upload_data_to_google_sheets(sheet_title: str, data_frame: pd.DataFrame) -> None:
    """
    Uploads the cleaned DataFrame to a specified Google Sheets sheet.
    Args:
        sheet_title (str): The title of the Google Sheets sheet.
        data_frame (pd.DataFrame): The DataFrame to be uploaded.
    """
    try:
        # Authenticate with Google Sheets
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
        sheets_client = gspread.authorize(credentials)

        # Open the sheet and clear existing data before adding new data
        sheet = sheets_client.open(sheet_title).sheet1
        sheet.clear()

        # Prepare data for upload by converting DataFrame to a list of lists
        sheet_data = [data_frame.columns.values.tolist()] + data_frame.values.tolist()
        sheet.update('A1', sheet_data)
        
        logging.info("Data successfully uploaded to Google Sheets.")
    except Exception as sheet_error:
        logging.error(f"Error occurred while uploading data to Google Sheets: {sheet_error}")
        raise

def execute_etl_process() -> None:
    """
    Orchestrates the entire ETL (Extract, Transform, Load) process of retrieving, processing,
    and uploading data.
    """
    ftp_server = 'ftp.example.com'
    file_name = 'data.csv'
    sheet_title = 'Sheet1'

    try:
        raw_data_frame = fetch_data_from_ftp(ftp_server, file_name)
        cleaned_data_frame = process_and_clean_data(raw_data_frame)
        upload_data_to_google_sheets(sheet_title, cleaned_data_frame)
    except Exception as etl_error:
        logging.error("An error occurred during the ETL process.")
        logging.exception(etl_error)

if __name__ == "__main__":
    execute_etl_process()