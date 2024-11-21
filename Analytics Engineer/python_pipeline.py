import ftplib
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

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
