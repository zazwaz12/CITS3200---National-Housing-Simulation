import pandas as pd
import os

# Accessing the DataFiles Path
home_dir = os.path.expanduser("~")
datafiles_path = os.path.join(home_dir, "Desktop", "DataFiles")

# Setting the FilesIn as the source files, and the AppStaging for the pickles
in_path = os.path.join(datafiles_path, "FilesIn")
standard_in = os.path.join(in_path, "Standard")
authority_code_in = os.path.join(in_path, "Authority Code")
pickles_out_path = os.path.join(datafiles_path, "AppStaging")
files_out_path = os.path.join(datafiles_path, "FilesOut")

# Defining the path for the Excel file
codes_file_path = os.path.join(files_out_path, 'code-meanings.xlsx')

def read_codes():
    # Initialize an empty dictionary to hold the DataFrames
    dataframes_dict = {}

    # Loop through all the files in codes
    for filename in os.listdir(authority_code_in):
        if filename.endswith('.psv'):
            file_path = os.path.join(authority_code_in, filename)
            
            try:
                # Read the PSV file into a DataFrame
                df = pd.read_csv(file_path, delimiter='|')
                
                # Use the filename (without extension) as the key for the dictionary
                key = os.path.splitext(filename)[0]
                dataframes_dict[key] = df
                
            # The code files may be empty - the version I had were empty
            except pd.errors.EmptyDataError:
                continue
            except pd.errors.ParserError:
                print(f"Skipped malformed file: {filename}")
            except Exception as e:
                print(f"Error processing file {filename}: {e}")

    # Write the dictionary of DataFrames to an Excel file
    with pd.ExcelWriter(codes_file_path) as writer:
        for sheet_name, df in dataframes_dict.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    return dataframes_dict

# Example logic check (to be transitioned into unit tests)
"""
print(len(os.listdir(standard_in)))  # LOGIC CHECK SHOULD EQUAL 171
print(len(os.listdir(authority_code_in)))  # LOGIC CHECK SHOULD EQUAL 16
"""

if __name__ == "__main__":
    read_codes()
