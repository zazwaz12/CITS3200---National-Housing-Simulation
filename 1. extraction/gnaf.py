import pandas as pd
import pickle
import os

# Accessing the DataFiles Path
home_dir = os.path.expanduser("~")
datafiles_path = os.path.join(home_dir, "Desktop", "DataFiles")

# Setting the FilesIn as the source files, and the AppStaging for the pickles
in_path = os.path.join(datafiles_path, "FilesIn")
standard_in = os.path.join(in_path, "Standard")
authority_code_in = os.path.join(in_path, "Authority Code")
pickle_file_path = os.path.join(datafiles_path, "AppStaging", 'locational-data.pkl')
files_out_path = os.path.join(datafiles_path, "FilesOut")

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
    with pd.ExcelWriter(os.path.join(files_out_path, 'code-meanings.xlsx')) as writer:
        for sheet_name, df in dataframes_dict.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    return dataframes_dict


def read_data_to_dictionary_pickle():
    print("Reading Standard files")
    dataframes_dict = {}
    for filename in os.listdir(standard_in):
        if filename.endswith('.psv'):
            file_path = os.path.join(standard_in, filename)
            try:
                df = pd.read_csv(file_path, delimiter='|')
                key = os.path.splitext(filename)[0]
                dataframes_dict[key] = df
            except pd.errors.EmptyDataError:
                print(f"Skipped empty file: {filename}")
            except Exception as e:
                print(f"Error processing file {filename}: {e}")

    with open(pickle_file_path, 'wb') as pickle_file:
        pickle.dump(dataframes_dict, pickle_file)

def output_files_summaries():
    print("Reading in the pickle")
    with open(pickle_file_path, 'rb') as file:
        data_dict = pickle.load(file)

    data_info = []
    total_rows = 0
    for file_name, df in data_dict.items():
        df_rows = df.shape[0]
        df_columns = df.shape[1]
        total_rows += df_rows
        df_head = df.head().to_string(index=False)
        data_info.append([file_name, df_columns, df_rows, df_head])

    data_info.append(["All these files put together are", '', total_rows, ''])
    info_df = pd.DataFrame(data_info, columns=['File Name', 'Columns', 'Rows', 'Head'])
    info_df.to_csv(os.path.join(files_out_path,'data_info.csv'), index=False)
    return data_dict

def filter_by_lat_lon(data_dict):
    print("Filtering DataFrames by LATITUDE and LONGITUDE")
    data_info = []
    total_rows = 0
    filtered_data_dict = {}
    for file_name, df in data_dict.items():
        if 'LATITUDE' in df.columns and 'LONGITUDE' in df.columns:
            df_shape = df.shape
            df_head = df.head().to_string(index=False)
            total_rows += df_shape[0]
            data_info.append([file_name, df_shape[1], df_shape[0], df_head])
            filtered_data_dict[file_name] = df

    data_info.append(["All these files put together are", '', total_rows, ''])
    info_df = pd.DataFrame(data_info, columns=['File Name', 'Columns', 'Rows', 'Head'])
    info_df.to_csv(os.path.join(files_out_path,'data_house_info.csv'), index=False)
    return filtered_data_dict

if __name__ == "__main__":
    # Run the functions for Authority Code and Standard files
    #read_codes()
    #read_data_to_dictionary_pickle()
    data_dict = output_files_summaries()
    filter_by_lat_lon(data_dict)


# Example logic check (to be transitioned into unit tests)
"""
print(len(os.listdir(standard_in)))  # LOGIC CHECK SHOULD EQUAL 171
print(len(os.listdir(authority_code_in)))  # LOGIC CHECK SHOULD EQUAL 16
"""
