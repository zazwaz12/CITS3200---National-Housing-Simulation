""" 
    A script that reads in from the locational data pickle and outputs a pickle 
    of a housing dataframe from DEFAULT_GEOCODE for first iteration
"""
import pandas as pd
import pickle
import os

# Accessing the DataFiles Path
home_dir = os.path.expanduser("~")
datafiles_path = os.path.join(home_dir, "Desktop", "DataFiles")

# Setting the FilesOut and the AppStaging for the pickles
pickle_file_path = os.path.join(datafiles_path, "AppStaging", 'locational-data.pkl')
files_out_path = os.path.join(datafiles_path, "FilesOut")
output_pickle_path = os.path.join(datafiles_path, "AppStaging", 'housing_data.pkl')

def read_in_files():
    print("Reading in the pickle")
    
    # Load the pickled dictionary
    with open(pickle_file_path, 'rb') as file:
        data_dict = pickle.load(file)
    
    # Initialize an empty DataFrame for concatenation
    concatenated_df = pd.DataFrame()
    
    for key, df in data_dict.items():
        if "DEFAULT_GEOCODE" in key:
            # Add a state column with the first part of the key (split on '_')
            df['ADDED-COLUMN-STATE'] = key.split('_')[0]
            
            # Stacking the DataFrame
            concatenated_df = pd.concat([concatenated_df, df], ignore_index=True)
    
    print(concatenated_df)
    # Saving the concatenated DataFrame to a pickle file
    with open(output_pickle_path, 'wb') as file:
        pickle.dump(concatenated_df, file)

    print(f"Concatenated DataFrame saved to {output_pickle_path}")

    # Sampling 750,000 rows from the concat DF for future reference
    sample_df = concatenated_df.sample(n=750000, random_state=1)
    sample_df.to_csv(os.path.join(files_out_path, "houses-sample.csv"), index=False)


if __name__ == "__main__":
    read_in_files()
