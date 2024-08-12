''' 
    This is the time consuming extraction of data from the Excel
    Once pickled this is not run.
    Ensure all file references are to your local desktop file, not the SharePoint
'''

import os

# Accessing the DataFiles Path
home_dir = os.path.expanduser("~")
datafiles_path = os.path.join(home_dir, "Desktop", "DataFiles")

# Setting the FilesIn as the source files, and the AppStaging for the pickles
in_path = os.path.join(datafiles_path, "FilesIn")
out_path = os.path.join(datafiles_path, "AppStaging")
