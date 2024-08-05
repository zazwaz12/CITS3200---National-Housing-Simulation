from dirsync import sync
import os

def sync_onedrive_to_desktop(username, onedrive_name):
    user_profile = os.path.join('C:\\Users', username)
    source_path = os.path.join(user_profile, onedrive_name, "DataFiles")
    target_path = os.path.join(user_profile, 'Desktop', "DataFiles")
 
    sync_mode = 'sync'
    # Perform the sync
    sync(source_path, target_path, sync_mode, purge=False, verbose=True)
    print(f"Synced {source_path} to {target_path}")

# Main script execution
if __name__ == "__main__":
    # Prompt the names of your OneDrive and User for your Desktop path
    username = input("Please enter your Windows username: ")
    onedrive_name = input("Please enter your OneDrive name (e.g., OneDrive - UWA): ")

    # These are not optional namings, we will all use the reference to DataFiles - FilesIn, FilesOut and AppStaging
    onedrive_folder_name = "DataFiles"
    desktop_folder_name = "DataFiles" 

    # Run the sync
    sync_onedrive_to_desktop(username, onedrive_name)

