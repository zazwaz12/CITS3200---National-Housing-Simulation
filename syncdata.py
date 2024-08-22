"""
Script for syncing local OneDrive folder to Desktop folder
"""
# pyright: reportMissingTypeStubs=false
from dirsync import sync
import os


def sync_onedrive_to_desktop(
    username: str, onedrive_name: str, target_path: str
) -> None:
    """
    Syncs the OneDrive folder to folder in out_path
    """
    user_profile = os.path.join("C:\\Users", username)
    source_path = os.path.join(user_profile, onedrive_name, "DataFiles")

    sync(source_path, target_path, "sync", purge=False, verbose=True)
    print(f"Synced {source_path} to {target_path}")


# Main script execution
if __name__ == "__main__":
    username = input("Please enter your Windows username: ")
    onedrive_name = input("Please enter your OneDrive name (e.g., OneDrive - UWA): ")
    target_path = (
        input(
            "Please enter the path to the DataFiles folder to sync (default: ./DataFiles): "
        )
        or "./DataFiles"
    )

    sync_onedrive_to_desktop(username, onedrive_name, target_path)
