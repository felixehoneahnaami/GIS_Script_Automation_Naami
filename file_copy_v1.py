
"""

Author: Felix Ehoneah Naami

Summary:This script copies all files (except .db and .Temp files) from a source directory (drive1) to a destination directory (drive2).
        It uses parallel processing to handle files and subdirectories efficiently. Logging is used to track the copy operations and any errors that occur during the process. 

"""


import os
import shutil
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define the directories
drive1 = r"\\tsclient\D\New folder"  # Source location
drive2 = r"\\WorldDriveAFR.newmont.net\share\worldcon\AFRICA\_Prospectivity\All_WAXI_DATA\WAXI_2024"  # Destination location

# Define the directory where the log files will be saved
log_directory = r"\\WorldDriveAFR.newmont.net\share\worldcon\AFRICA\_Prospectivity\All_WAXI_DATA\WAXI_2024"    # Log file location
os.makedirs(log_directory, exist_ok=True)
print(f"Log directory created or already exists at: {log_directory}")

# Get the current date and time
current_time = datetime.now().strftime("%Y%m%d_%I%M%S%p")

# Set up logging for general information
log_file_path = os.path.join(log_directory, f'copy_log_{current_time}.log')
logger = logging.getLogger('main_logger')
logger.setLevel(logging.INFO)

# File handler for the general log
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Add the file handler to the logger
logger.addHandler(file_handler)

# Set up logging for errors
error_log_file_path = os.path.join(log_directory, f'error_log_{current_time}.log')
error_logger = logging.getLogger('error_logger')
error_logger.setLevel(logging.ERROR)

# File handler for the error log
error_file_handler = logging.FileHandler(error_log_file_path)
error_file_handler.setLevel(logging.ERROR)
error_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Add the file handler to the error logger
error_logger.addHandler(error_file_handler)

# Function to copy a file
def copy_file(src, dst):
    try:
        shutil.copyfile(src, dst)
        file_size = os.path.getsize(dst)
        print(f"Copied file: {src} to {dst}, Size: {file_size} bytes")  # User Notification
        logger.info(f"Copied file: {src} to {dst}, Size: {file_size} bytes")
    except Exception as e:
        print(f"Error copying file {src} to {dst}: {e}")  # User Notification
        error_logger.error(f"Error copying file {src} to {dst}: {e}")

# Function to check if the file already exists at the destination
def file_exists(dst):
    return os.path.exists(dst)

# Function to process files and copy them
def process_file(file_pair):
    file1, file2 = file_pair
    try:
        # Check if the file already exists at the destination
        if file_exists(file1):
            print(f"Skipped file: {file2}, already exists at destination.")  # User Notification
            logger.info(f"Skipped file: {file2}, already exists at destination.")
        else:
            copy_file(file2, file1)
            print(f"Copied new file: {file2} to {file1}")  # User Notification
            logger.info(f"Copied new file: {file2} to {file1}")
    except Exception as e:
        print(f"Error processing file {file2} to {file1}: {e}")  # User Notification
        error_logger.error(f"Error processing file {file2} to {file1}: {e}")

# Function to process directories and copy them
def process_directory(source_dir, target_dir):
    try:
        if not os.path.exists(target_dir):
            shutil.copytree(source_dir, target_dir)
            folder_size = sum(
                os.path.getsize(os.path.join(root, f))
                for root, _, files in os.walk(target_dir)
                for f in files
            )
            print(f"Copied folder: {source_dir} to {target_dir}, Total Size: {folder_size} bytes")  # User Notification
            logger.info(f"Copied folder: {source_dir} to {target_dir}, Total Size: {folder_size} bytes")
        else:
            print(f"Directory already exists: {target_dir}")  # User Notification
            logger.info(f"Directory already exists: {target_dir}")
    except Exception as e:
        print(f"Error copying directory {source_dir} to {target_dir}: {e}")  # User Notification
        error_logger.error(f"Error copying directory {source_dir} to {target_dir}: {e}")

# Prepare file pairs and subfolder paths for processing
file_pairs = []
folder_tasks = []

# Traverse through drive1 to copy all files to drive2
for root, subfolders, files in os.walk(drive1):
    # Create the relative path for drive1 and corresponding path in drive2
    relative_path = os.path.relpath(root, drive1)
    target_folder = os.path.join(drive2, relative_path)
    os.makedirs(target_folder, exist_ok=True)

    # Collect file pairs for copying
    for file in files:
        if not file.endswith(('.db', '.Temp')):  # Allow all files, except .db and .Temp files
            file1 = os.path.join(drive2, relative_path, file)
            file2 = os.path.join(root, file)
            file_pairs.append((file1, file2))

    # Collect subfolders for directory processing
    for subfolder in subfolders:
        if not subfolder.endswith(('.db', '.Temp')):  # Allow all subfolders, except .db and .Temp files
            source_subfolder = os.path.join(root, subfolder)
            target_subfolder = os.path.join(target_folder, subfolder)
            folder_tasks.append((source_subfolder, target_subfolder))

# Notify user about start of processing
print("Starting file and folder processing...")  # User Notification

# Process files using ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=4) as executor:  # Adjust max_workers as needed
    file_futures = {executor.submit(process_file, pair): pair for pair in file_pairs}
    folder_futures = {executor.submit(process_directory, task[0], task[1]): task for task in folder_tasks}

    # Check file processing futures
    for future in as_completed(file_futures):
        file_pair = file_futures[future]
        try:
            future.result()  # Raises any exception caught during processing
        except Exception as e:
            print(f"Error processing file {file_pair[1]}: {e}")  # User Notification
            error_logger.error(f"Error processing file {file_pair[1]}: {e}")

    # Check folder processing futures
    for future in as_completed(folder_futures):
        folder_task = folder_futures[future]
        try:
            future.result()  # Raises any exception caught during processing
        except Exception as e:
            print(f"Error processing directory {folder_task[0]}: {e}")  # User Notification
            error_logger.error(f"Error processing directory {folder_task[0]}: {e}")

# Notify user about completion of processing
print(f"100% Complete: Files and folders copied successfully!")  # User Notification
