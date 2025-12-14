""" Summary: This script is designed to compare files between two directories (source and destination), and copy or update the files based on a comparison of their size and hash values.
It also logs the details of the copy operation and any errors that occur during the process.
    Workflow:
    1. Set up directories and log files.
    2. Traverse the source directory, generating pairs of corresponding files to compare.
    3. Compare files based on size and hash.
    4. Copy files from the source to the destination if needed.
    5. Handle errors during the copy process and log them.
    6. Clean up temporary directories used during processing.
    7. Notify the user about the progress and completion of the task.

    Author: Felix Ehoneah Naami """


import os
import shutil
import hashlib
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Define the two directories
#drive1 = r"C:\Test_copy"  # Destination location
#drive2 = r"C:\Felix Naami\Presentation"  # Source location

# Define the directory where the log files will be saved
#log_directory = r"C:\Test_copy"
#os.makedirs(log_directory, exist_ok=True)
#print(f"Log directory created or already exists at: {log_directory}")

# Define the two directories
drive1 = r"\\WorldDriveAFR.newmont.net\share\exploration\WORLD\AFRICA\_Report\Scientific Papers" # Destination location
drive2 = r"\\WorldDriveAUS.newmont.net\Share\World\zDataCleanup\AFRICA-Data from Musie\Scientific Papers" # Source location

# Define the directory where the log files will be saved
log_directory = r"\\WorldDriveAFR.newmont.net\share\exploration\WORLD\AFRICA\_Administration\GHA_Log_Directories"
os.makedirs(log_directory, exist_ok=True)
print(f"Log directory created or already exists at: {log_directory}")


# Get the current date and time in the desired format (YYYYMMDD and 12-hour time format with AM/PM)
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

# Set up logging for errors only
error_log_file_path = os.path.join(log_directory, f'error_log_{current_time}.log')
error_logger = logging.getLogger('error_logger')
error_logger.setLevel(logging.ERROR)

# File handler for the error log
error_file_handler = logging.FileHandler(error_log_file_path)
error_file_handler.setLevel(logging.ERROR)
error_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Add the file handler to the error logger
error_logger.addHandler(error_file_handler)

# Function to calculate SHA-256 hash of a file
def get_file_hash(file_path, chunk_size=4096):
    """Returns the SHA-256 hash of the file."""
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(chunk_size), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception as e:
        error_logger.error(f"Error calculating hash for file {file_path}: {e}")
        return None

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

# Function to process a single file comparison and copying
def process_file(file_pair):
    file1, file2 = file_pair
    try:
        if os.path.exists(file1):
            size1 = os.path.getsize(file1)
            size2 = os.path.getsize(file2)
            
            # Only calculate hash if sizes are the same
            if size1 == size2:
                hash1 = get_file_hash(file1)
                hash2 = get_file_hash(file2)
                if hash1 != hash2:
                    copy_file(file2, file1)
                    print(f"Overwritten file: {file2} -> {file1} (Hashes differed)")  # User Notification
                    logger.info(f"Overwritten file: {file2} -> {file1} (Hashes differed)")
                else:
                    print(f"Skipped file: {file2}, identical to {file1} (Hashes match)")  # User Notification
                    logger.info(f"Skipped file: {file2}, identical to {file1} (Hashes match)")
            else:
                # Copy if sizes are different
                copy_file(file2, file1)
                print(f"Overwritten file due to size difference: {file2} -> {file1}")  # User Notification
                logger.info(f"Overwritten file due to size difference: {file2} -> {file1}")
        else:
            copy_file(file2, file1)
            print(f"Copied new file: {file2} to {file1}")  # User Notification
            logger.info(f"Copied new file: {file2} to {file1}")
    except Exception as e:
        print(f"Error processing file {file2} to {file1}: {e}")  # User Notification
        error_logger.error(f"Error processing file {file2} to {file1}: {e}")

# Function to process directories
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

# Traverse through drive2 to compare with drive1
for root, subfolders, files in os.walk(drive2):
    #if os.path.basename(root).endswith('.gdb'):
        #print(f"Skipping folder: {root} (ends with .gdb)")  # User Notification
        #logger.info(f"Skipping folder: {root} (ends with .gdb)")
        #continue

    # Create the relative path for drive2 and corresponding path in drive1
    relative_path = os.path.relpath(root, drive2)
    target_folder = os.path.join(drive1, relative_path)
    os.makedirs(target_folder, exist_ok=True)

    # Collect file pairs for comparison
    for file in files:
        if not file.endswith(('.db', '.Temp')):
            file1 = os.path.join(drive1, relative_path, file)
            file2 = os.path.join(root, file)
            file_pairs.append((file1, file2))

    # Collect subfolders for directory processing
    for subfolder in subfolders:
        if not subfolder.endswith(('.db', '.Temp')):
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
print(f"100% Complete: File and folder differences logged in {log_file_path} and errors in {error_log_file_path}")  # User Notification
