
"""
Author: Felix Ehoneah Naami 

Description:
This script scans the input_folder for duplicate files across both GIS and non-GIS formats.
For each set of duplicates, it retains the oldest copy (based on creation time) and copies the rest to the output_folder, preserving the original directory structure.

Key Features:
- Supports various file types including Shapefiles, Geodatabases, MapInfo files, images, packages, ZIP archives, and generic files.
- For ZIP files, contents are extracted, scanned for duplicates, and copied if necessary.
- Maintains detailed logs and generates a CSV report of all file actions.
- Cleans up empty directories in the input_folder after processing.

Usage:
Customize the `input_folder` and `output_folder` variables before running the script.

"""


import os
import hashlib
import shutil
import logging
import zipfile
import datetime
import time
from collections import defaultdict
import csv
import arcpy
from glob import glob
from pathlib import Path


# === Configuration ===

# Define the input and output directories for scanning and saving duplicate files 
input_folder = r"\\WorldDriveAFR.newmont.net\share\exploration\WORLD\AFRICA\GHA\SEFWI\Ahafo"
output_folder = r"\\WorldDriveAFR.newmont.net\share\exploration\WORLD\AFRICA\GHA\_Presentations\Working Files\Ahafo"

# Supported file extensions by category
SHAPEFILE_EXTENSIONS = ['.shp', '.shx', '.dbf', '.prj', '.cpg', '.sbn', '.sbx', '.shp.xml']
IMAGEFILE_EXTENSIONS = ['.tif', '.jpg', '.tfw', '.aux.xml', '.ovr', '.rrd', '.prj', '.tif.xml', '.tif.aux.xml', '.tif.ovr', '.grd', '.jpeg', '.png', '.png.xml', '.gif', '.bmp', '.tiff', '.img', '.jp2', '.crf', '.psd', '.ora', '.bt', '.dt2', '.ecw', '.dat', '.lgg', '.asc', '.hdr', '.bip', '.bil', '.clr', '.stx', '.bag', '.tff', '.raw', '.ers']
MAPINFO_EXTENSIONS = ['.tab', '.dat', '.dbf', '.map', '.id', '.ind', '.mif', '.mid', '.wor', '.gml', '.tab.xml']
GDB_EXTENSIONS = ['.gdb', '.geodatabase', '.mdb', '.accdb', '.sde']
NON_GIS_DB_EXTENSIONS = ['.sqlite', '.sql']
PACKAGE_EXTENSIONS = ['.mpk', '.mpkx', '.lpk', '.lpkx']
ZIP_EXTENSIONS = ['.zip', '.7z']

# Normalize all extensions to lower-case to ensure consistent comparisons
SHAPEFILE_EXTENSIONS = [ext.lower() for ext in SHAPEFILE_EXTENSIONS]
IMAGEFILE_EXTENSIONS = [ext.lower() for ext in IMAGEFILE_EXTENSIONS]
MAPINFO_EXTENSIONS = [ext.lower() for ext in MAPINFO_EXTENSIONS]
#GDB_EXTENSIONS = [ext.lower() for ext in GDB_EXTENSIONS]
NON_GIS_DB_EXTENSIONS = [ext.lower() for ext in NON_GIS_DB_EXTENSIONS]
PACKAGE_EXTENSIONS = [ext.lower() for ext in PACKAGE_EXTENSIONS]
ZIP_EXTENSIONS = [ext.lower() for ext in ZIP_EXTENSIONS]

# Global log to track all file actions (copy, delete, etc.)
actions_log = []

# Function to calculate SHA-256 hash of a file
def get_file_hash(file_path, logger):
    sha256 = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(8192), b""):
                sha256.update(byte_block)
        return sha256.hexdigest()
    except Exception as e:
        logger.error(f"Error calculating hash for file {file_path}: {e}")
        return None

# Utility function to get creation time of a file
def get_creation_time(path):
    try:
        return os.path.getctime(path)
    except:
        return float('inf')

def _safe_ctime(path):
    try:
        return os.path.getctime(path)
    except FileNotFoundError:
        return float('inf')  

# Print actions to a csv file
def write_csv_report(output_folder):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    report_path = os.path.join(output_folder, f"summary_report_{timestamp}.csv")
    with open(report_path, mode='w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["Action", "Source", "Destination", "File Type"])
        writer.writerows(actions_log)
    print(f"CSV Report saved at: {report_path}")

# Logging
def setup_logger(output_folder):
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log_path = os.path.join(output_folder, f'file_check_log_{timestamp}.txt')
    logger = logging.getLogger("duplicate_logger")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(log_path)
    fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(fh)
    print(f"Logging to file: {log_path}")
    return logger

# Copies a file from the source (src) to the destination (dst) path.
# Ensures the destination directory exists, preserves file metadata,
# logs the action, and records it in the CSV-compatible `actions_log`.
def copy_file(src, dst, logger):
    try:
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        shutil.copy2(src, dst)
        logger.info(f"Copied: {src} -> {dst}")
        actions_log.append(["Copied", src, dst, os.path.splitext(src)[1]])
        print(f"Copied: {src} -> {dst}")
    except Exception as e:
        logger.error(f"Copy failed {src} -> {dst}: {e}")

# Deletes the specified file from disk.
# Logs the deletion and records the action in the `actions_log`. 
def delete_file(path, logger):
    try:
        os.remove(path)
        logger.info(f"Deleted file: {path}")
        actions_log.append(["Deleted", path, "", os.path.splitext(path)[1]])
        print(f"Deleted: {path}")
    except Exception as e:
        logger.error(f"Delete failed: {path} - {e}")

# Deletes a geodatabase folder, retrying multiple times in case the folder is locked or in use.
# This is useful for `.gdb` directories that may have open locks by ArcGIS or background processes.
# Retries `retries` times, waiting `delay` seconds between each attempt.
#def retry_delete_folder(folder_path, logger, retries=5, delay=1):
    #for _ in range(retries):
        #try:
            #shutil.rmtree(folder_path)
            #logger.info(f"Deleted folder: {folder_path}")
            #actions_log.append(["Deleted GDB", folder_path, "", "Geodatabase"])
            #print(f"Deleted folder: {folder_path}")
            #return
        #except Exception as e:
            #logger.error(f"Retry delete failed for {folder_path}: {e}")
            #time.sleep(delay)

# Geodatabase class handles detection, hashing, copying, and deletion of Esri file geodatabases (.gdb).
# It uses ArcPy to introspect the contents of the geodatabase (feature classes and tables) for content hashing.
#class Geodatabase:
    #def __init__(self, path, logger):
        #self.path = path     # Full path to the .gdb folder
        #self.logger = logger  # Logger instance for structured logging

# Computes a unique hash based on the non-spatial contents of all feature classes and tables in the geodatabase.
# Geometry and object ID fields are excluded to avoid variability in hashing.
    #def hash(self):
        #try:
            #arcpy.env.workspace = self.path
            #items = arcpy.ListFeatureClasses() + arcpy.ListTables()
            #if not items:
                #return None  # Return nothing if database is empty
            #hashes = []
            #for item in sorted(items):
                #item_path = os.path.join(self.path, item)
                #fields = [f.name for f in arcpy.ListFields(item_path) if f.type not in ('Geometry', 'OID')]
                #if not fields:
                    #self.logger.warning(f"No valid fields found in {item_path}; skipping.")
                    #continue

                #with arcpy.da.SearchCursor(item_path, fields) as cursor:
                    #for row in cursor:
                        #row_data = ''.join(map(str, row))
                        #hashes.append(hashlib.sha256(row_data.encode()).hexdigest())

            # Combine and hash all item-level hashes into one final hash
            #return hashlib.sha256(''.join(hashes).encode()).hexdigest() if hashes else None
        #except Exception as e:
            #self.logger.error(f"Error hashing geodatabase {self.path}: {e}")
            #return None

    # Verifies that the geodatabase contains valid feature classes or tables.
    # Returns False if database is empty or unreadable.
    #def validate(self):
        #try:
            #arcpy.env.workspace = self.path
            #return bool(arcpy.ListFeatureClasses() or arcpy.ListTables())
        #except:
            #return False

    #def copy_to(self, dst):
        #try:
            #if os.path.exists(dst):
                #shutil.rmtree(dst)
            #arcpy.Copy_management(self.path, dst)
            #self.logger.info(f"Copied GDB from {self.path} -> {dst}")
            #actions_log.append(["Copied GDB", self.path, dst, "Geodatabase"])
            #print(f"Copied GDB: {self.path} -> {dst}")
            #return True
        #except Exception as e:
            #self.logger.error(f"Failed to copy GDB {self.path}: {e}")
            #return False
    
    # Deletes the geodatabase folder using a retry mechanism to handle file locks.
    #def delete(self):
        #retry_delete_folder(self.path, self.logger)

# ShapefileGroup class handles detection, grouping, hashing, copying, and deletion of ESRI Shapefile components.
# A shapefile is composed of multiple files with the same base name but different extensions (.shp, .shx, .dbf, etc.).
class ShapefileGroup:
    def __init__(self, shp_path, logger):
        self.logger = logger
        self.base_path = os.path.splitext(shp_path)[0]  # Base path without extension

        # Collect all files that match the base name and have valid shapefile-related extensions
        all_candidates = glob(f"{self.base_path}.*")
        self.files = [
            f for f in all_candidates
            if os.path.splitext(f)[1].lower() in SHAPEFILE_EXTENSIONS
        ]

        if not self.files:
            self.logger.warning(f"No shapefile components found for {self.base_path}")

    # Computes a combined SHA-256 hash of the shapefile components.
    # This helps detect duplicates based on file content rather than just names.        
    def hash(self):
        sha256 = hashlib.sha256()
        for f in self.files:
            try:
                with open(f, 'rb') as file:
                    for chunk in iter(lambda: file.read(8192), b''):
                        sha256.update(chunk)
            except Exception as e:
                self.logger.error(f"Error reading {f} for hash: {e}")
        return sha256.hexdigest()

    # Copies all shapefile components to the corresponding output folder,
    # preserving relative directory structure.
    def copy_to(self, input_folder, output_folder):
        for f in self.files:
            rel_path = os.path.relpath(os.path.dirname(f), input_folder)
            dst = os.path.join(output_folder, rel_path, os.path.basename(f))
            copy_file(f, dst, self.logger)

    # Deletes all associated shapefile components from disk.
    def delete(self):
        for f in self.files:
            delete_file(f, self.logger)

# ImageGroup class handles grouping, hashing, copying, and deletion of related raster/image files.
# Raster datasets often consist of multiple sidecar files (e.g., .tif, .tfw, .aux.xml, .ovr) that must be treated as a group.
class ImageGroup:
    def __init__(self, file_path, logger):
        self.base_path = os.path.splitext(file_path)[0]
        self.logger = logger
        
        # Collect all files with the same base path that match known raster/image extensions
        self.files = [
            f for f in glob(f"{self.base_path}*")
            if any(f.lower().endswith(ext) for ext in IMAGEFILE_EXTENSIONS)
        ]

    # Compute a SHA-256 hash of all grouped image files.
    # Ensures detection of duplicate datasets based on file content, not name.
    def hash(self):
        sha256 = hashlib.sha256()
        for f in self.files:
            try:
                with open(f, "rb") as file:
                    for chunk in iter(lambda: file.read(8192), b""):
                        sha256.update(chunk)
            except Exception as e:
                self.logger.error(f"Error reading {f} for hash: {e}")
        return sha256.hexdigest()

    # Copy all image components to the output folder, preserving relative directory structure
    def copy_to(self, input_folder, output_folder):
        for f in self.files:
            rel_path = os.path.relpath(os.path.dirname(f), input_folder)
            dst = os.path.join(output_folder, rel_path, os.path.basename(f))
            copy_file(f, dst, self.logger)

    # Delete all image-related files that belong to this group
    def delete(self):
        for f in self.files:
            delete_file(f, self.logger)
        
# ArcGIS PackageFile class
# Handles operations on single ArcGIS package files (e.g., .mpk, .lpkx).
# These are treated as atomic files—self-contained and not grouped with others.
class PackageFile:
    def __init__(self, file_path, logger):
        self.file_path = file_path   # Full path to the package file
        self.logger = logger    # Logger instance for recording actions

    # Compute the hash of the package file for duplicate detection
    def hash(self):
        return get_file_hash(self.file_path, self.logger)

    # Copy the package file to the output folder, preserving folder structure
    def copy_to(self, input_folder, output_folder):
        rel_path = os.path.relpath(os.path.dirname(self.file_path), input_folder)
        dst = os.path.join(output_folder, rel_path, os.path.basename(self.file_path))
        copy_file(self.file_path, dst, self.logger)

    # Delete the original package file (after copying, if a duplicate)
    def delete(self):
        delete_file(self.file_path, self.logger)

# MapInfoGroup class
# Handles grouping, hashing, copying, and deleting of MapInfo file components
# (e.g., .tab, .dat, .map, .tab.xml) that belong to the same logical dataset.
class MapInfoGroup:
    def __init__(self, tab_path, logger):
        self.logger = logger                            # Logger instance for status and error reporting
        self.tab_path = tab_path                        # Full path to the .tab file (entry point)
        self.base_path = self._get_base_path(tab_path)  # Extract base path (without extensions)
        self.files = self._gather_components()          # Collect all related MapInfo files

    def _get_base_path(self, tab_path):
         # Extracts the common base of the file by stripping all extensions,
        # including compound ones like '.tab.xml', e.g., 'map.v1.tab' → 'map'
        # This works even if filename has dots (e.g., 'map.v1.tab')
        base = Path(tab_path)
        while base.suffix:
            base = base.with_suffix('')
        return str(base)

    def _gather_components(self):
        # Searches for all related MapInfo files using defined extensions.
        # Includes both simple (e.g., .tab, .dat) and compound extensions (e.g., .tab.xml).
        matched_files = []
        for ext in MAPINFO_EXTENSIONS:
            pattern = f"{self.base_path}{ext}"
            found = glob(pattern)
            matched_files.extend(found)

            # Check for compound extensions like .tab.xml, .dat.xml, etc.
            compound = glob(f"{self.base_path}{ext}.xml")
            matched_files.extend(compound)

        if not matched_files:
            self.logger.warning(f"No MapInfo components found for base: {self.base_path}")
        else:
            self.logger.info(f"MapInfo components for {self.base_path}: {matched_files}")
        return matched_files

    def hash(self):
        # Computes a combined SHA-256 hash of all related component files
        # to detect duplicates based on content, not just filename.
        sha256 = hashlib.sha256()
        for f in sorted(self.files):    # Sort ensures consistent hash ordering
            try:
                with open(f, 'rb') as file:
                    for chunk in iter(lambda: file.read(8192), b''):
                        sha256.update(chunk)
            except Exception as e:
                self.logger.error(f"Error reading {f} for MapInfo hash: {e}")
        return sha256.hexdigest()

    def copy_to(self, input_folder, output_folder):
        # Copies all component files to the corresponding location in output_folder.
        for f in self.files:
            rel_path = os.path.relpath(os.path.dirname(f), input_folder)
            dst = os.path.join(output_folder, rel_path, os.path.basename(f))
            copy_file(f, dst, self.logger)

    def delete(self):
        # Deletes all grouped component files from the input directory.
        for f in self.files:
            delete_file(f, self.logger)


# ZipFileHandler class
# Handles identification, duplication, and deletion of ZIP archive files.
# Used for detecting and removing duplicate compressed files in the dataset.
class ZipFileHandler:
    def __init__(self, file_path, logger):
        self.file_path = file_path          # Full path to the ZIP or 7z file
        self.logger = logger                # Logger for recording operations and errors
        self.base_path = file_path          # Used as the key path for sorting and tracking

    def hash(self):
        # Generates a SHA-256 hash of the file content to detect duplicates
        return get_file_hash(self.file_path, self.logger)

    def copy_to(self, input_folder, output_folder):
        # Copies the ZIP file to the corresponding location inside the output folder
        rel_path = os.path.relpath(os.path.dirname(self.file_path), input_folder)
        dst = os.path.join(output_folder, rel_path, os.path.basename(self.file_path))
        copy_file(self.file_path, dst, self.logger)

    def delete(self):
        # Deletes the ZIP file from the source directory
        delete_file(self.file_path, self.logger)

# NonGISDatabase class
# Handles detection and cleanup of duplicate non-spatial database files such as .sqlite or .sql.
class NonGISDatabase:
    def __init__(self, file_path, logger):
        self.file_path = file_path          # Full path to the non-GIS database file
        self.logger = logger                # Logger to track actions and errors
        self.base_path = file_path          # Used as a reference path for comparison/sorting

    def hash(self):
        # Generate a SHA-256 hash of the database file content to identify duplicates
        return get_file_hash(self.file_path, self.logger)

    def copy_to(self, input_folder, output_folder):
        # Copy the file into the output directory while preserving relative path structure
        rel_path = os.path.relpath(os.path.dirname(self.file_path), input_folder)
        dst = os.path.join(output_folder, rel_path, os.path.basename(self.file_path))
        copy_file(self.file_path, dst, self.logger)

    def delete(self):
        # Remove the original file after processing (if marked as a duplicate)
        delete_file(self.file_path, self.logger)

# GenericFile class
# Handles duplicate detection and management for non-GIS, non-specialized files.
# Includes all files that are not shapefiles, geodatabases, images, packages, ZIPs, or MapInfo files.
class GenericFile:
    def __init__(self, file_path, logger):
        self.logger = logger
        self.file_path = file_path                      # Original file path
        self.base_path = os.path.splitext(file_path)[0]
        self.files = [
            f for f in glob(f"{self.base_path}*")
            if os.path.isfile(f)
        ]

    def hash(self):
        # Computes a combined SHA-256 hash of all associated file contents in the group
        sha256 = hashlib.sha256()
        for f in self.files:
            try:
                with open(f, 'rb') as file:
                    for chunk in iter(lambda: file.read(8192), b''):
                        sha256.update(chunk)
            except Exception as e:
                self.logger.error(f"Error hashing {f}: {e}")
        return sha256.hexdigest()

    def copy_to(self, input_folder, output_folder):
        # Copies all grouped files to the output directory, preserving folder structure
        for f in self.files:
            rel_path = os.path.relpath(os.path.dirname(f), input_folder)
            dst = os.path.join(output_folder, rel_path, os.path.basename(f))
            copy_file(f, dst, self.logger)

    def delete(self):
        # Deletes all files in the group (only if marked as duplicates)
        for f in self.files:
            delete_file(f, self.logger)


"""
    Generic processor for handling duplicates of files based on extension groups.
    This method works for single-file types (e.g., ZIP, package) or grouped types (e.g., images).
    
    Parameters:
    - handler_class: The class used to wrap and process the file (e.g., ImageGroup, PackageFile).
    - extensions: List of extensions that define this file type.
    - file_label: Descriptive label for logging/reporting (e.g., "Image", "ZIP").
"""
class DuplicateProcessor:
    def __init__(self, input_folder, output_folder, logger):
        # Initializes the processor with input/output paths and a logger instance.
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.logger = logger

    def _process_files(self, handler_class, extensions, file_label):
        hash_to_files = defaultdict(list)       # Maps file hash → list of file/group objects

        # Walk through all files in the input directory
        for root, _, files in os.walk(self.input_folder):
            for file in files:
                # Match by extension (case-insensitive)
                if any(file.lower().endswith(ext) for ext in extensions):
                    path = os.path.join(root, file)
                    obj = handler_class(path, self.logger)      # Wrap file in its handler class
                    file_hash = obj.hash()
                    if file_hash:
                        hash_to_files[file_hash].append(obj)

        # Process each group of files that have identical hashes    
        for group in hash_to_files.values():
            if len(group) > 1:
                # Sort duplicates by file creation time (older = original)
                group.sort(key=lambda x: _safe_ctime(getattr(x, 'base_path', getattr(x, 'file_path', ''))))
                original = group[0]
                duplicates = group[1:]

                # Retrieve a representative path for logging
                path_attr = getattr(original, 'base_path', getattr(original, 'file_path', None))
                if path_attr:
                    self.logger.info(f"Retaining original {file_label}: {path_attr}")
                    print(f"Retaining original: {path_attr}")

                # Copy and delete each duplicate
                for dup in duplicates:
                    dup_path = getattr(dup, 'base_path', getattr(dup, 'file_path', None))
                    if not dup_path:
                        continue
                    rel_path = os.path.relpath(os.path.dirname(dup_path), self.input_folder)
                    dst = os.path.join(self.output_folder, rel_path, os.path.basename(dup_path))
                    
                    # Call .copy_to() using flexible signature
                    if hasattr(dup, 'copy_to'):
                        try:
                            # Support both signatures
                            dup.copy_to(self.input_folder, self.output_folder)
                        except TypeError:
                            dup.copy_to(dst)
                    
                    # Delete from input folder after successful copy
                    if hasattr(dup, 'delete'):
                        dup.delete()

        """
    Detects and handles duplicate geodatabases (.gdb, .mdb, etc.) within the input folder.

    This method:
    - Recursively scans all directories for geodatabases.
    - Generates a content hash for each geodatabase using its internal data (via arcpy).
    - Retains the oldest geodatabase (based on creation time).
    - Copies duplicates to the output folder before deleting them from the input folder.
    """
    #def _process_gdbs(self):
        #hash_to_gdbs = defaultdict(list)                # Dictionary to group geodatabases by content hash
        
        # Traverse the input directory to find geodatabases
        #for root, dirs, _ in os.walk(self.input_folder):
            #for d in dirs:
                #gdb_path = os.path.join(root, d)
                #if any(gdb_path.endswith(ext) for ext in GDB_EXTENSIONS):
                    #gdb = Geodatabase(gdb_path, self.logger)
                    #gdb_hash = gdb.hash()
                    #if gdb_hash:
                        #hash_to_gdbs[gdb_hash].append(gdb)

        # Process groups of geodatabases with identical content
        #for group in hash_to_gdbs.values():
            #if len(group) > 1:
                # Sort by creation time to retain the oldest geodatabase
                #group.sort(key=lambda x: _safe_ctime(x.path))
                #original = group[0]
                #duplicates = group[1:]
                
                #self.logger.info(f"Retaining original Geodatabase: {original.path}")
                #print(f"Retaining original: {original.path}")
                
                # Copy and delete duplicate geodatabases
                #for dup in duplicates:
                    #rel = os.path.relpath(os.path.dirname(dup.path), self.input_folder)
                    #dst = os.path.join(self.output_folder, rel, os.path.basename(dup.path))
                    #if dup.validate():              # Ensure the geodatabase is accessible and not corrupted
                        #if dup.copy_to(dst):
                            #dup.delete()

    
    """
    Identifies and processes duplicate shapefile groups within the input directory.

    Shapefiles consist of multiple files (.shp, .shx, .dbf, .prj, etc.) sharing the same base name.
    This method:
    - Searches for all .shp files and builds complete file groups using the ShapefileGroup class.
    - Computes a hash for the grouped files based on their content.
    - Identifies duplicates by comparing hashes.
    - Retains the oldest shapefile group (based on creation time).
    - Copies duplicates to the output folder and deletes them from the input.
    """
    def _process_shapefiles(self):
        hash_to_shapes = defaultdict(list)

        # Walk through all files under the input directory
        for root, _, files in os.walk(self.input_folder):
            for file in files:
                if file.lower().endswith(".shp"):           # Only consider .shp as entry point
                    path = os.path.join(root, file)
                    shp = ShapefileGroup(path, self.logger)  # Construct full group using base name
                    shp_hash = shp.hash()                    # Generate hash for the shapefile group
                    if shp_hash:
                        hash_to_shapes[shp_hash].append(shp)

        # Identify and process duplicate shapefiles
        for group in hash_to_shapes.values():
            if len(group) > 1:
                # Sort to retain the oldest based on creation timestamp
                group.sort(key=lambda x: _safe_ctime(x.base_path))
                original = group[0]
                duplicates = group[1:]
                
                self.logger.info(f"Retaining original Shapefile: {original.base_path}")
                print(f"Retaining original: {original.base_path}")
                
                # Copy and delete each duplicate
                for dup in duplicates:
                    dup.copy_to(self.input_folder, self.output_folder)
                    dup.delete()

    
    """
    Identifies and handles duplicate MapInfo file groups in the input directory.

    MapInfo datasets typically consist of multiple files (.tab, .dat, .map, .id, .ind, etc.)
    that share a common base name. This method:
    - Locates all .tab files to initiate the group construction.
    - Uses MapInfoGroup to collect associated components for hashing.
    - Compares file group hashes to identify duplicates.
    - Retains the oldest version based on file creation time.
    - Copies duplicate groups to the output directory before deletion.
    """
    def _process_mapinfo(self):
        hash_to_groups = defaultdict(list)              # Holds MapInfo groups keyed by content hash

        # Traverse input folder recursively to find .tab files
        for root, _, files in os.walk(self.input_folder):
            for file in files:
                if file.lower().endswith(".tab"):
                    path = os.path.join(root, file)
                    group = MapInfoGroup(path, self.logger)     # Create a full MapInfo file group
                    group_hash = group.hash()     # Generate hash representing the group’s content
                    if group_hash:
                        hash_to_groups[group_hash].append(group)

        # Identify and handle duplicates
        for group in hash_to_groups.values():
            if len(group) > 1:
                # Sort by creation time to retain the oldest group
                group.sort(key=lambda x: _safe_ctime(x.base_path))
                original = group[0]
                duplicates = group[1:]
                self.logger.info(f"Retaining original MapInfo: {original.base_path}")
                print(f"Retaining original: {original.base_path}")
                
                # Copy and delete all identified duplicates
                for dup in duplicates:
                    dup.copy_to(self.input_folder, self.output_folder)
                    dup.delete()


    """
    Detects and removes duplicate *non-GIS-specific* files from the input directory.

    This method handles all files that do not match known GIS-related extensions 
    (like shapefiles, geodatabases, MapInfo files, images, packages, and zip archives).

    - Skips known GIS file types based on their extensions.
    - Groups files by content hash using the GenericFile handler.
    - For each group of duplicates:
        - Retains the oldest version based on creation time.
        - Copies the duplicate(s) to the output folder for record-keeping.
        - Deletes the duplicate(s) from the input directory.
    """
    def _process_generic_files(self):
        # Combine all known extensions to exclude GIS-related files
        known_exts = set(
            SHAPEFILE_EXTENSIONS + IMAGEFILE_EXTENSIONS + MAPINFO_EXTENSIONS +
            GDB_EXTENSIONS + NON_GIS_DB_EXTENSIONS + PACKAGE_EXTENSIONS + ZIP_EXTENSIONS
        )
        hash_to_files = defaultdict(list)           # Groups of files by their content hash

        # Traverse input directory to identify unknown (generic) file types
        for root, _, files in os.walk(self.input_folder):
            for file in files:
                ext = os.path.splitext(file)[1].lower()
                if ext in known_exts:
                    continue                # Skip files with known GIS-related extensions
                path = os.path.join(root, file)
                gf = GenericFile(path, self.logger)
                f_hash = gf.hash()
                if f_hash:
                    hash_to_files[f_hash].append(gf)

        # Process duplicates: retain oldest, copy/delete the rest
        for group in hash_to_files.values():
            if len(group) > 1:
                group.sort(key=lambda x: _safe_ctime(getattr(x, 'base_path', getattr(x, 'file_path', ''))))
                original = group[0]
                duplicates = group[1:]
                self.logger.info(f"Retaining original file: {original.file_path}")
                print(f"Retaining original: {original.file_path}")
                for dup in duplicates:
                    dup.copy_to(self.input_folder, self.output_folder)
                    dup.delete()



    """
    Executes all duplicate detection and removal processes for different file types.

    The processing follows this order:
    1. Geodatabases (.gdb, .mdb, etc.)
    2. Shapefiles (.shp and associated components)
    3. MapInfo files (.tab and its related files)
    4. Image files (e.g., .tif, .jpg, etc.)
    5. ArcGIS packages (.mpk, .lpk, etc.)
    6. ZIP archives (.zip, .7z)
    7. Non-GIS database files (.sqlite, .sql)
    8. Generic files not matching any known extension group

    Each method identifies duplicates by computing a hash and comparing them.
    Retains the oldest version and deletes the rest after copying to the output folder.
    """
    def run_all(self):
        #self._process_gdbs()
        self._process_shapefiles()
        self._process_mapinfo()
        self._process_files(ImageGroup, IMAGEFILE_EXTENSIONS, "Image")
        self._process_files(PackageFile, PACKAGE_EXTENSIONS, "Package")
        self._process_files(ZipFileHandler, ZIP_EXTENSIONS, "ZIP")
        self._process_files(NonGISDatabase, NON_GIS_DB_EXTENSIONS, "Non-GIS Database")
        self._process_generic_files()


"""
    Main script entry point.

    Responsibilities:
    - Ensure the output directory exists.
    - Set up logging to capture actions and errors.
    - Instantiate the DuplicateProcessor to perform all deduplication tasks.
    - Generate a CSV report summarizing actions taken.

    """
def main():
    os.makedirs(output_folder, exist_ok=True)
    logger = setup_logger(output_folder)
    logger.info("Starting duplicate scan...")
    print("Starting duplicate scan...")

    processor = DuplicateProcessor(input_folder, output_folder, logger)
    processor.run_all()

    write_csv_report(output_folder)
    logger.info("Duplicate processing complete.")
    print("✅ 100% Duplicate processing complete.")

# Ensure script runs only when executed directly, not when imported
if __name__ == '__main__':
    main()
