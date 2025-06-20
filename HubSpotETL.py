import os
import pandas as pd
from datetime import datetime, date, timedelta
import glob
import time
from logger_config import logger
import shutil
import zipfile
import sys
import re
import json
from constants import (
    SOURCE_FOLDER,
    STAGING_FOLDER,
    DESTINATION_FOLDER,
    isTest,
    NUM_EXTRACTS_TO_KEEP,
)

os.makedirs(STAGING_FOLDER, exist_ok=True)
if isTest:
    os.makedirs(DESTINATION_FOLDER, exist_ok=True)

files = []


def cleanup_staging_folder(staging_folder):
    try:
        print("Cleaning up staging...")
        logger.info("Cleaning up staging...")
        # Remove all files
        for file in os.listdir(staging_folder):
            file_path = os.path.join(staging_folder, file)
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"Removed {file_path}")
                logger.info(f"Removed {file_path}")
    except Exception as ex:
        logger.exception(f"Error cleaning up staging: {ex}")
        sys.exit(1)
        raise


def copy_files_to_network(files, PROCESSDATE, source_folder, network_folder):
    try:
        print(f"DEBUG: network_folder = '{network_folder}'")
        print(f"DEBUG: repr(network_folder) = {repr(network_folder)}")
        print(f"DEBUG: os.path.exists = {os.path.exists(network_folder)}")
        print(f"DEBUG: os.path.isdir = {os.path.isdir(network_folder)}")
        try:
            print(f"DEBUG: os.listdir(network_folder) = {os.listdir(network_folder)}")
        except Exception as e:
            print(f"DEBUG: os.listdir(network_folder) failed: {e}")
        # Ensure the network folder is a directory
        if not os.path.isdir(network_folder):
            print(f"Error: The path {network_folder} exists but is not a directory.")
            logger.error(f"The path {network_folder} exists but is not a directory.")
            raise NotADirectoryError(
                f"The path {network_folder} exists but is not a directory."
            )

        for file_path, filename in files:
            # Check if the file is accessible
            if not os.path.exists(file_path):
                print(f"Error: File {file_path} does not exist.")
                logger.error(f"Error: File {file_path} does not exist.")
                raise FileNotFoundError(f"File {file_path} does not exist.")
            if not os.access(file_path, os.R_OK):
                print(
                    f"Error: File {file_path} is not accessible (no read permissions)."
                )
                logger.error(
                    f"Error: File {file_path} is not accessible (no read permissions)."
                )
                raise PermissionError(
                    f"File {file_path} is not accessible (no read permissions)."
                )

    except Exception as ex:
        logger.exception(f"Error accessing network file: {ex}")
        sys.exit(1)
        raise

    # today = date.today()
    # yesterday = (today - pd.Timedelta(days=1)).strftime("%Y%m%d")

    # Create a subfolder in the network folder
    dated_network_folder = os.path.join(network_folder, PROCESSDATE)
    if not os.path.exists(dated_network_folder):
        os.makedirs(dated_network_folder, exist_ok=True)

    # Copy the masked EXTRACT.CARD file to the network folder, replacing it if it already exists
    destination = os.path.join(os.path.join(network_folder, PROCESSDATE), filename)
    try:
        shutil.copyfile(
            os.path.join(file_path, filename), destination
        )  # Overwrites if the file exists
        print(f"Copied {file_path} to {destination}")
        logger.info(f"Copied {file_path} to {destination}")
    except Exception as e:
        logger.exception(
            f"Error copying {file_path} to network folder: {destination}: {e}"
        )
        sys.exit(1)
        raise

    # Copy all files in the source folder for the given PROCESSDATE
    source_folder_path = os.path.join(source_folder, PROCESSDATE)
    all_files = [
        os.path.join(source_folder_path, f)
        for f in os.listdir(source_folder_path)
        if os.path.isfile(os.path.join(source_folder_path, f))
    ]
    try:
        for src_path in all_files:
            filename = os.path.basename(src_path)
            if filename == "EXTRACT.CARD":
                continue  # Skip EXTRACT.CARD
            dest_path = os.path.join(
                os.path.join(network_folder, PROCESSDATE), filename
            )
            shutil.copyfile(src_path, dest_path)
            print(f"Copied {src_path} to {dest_path}")
            logger.info(f"Copied {src_path} to {dest_path}")
        print("Files successfully copied to network folder...")
        logger.info("Files successfully copied to network folder...")
    except:
        logger.exception(f"Error copying EXTRACT.* files: {ex}")
        sys.exit(1)
        raise


def mask_column(series):
    # Mask all but last 4 characters
    return series.apply(
        lambda x: (
            "*" * (len(str(x)) - 4) + str(x)[-4:]
            if pd.notnull(x) and len(str(x)) > 4
            else x
        )
    )


def slice_last4_column(series):
    # Return only the last 4 characters of each value
    return series.apply(
        lambda x: str(x)[-4:] if pd.notnull(x) and len(str(x)) > 4 else x
    )


def process_extract_card(PROCESSDATE):
    # Find subfolder(YYYYMMDD) in source_folder ("\\vsarcu02\k$\ARCUFTP_ARCHIVE\SYM000")
    subfolder_path = os.path.join(SOURCE_FOLDER, PROCESSDATE)
    extract_card_path = os.path.join(subfolder_path, "EXTRACT.CARD")

    lastfile_path = os.path.join(subfolder_path, "XTRACT_LASTFILE.txt")
    if os.path.exists(lastfile_path):
        logger.info(f"Found {lastfile_path}")
    else:
        print(f"File {lastfile_path} not found. Extract incomplete.")
        logger.warning(f"File {lastfile_path} not found. Extract incomplete.")
        sys.exit(2)

    if os.path.exists(extract_card_path):
        if not os.path.exists(STAGING_FOLDER):
            os.makedirs(STAGING_FOLDER, exist_ok=True)
        dest_path = os.path.join(STAGING_FOLDER, "EXTRACT.CARD")
        shutil.copyfile(extract_card_path, dest_path)
        print(f"Copied {extract_card_path} to {dest_path}")
        logger.info(f"Copied {extract_card_path} to {dest_path}")

    else:
        print(f"File {extract_card_path} not found.")
        logger.warning(f"File {extract_card_path} not found.")
        sys.exit(2)

    # Read the file with no header, ~ delimiter
    df = pd.read_csv(dest_path, delimiter="~", header=None, dtype=str)
    # Mask the 3rd column (index 2)
    df[2] = mask_column(df[2])

    # # Add two null columns to the end of df
    # df[len(df.columns)] = None
    # df[len(df.columns)] = None

    # Print and log the number of columns
    num_columns = df.shape[1]
    print(f"Number of columns in DataFrame: {num_columns}")
    logger.info(f"Number of columns in DataFrame: {num_columns}")

    print("Processed EXTRACT.CARD file and masked the 3rd column.")
    logger.info("Processed EXTRACT.CARD file and masked the 3rd column.")
    return df


def zip_network_folder(network_folder, PROCESSDATE):
    folder_to_zip = os.path.join(network_folder, PROCESSDATE)
    zip_path = os.path.join(network_folder, f"{PROCESSDATE}.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(folder_to_zip):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, folder_to_zip)
                zipf.write(file_path, arcname)
    print(f"Created zip archive: {zip_path}")
    logger.info(f"Created zip archive: {zip_path}")


def save_masked_card_extract(df, staging_folder):
    # Save the processed DataFrame to CSV in the same format as the original
    csv_file_path = os.path.join(staging_folder, f"EXTRACT.CARD")
    df.to_csv(
        csv_file_path,
        sep="~",
        header=False,
        index=False,
        quoting=3,  # csv.QUOTE_NONE
    )
    print(f"Processed data saved to {csv_file_path}")
    logger.info(f"Processed data saved to {csv_file_path}")


def cleanup_network_folder(network_folder, PROCESSDATE):
    # Path to the dated subfolder and the zip file
    dated_folder = os.path.join(network_folder, PROCESSDATE)
    zip_file = os.path.join(network_folder, f"{PROCESSDATE}.zip")

    # Remove the dated folder and its contents if it exists
    if os.path.exists(dated_folder) and os.path.isdir(dated_folder):
        shutil.rmtree(dated_folder)
        print(f"Removed folder and contents: {dated_folder}")
        logger.info(f"Removed folder and contents: {dated_folder}")

    # Optionally, check if the zip file exists
    if os.path.exists(zip_file):
        print(f"Zip file remains: {zip_file}")
        logger.info(f"Zip file remains: {zip_file}")
    else:
        print(f"Warning: Zip file not found: {zip_file}")
        logger.warning(f"Zip file not found: {zip_file}")


def keep_latest_number_of_extracts(DESTINATION_FOLDER, NUM_EXTRACTS_TO_KEEP):
    # Regex to match filenames like 20240618.zip
    pattern = re.compile(r"^(\d{8})\.zip$")
    files = []

    for filename in os.listdir(DESTINATION_FOLDER):
        match = pattern.match(filename)
        if match:
            date_str = match.group(1)
            try:
                file_date = datetime.strptime(date_str, "%Y%m%d")
                files.append((file_date, filename))
            except ValueError:
                continue

    # Sort files by date descending
    files.sort(reverse=True)
    # Keep only the latest num_extracts
    for _, filename in files[NUM_EXTRACTS_TO_KEEP:]:
        file_path = os.path.join(DESTINATION_FOLDER, filename)
        os.remove(file_path)


def get_process_date():
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    process_date = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            try:
                config = json.load(f)
                if (
                    "ProcessDate" in config
                    and config["ProcessDate"]
                    and str(config["ProcessDate"]).lower() != "null"
                ):
                    process_date = config["ProcessDate"]
            except Exception as e:
                print(f"Warning: Could not read config.json: {e}")
    return process_date


# Main function
def main(start_time):

    try:
        # Define schedule
        # today = date.today()
        # yesterday = (today - pd.Timedelta(days=1)).strftime("%Y%m%d")

        PROCESSDATE = get_process_date()
        print(f"Using process date: {PROCESSDATE}")
        logger.info(f"Using process date: {PROCESSDATE}")

        # # Process the file and mask the 3rd column
        print("Processing EXTRACT.CARD file...")
        df = process_extract_card(PROCESSDATE)

        print(df.head())  # Show a preview or continue processing as needed
        logger.info(f"Processed data:\n{df.head()}")

        # Save the processed DataFrame to CSV in the same format as the original
        print("Saving masked card extract...")
        save_masked_card_extract(df, STAGING_FOLDER)

        # Add the CSV file path to the list of files to copy
        files.append((STAGING_FOLDER, f"EXTRACT.CARD"))

        # Copy files to the network folder
        print("Copying files to network folder...")
        logger.info("Copying files to network folder...")
        copy_files_to_network(files, PROCESSDATE, SOURCE_FOLDER, DESTINATION_FOLDER)

        print("Zipping network folder...")
        logger.info("Zipping network folder...")
        zip_network_folder(DESTINATION_FOLDER, PROCESSDATE)

        print("Cleaning up network folder...")
        logger.info("Cleaning up network folder...")
        cleanup_network_folder(DESTINATION_FOLDER, PROCESSDATE)

        print("Cleaning up staging folder...")
        logger.info("Cleaning up staging folder...")
        cleanup_staging_folder(STAGING_FOLDER)

        print(f"Keeping latest {NUM_EXTRACTS_TO_KEEP} extracts...")
        logger.info(f"Keeping latest {NUM_EXTRACTS_TO_KEEP} extracts...")
        keep_latest_number_of_extracts(DESTINATION_FOLDER, NUM_EXTRACTS_TO_KEEP)

    except Exception as ex:
        logger.exception(f"Error in main execution: {ex}")
        sys.exit(1)
        raise
    finally:
        elapsed = time.time() - start_time
        elapsed_minutes = elapsed / 60
        print(f"Main completed in {elapsed_minutes:.2f} minutes.")
        logger.info(f"Main completed in {elapsed_minutes:.2f} minutes.")


if __name__ == "__main__":
    starting_time = time.time()
    print("Starting HubSpot Export...")
    logger.info("Starting HubSpot Export...")
    main(starting_time)
