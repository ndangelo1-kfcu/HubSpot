import os
import pandas as pd
from datetime import datetime, date
import glob
import time
from logger_config import logger
import shutil
import zipfile
import sys
import re
from constants import (
    source_folder,
    staging_folder,
    destination_folder,
    isTest,
    num_extracts_to_keep,
)

os.makedirs(staging_folder, exist_ok=True)
if isTest:
    os.makedirs(destination_folder, exist_ok=True)

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


def copy_files_to_network(files, source_folder, network_folder):
    try:
        # Ensure the network folder exists and is a directory
        if not os.path.exists(network_folder):
            os.makedirs(network_folder, exist_ok=True)
        elif not os.path.isdir(network_folder):
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

    today = date.today()
    yesterday = (today - pd.Timedelta(days=1)).strftime("%Y%m%d")

    # Create a subfolder in the network folder
    dated_network_folder = os.path.join(network_folder, yesterday)
    if not os.path.exists(dated_network_folder):
        os.makedirs(dated_network_folder, exist_ok=True)

    # Copy the masked EXTRACT.CARD file to the network folder, replacing it if it already exists
    destination = os.path.join(os.path.join(network_folder, yesterday), filename)
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

    # Copy all all other EXTRACT.* files:
    # Find all files that match EXTRACT.*
    pattern = os.path.join(os.path.join(source_folder, yesterday), "EXTRACT.*")
    extract_files = glob.glob(pattern)
    try:
        x = 10
        for src_path in extract_files:
            filename = os.path.basename(src_path)
            if filename == "EXTRACT.CARD":
                continue  # Skip EXTRACT.CARD
            dest_path = os.path.join(os.path.join(network_folder, yesterday), filename)
            shutil.copyfile(src_path, dest_path)
            print(f"Copied {src_path} to {dest_path}")
            logger.info(f"Copied {src_path} to {dest_path}")
            x = x - 1
            if x == 0:
                break  # testing
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


def process_extract_card(file_path):
    # Read the file with no header, ~ delimiter
    df = pd.read_csv(file_path, delimiter="~", header=None, dtype=str)
    # Mask the 3rd column (index 2)
    df[2] = mask_column(df[2])
    print("Processed EXTRACT.CARD file and masked the 3rd column.")
    logger.info("Processed EXTRACT.CARD file and masked the 3rd column.")
    return df


def zip_network_folder(network_folder, yesterday):
    folder_to_zip = os.path.join(network_folder, yesterday)
    zip_path = os.path.join(network_folder, f"{yesterday}.zip")
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
    csv_file_path = os.path.join(staging_folder, f"EXTRACT.CARD_masked.txt")
    df.to_csv(
        csv_file_path,
        sep="~",
        header=False,
        index=False,
        quoting=3,  # csv.QUOTE_NONE
    )
    print(f"Processed data saved to {csv_file_path}")
    logger.info(f"Processed data saved to {csv_file_path}")


def cleanup_network_folder(network_folder, yesterday):
    # Path to the dated subfolder and the zip file
    dated_folder = os.path.join(network_folder, yesterday)
    zip_file = os.path.join(network_folder, f"{yesterday}.zip")

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


def keep_latest_number_of_extracts(destination_folder, num_extracts=7):
    # Regex to match filenames like 20240618.zip
    pattern = re.compile(r"^(\d{8})\.zip$")
    files = []

    for filename in os.listdir(destination_folder):
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
    for _, filename in files[num_extracts:]:
        file_path = os.path.join(destination_folder, filename)
        os.remove(file_path)


# Main function
def main():
    start_time = time.time()
    try:
        # Define schedule
        today = date.today()
        yesterday = (today - pd.Timedelta(days=1)).strftime("%Y%m%d")

        # Find subfolder(YYYYMMDD) in source_folder ("\\vsarcu02\k$\ARCUFTP_ARCHIVE\SYM000")
        subfolder_path = os.path.join(source_folder, yesterday)
        extract_card_path = os.path.join(subfolder_path, "EXTRACT.CARD")

        if os.path.exists(extract_card_path):
            if not os.path.exists(staging_folder):
                os.makedirs(staging_folder, exist_ok=True)
            dest_path = os.path.join(staging_folder, "EXTRACT.CARD")
            shutil.copyfile(extract_card_path, dest_path)
            print(f"Copied {extract_card_path} to {dest_path}")
            logger.info(f"Copied {extract_card_path} to {dest_path}")

        else:
            print(f"File {extract_card_path} not found.")
            logger.warning(f"File {extract_card_path} not found.")

        # Process the file and mask the 3rd column
        print("Processing EXTRACT.CARD file...")
        df = process_extract_card(dest_path)

        print(df.head())  # Show a preview or continue processing as needed
        logger.info(f"Processed data:\n{df.head()}")

        # Save the processed DataFrame to CSV in the same format as the original
        print("Saving masked card extract...")
        save_masked_card_extract(df, staging_folder)

        # Add the CSV file path to the list of files to copy
        files.append((staging_folder, f"EXTRACT.CARD_masked.txt"))

        # Copy files to the network folder
        print("Copying files to network folder...")
        logger.info("Copying files to network folder...")
        copy_files_to_network(files, source_folder, destination_folder)

        print("Zipping network folder...")
        logger.info("Zipping network folder...")
        zip_network_folder(destination_folder, yesterday)

        print("Cleaning up network folder...")
        logger.info("Cleaning up network folder...")
        cleanup_network_folder(destination_folder, yesterday)

        print("Cleaning up staging folder...")
        logger.info("Cleaning up staging folder...")
        cleanup_staging_folder(staging_folder)

        print(f"Keeping latest {num_extracts_to_keep} extracts...")
        logger.info(f"Keeping latest {num_extracts_to_keep} extracts...")
        keep_latest_number_of_extracts(destination_folder, num_extracts_to_keep)

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
    print("Starting HubSpot Export...")
    main()
