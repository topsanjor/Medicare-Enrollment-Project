# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b78a635f-b24b-4c5a-a036-d4843a7afc22",
# META       "default_lakehouse_name": "rawFiles",
# META       "default_lakehouse_workspace_id": "3b06e9fb-b107-48ac-9eb9-bfbc1e4e89ec",
# META       "known_lakehouses": [
# META         {
# META           "id": "b78a635f-b24b-4c5a-a036-d4843a7afc22"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "a45ea507-60c2-424f-9268-ff75de6a4310",
# META       "known_warehouses": [
# META         {
# META           "id": "a45ea507-60c2-424f-9268-ff75de6a4310",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import requests

folder_path = "/lakehouse/rawFiles/Files/MedicareEnrollmentData"
os.makedirs(folder_path, exist_ok=True)

file_urls = [
    "https://data.cms.gov/download/abcd/file1.zip",
    "https://data.cms.gov/download/abcd/file2.zip"
]

for url in file_urls:
    filename = url.split("/")[-1]
    save_path = os.path.join(folder_path, filename)
    tmp_path = save_path + ".tmp"

    # Delete previous file if exists
    if os.path.exists(save_path):
        os.remove(save_path)

    # Download file
    response = requests.get(url)
    response.raise_for_status()

    # Atomic write
    with open(tmp_path, "wb") as f:
        f.write(response.content)
    os.replace(tmp_path, save_path)  # overwrite

print("All files downloaded in overwrite mode successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import requests
import zipfile

# -----------------------------
# CONFIG
# -----------------------------
lakehouse_folder = "/lakehouse/rawFiles/Files/MedicareTotalEnrollmentData"
os.makedirs(lakehouse_folder, exist_ok=True)

# List of all downloadable ZIP/CSV URLs from the CMS Medicare Total Enrollment dataset
file_urls = [
    "https://data.cms.gov/sites/default/files/2025-09/e97fd774-4859-43bd-87b7-38e734075aa5/MDCR%20ENROLL%20AB%201-8_CPS_02ENR_2023.zip",
    "https://data.cms.gov/sites/default/files/2025-09/dd229ae6-3570-4311-9a52-6e987e95f0a2/MDCR%20ENROLL%20AB%201-8_CPS_02ENR_2022.zip",
    "https://data.cms.gov/sites/default/files/2023-02/CPS%20MDCR%20ENROLL%20AB%201-8%202021.zip",
    "https://data.cms.gov/sites/default/files/2022-02/CPS%20MDCR%20ENROLL%20AB%201-8%202020.zip",
    "https://data.cms.gov/sites/default/files/2021-01/CPS_MDCR_ENROLL_AB_1-8.zip",
    "https://data.cms.gov/sites/default/files/2021-01/CPS%20MDCR%20TOTAL%20ENROLL%20AB%201-8.zip",
    "https://data.cms.gov/sites/default/files/2019-12/2017_Total_Med_Enroll.zip",
    "https://data.cms.gov/sites/default/files/2020-01/2016_Total_Med_Enroll_0.zip",
    "https://data.cms.gov/sites/default/files/2020-01/2015_Total_Med_Enroll.zip",
    "https://data.cms.gov/sites/default/files/2020-01/2014_Total_Med_Enroll.zip",
    "https://data.cms.gov/sites/default/files/2020-01/2013_Total_Med_Enroll_0.zip"
]

# -----------------------------
# DOWNLOAD FILES
# -----------------------------
for url in file_urls:
    filename = url.split("/")[-1]
    save_path = os.path.join(lakehouse_folder, filename)
    tmp_path = save_path + ".tmp"

    # Delete previous file if it exists (overwrite mode)
    if os.path.exists(save_path):
        os.remove(save_path)

    print(f"Downloading {filename} ...")
    r = requests.get(url)
    r.raise_for_status()  # raise error if download fails

    # Atomic write
    with open(tmp_path, "wb") as f:
        f.write(r.content)
    os.replace(tmp_path, save_path)

print("All files downloaded successfully!")

# -----------------------------
# OPTIONAL: Extract ZIP files
# -----------------------------
for f in os.listdir(lakehouse_folder):
    if f.endswith(".zip"):
        zip_path = os.path.join(lakehouse_folder, f)
        print(f"Extracting {f} ...")
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(lakehouse_folder)

print("All ZIP files extracted.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
