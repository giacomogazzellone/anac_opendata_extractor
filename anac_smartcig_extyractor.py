# Replicability Script: ANAC SmartCIG Data Extraction
# Author: [Your Name/Research Unit]
# Description: This script automates the retrieval and cleaning of SmartCIG 
# datasets from the official ANAC portal, specifically for simplified procedures.

import pandas as pd
import requests
import io
import zipfile
import os
import sys

# Adding the root directory to sys.path to allow importing from 'utils'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.data_utils import clean_currency

def run_smartcig_extraction(config):
    """
    Handles the batch download and processing of ANAC SmartCIG CSV files.
    """
    output_path = config['output_filename']
    write_header = not os.path.exists(output_path)

    print(f"--- Starting SMARTCIG extraction: {config['start_year']} - {config['end_year']} ---")

    for year in range(config['start_year'], config['end_year'] + 1):
        for month in range(1, 13):
            month_str = f"{month:02d}"
            
            # SmartCIG specific URL pattern
            url = f"https://dati.anticorruzione.it/opendata/download/dataset/smartcig-{year}/filesystem/smartcig_csv_{year}_{month_str}.zip"
            
            try:
                print(f"Downloading SmartCIG: {year}/{month_str}...", end="\r")
                response = requests.get(url, timeout=30)
                response.raise_for_status()

                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    csv_filename = z.namelist()[0]
                    with z.open(csv_filename) as f:
                        # Header Peek
                        file_header = pd.read_csv(f, sep=';', encoding='latin1', nrows=0).columns.tolist()
                        
                        missing_cols = [c for c in config['target_columns'] if c not in file_header]
                        
                        if missing_cols:
                            print(f"\n[COLUMN ERROR] {year}/{month_str} skipped. Missing: {missing_cols}")
                            continue 

                        f.seek(0) 
                        reader = pd.read_csv(
                            f, 
                            sep=';', 
                            encoding='latin1', 
                            usecols=config['target_columns'], 
                            chunksize=40000
                        )

                        for chunk in reader:
                            # Apply filters
                            mask = True
                            for col, val in config['value_filters'].items():
                                mask &= (chunk[col].astype(str).str.upper() == str(val).upper())
                            
                            filtered_df = chunk[mask].copy()

                            if not filtered_df.empty:
                                # Clean amount using centralized utility function
                                if config['clean_amounts'] and config['amount_column'] in filtered_df.columns:
                                    filtered_df[config['amount_column']] = clean_currency(filtered_df[config['amount_column']])

                                # Save result
                                filtered_df.to_csv(output_path, mode='a', index=False, header=write_header, sep=',')
                                write_header = False
                                print(f"Saved {len(filtered_df)} SmartCIG records for {year}/{month_str}!")

            except Exception:
                # Silently skip errors (e.g., 404 for future months)
                continue

    print(f"\n--- Extraction complete. File saved in: {os.path.abspath(output_path)} ---")

# ============================================================
# SMARTCIG CONFIGURATION
# ============================================================

smartcig_configuration = {
    'start_year': 2021,     # < --- CHOOSE START YEAR
    'end_year': 2023,       # < --- CHOOSE END YEAR
    'target_columns': ['smart_cig', 'oggetto_lotto', 'importo_lotto', 'denominazione_stazione_appaltante'], # < --- CHOOSE COLUMNS TO KEEP (strongly suggest to keep month, year, cig)
    'value_filters': {},    # < --- CHOOSE FILTERS,  Leave empty to get all procedures
    'output_filename': 'smartcig_extraction_2021_2023.csv',  # < --- SAVE AND RENAME FILE
    'clean_amounts': True,
    'amount_column': 'importo_lotto'
}

if __name__ == "__main__":
    run_smartcig_extraction(smartcig_configuration)
