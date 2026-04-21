# Replicability Script: ANAC (Italian Anti-Corruption Authority) Open Data Extraction
# Author: [Your Name/Research Unit]
# Description: This script automates the retrieval, filtering, and cleaning of 
# CIG (Codice Identificativo Gara) datasets from the official ANAC portal.

import pandas as pd
import requests
import io
import zipfile
import os

def run_anac_extraction(config):
    """
    Handles the batch download and processing of ANAC CSV files.
    Maintains Italian column names as per original source to ensure data integrity.
    """
    output_path = config['output_filename']
    write_header = not os.path.exists(output_path)

    print(f"--- Starting extraction period: {config['start_year']} - {config['end_year']} ---")

    for year in range(config['start_year'], config['end_year'] + 1):
        for month in range(1, 13):
            month_str = f"{month:02d}"
            # Official ANAC Open Data URL pattern
            url = f"https://dati.anticorruzione.it/opendata/download/dataset/cig-{year}/filesystem/cig_csv_{year}_{month_str}.zip"
            
            try:
                print(f"\nAttempting download: {year}/{month_str}...", end="\r")
                response = requests.get(url, timeout=30)
                response.raise_for_status()

                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    csv_filename = z.namelist()[0]
                    with z.open(csv_filename) as f:
                        # --- COLUMN DIAGNOSIS ---
                        # Peek at headers to verify presence of requested Italian variables
                        file_header = pd.read_csv(f, sep=';', encoding='latin1', nrows=0).columns.tolist()
                        
                        missing_cols = [c for c in config['target_columns'] if c not in file_header]
                        
                        if missing_cols:
                            print(f"\n[COLUMN ERROR] {year}/{month_str} skipped.")
                            print(f"Missing original variables: {missing_cols}")
                            continue 

                        # Reset stream and process in chunks for memory efficiency
                        f.seek(0) 
                        reader = pd.read_csv(
                            f, 
                            sep=';', 
                            encoding='latin1', 
                            usecols=config['target_columns'], 
                            chunksize=40000
                        )

                        for chunk in reader:
                            # Apply filters based on original Italian values
                            mask = True
                            for col, val in config['value_filters'].items():
                                mask &= (chunk[col].astype(str).str.upper() == str(val).upper())
                            
                            filtered_df = chunk[mask].copy()

                            if not filtered_df.empty:
                                # Standardize Italian currency format (1.234,56 -> 1234.56)
                                if config['clean_amounts'] and config['amount_column'] in filtered_df.columns:
                                    col_name = config['amount_column']
                                    filtered_df[col_name] = pd.to_numeric(
                                        filtered_df[col_name].astype(str)
                                        .str.replace('.', '', regex=False)
                                        .str.replace(',', '.', regex=False), 
                                        errors='coerce'
                                    )

                                # Append to the final CSV
                                filtered_df.to_csv(output_path, mode='a', index=False, header=write_header, sep=',')
                                write_header = False
                                print(f"Saved {len(filtered_df)} rows for {year}/{month_str}!")

            except Exception as e:
                # Silently skip missing months (often current month or future dates)
                continue

    print(f"\n--- Process complete. Data saved in: {os.path.abspath(output_path)} ---")

# ============================================================
# CIG CONFIGURATION
# ============================================================

user_configuration = {
    'start_year': 2020,                                                    # < --- CHOOSE START YEAR
    'end_year': 2023,                                                      # < --- CHOOSE END YEAR
    # target_columns uses the official Italian field names from ANAC
    'target_columns': ['cig', 
                       'tipo_scelta_contraente', 
                       'importo_lotto'],                                   # < --- CHOOSE COLUMNS TO KEEP (strongly suggest to keep month, year, cig)
    # value_filters targets specific Italian categorical strings
    'value_filters': {'tipo_scelta_contraente': 'PROCEDURA APERTA'},       # < --- CHOOSE FILTERS,  Leave empty to get all procedures 
    'output_filename': 'anac_extraction_2020_2023.csv',                    # < --- SAVE AND RENAME FILE
    'clean_amounts': True,
    'amount_column': 'importo_lotto'
}

if __name__ == "__main__":
    run_anac_extraction(user_configuration)
