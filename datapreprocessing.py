import numpy as np
import pandas as pd
import zipfile
import json

zip_path = 'all_json.zip'
dfs = []

required_columns = ['runs', 'overs', 'bowler_type', 'wicket_type']

with zipfile.ZipFile(zip_path, 'r') as z:
    file_list = z.namelist()
    for file_name in file_list:
        if file_name.endswith('.json'):
            try:
                with z.open(file_name) as f:
                    data = json.load(f)
                    # If data is a dict, try to get the main list
                    if isinstance(data, dict):
                        for v in data.values():
                            if isinstance(v, list):
                                data = v
                                break
                    df = pd.DataFrame(data)
                    df = df.drop(['extras', 'non-strike'], axis=1, errors='ignore')
                    # Only process if all required columns are present
                    if all(col in df.columns for col in required_columns):
                        df = df.dropna()
                        df['runs'] = df['runs'].astype(int)
                        df['overs'] = df['overs'].astype(int)
                        df['bowler_type'] = df['bowler_type'].astype('category').cat.codes
                        df['is_boundary'] = df['runs'].apply(lambda x: 1 if x >= 4 else 0)
                        df['is_wicket'] = df['wicket_type'].notnull().astype(int)
                        dfs.append(df)
                    else:
                        print(f"Skipping {file_name}: missing required columns.")
            except Exception as e:
                print(f"Error processing {file_name}: {e}")

if dfs:
    final_df = pd.concat(dfs, ignore_index=True)
    final_df.to_json('preprocessed_data.json', orient='records', lines=True)
    print("Preprocessing complete. Output saved to preprocessed_data.json.")
else:
    print("No valid JSON files found in the zip")