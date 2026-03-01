import pandas as pd
from src.load.loaders import load_to_delta
from src.db_connection.reader import list_raw_files, read_raw_json_gz
from src.transform.batdongsan.parse import parse_minimal
from src.quality.data_monitor import monitor_parsed
from src.db_connection.reader import read_delta_table
from utils.telegram import send_message

site = "batdongsan.com.vn"
layer = "parsed"

def parse():
    file_list =  list_raw_files(site)
    for path in file_list:
        df = read_raw_json_gz(path)
        final_df = None
        if 'html' in df.columns:
            parsed_series = df['html'].apply(parse_minimal)
            parsed_df = pd.json_normalize(parsed_series)

            meta_cols = [c for c in ['correlation_id', 'url', 'status'] if c in df.columns]
            final_df = pd.concat([df[meta_cols], parsed_df], axis=1)
        else:
            final_df = df
        print(final_df)
        load_to_delta(final_df, site, layer, bucket='data-lake')

    parsed_data = read_delta_table(site, "parsed")
    msg = monitor_parsed(parsed_data, site_name=site)
    send_message(msg)

        

