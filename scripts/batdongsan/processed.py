import pandas as pd
from src.load.loaders import load_to_delta
from src.db_connection.reader import read_delta_table
from src.transform.batdongsan.processed import transform_real_estate_df
from src.quality.data_monitor import monitor_processed
from utils.telegram import send_message
site = "batdongsan.com.vn"
previous_layer = "parsed"
layer = "processed"

def process():
    parsed_data = read_delta_table(site, previous_layer)
    print(parsed_data)
    df = transform_real_estate_df(parsed_data)
    load_to_delta(df, site, layer, bucket='data-lake')

    processed_data = read_delta_table(site, layer)
    msg = monitor_processed(processed_data, site_name=site)
    send_message(msg)

