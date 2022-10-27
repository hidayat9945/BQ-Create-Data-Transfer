import json
from dotenv import load_dotenv
load_dotenv()

from helpers import create_dts_s3, is_file_exist
from logger import logger

def main():
    config_file = "./config.json"
    if is_file_exist(config_file):
        with open(config_file) as f:
            configs = json.load(f)
        for conf in configs:
            create_dts_s3(
                display_name=conf["display_name"],
                dest_dataset=conf["destination_dataset"],
                dest_table=conf["destination_table"],
                s3_uri=conf["s3_uri"]
            )
    else:
        logger.error("File config.json is not found. Please provide it.")

if __name__ == "__main__":
    main()