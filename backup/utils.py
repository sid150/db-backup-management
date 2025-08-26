import json
import logging

logging.basicConfig(level=logging.INFO)

def get_db_config(db_type):
    try:
        with open('dbconfig.json', 'r') as f:
            config = json.load(f)

        if db_type not in config:
            raise ValueError(f"Database type {db_type} not supported")
        
        logging.info(f"Database config for {db_type} fetched successfully.")
        return {f"{db_type}.url": config[db_type]['url'] + "://" + config[db_type]['username'] + ":" + config[db_type]['password'] + "@" + config[db_type]['host'] + "/" + config[db_type]['database']}
        # return config[db_type]
    
    except Exception as e:
        logging.error(e)
        return
