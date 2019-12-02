import apihelper
import os
import io
import requests
import boto3
import json
import pandas as pd
import sys
import logging
import logging.handlers
from datetime import datetime, timedelta


def config_logging():
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.handlers.TimedRotatingFileHandler(
                "script.log", when="midnight", interval=1, backupCount=3
            ),
            logging.StreamHandler(sys.stdout),
        ],
    )

    logging.getLogger().setLevel(logging.DEBUG)

    logging.getLogger("botocore").setLevel(logging.ERROR)
    logging.getLogger("boto3").setLevel(logging.ERROR)

def post_in_slack(payload):

    if end_point == "kinduct.com":
        webhook_url = "https://hooks.slack.com/services/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"
    else:
        webhook_url = "https://hooks.slack.com/services/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"

    payload = "ACADIA POSITIONAL AEVERAGES ERROR: " + payload
    slack_data = {"text": payload}

    response = requests.post(
        webhook_url,
        data=json.dumps(slack_data),
        headers={"Content-Type": "application/json"},
    )
    return response


def get_credentials(bucket, clientname):
    key = f"rscript-files/{clientname}/{clientname}_admin.csv"
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(obj["Body"].read()))
    return df


if __name__ == "__main__":

    try :

        config_logging()

        logging.info("Script started")

        # get env variables
        manual_date = os.getenv("MANUAL_DATE")
        secure_bucket = os.getenv("FILE_LOCATION")
        end_point = os.getenv("END_POINT")
        clientname = "marquette"
        org_uids = ["rn3ckhm4khcnr9bm","hruuvspmxob5m4dt","oz5x18hqb9j0r4uk", \
            "mt2q15z71do6oxbn","mhtctw46ao4og1xv"," qjt9ugs28s5aaz72", \
            "rizs73qym6112lby","hjtszet5b5mzmu32","azqyzaey0y1oev45", \
            "wcv4m2b8ed8nh41a","iimgm6eqmtz0ky2n","vdh7dvj27xxoe3lh", \
            "9hhvai3eql7m7v1x","xo7gy3qym8bnawdg","r0t0bhzxfv1qnwzo", \
            "2wcvzu7gm8gfnk8b","4rralvwt0ys5rskd"]

        # metrics required
        metrics = ["Max VJ","Max RSI"]

        # get credentials
        creds = get_credentials(secure_bucket, clientname)
        client_id = creds["client_id"].iloc[0]
        client_secret = creds["client_secret"].iloc[0]

        # get date =======================================
        if manual_date is None:
            manual_date = datetime.strftime(datetime.now() - timedelta(1), "%Y-%m-%d")
            start_date = manual_date
            end_date = datetime.today().strftime("%Y-%m-%d")   
        else:
            if "," in manual_date:
                manual_date = manual_date.split(",")
                start_date = manual_date[0]
                end_date = manual_date[1]
            else:
                start_date = manual_date
                end_date = datetime.today().strftime("%Y-%m-%d")
        logging.info(f"Script executing for all days between {start_date}  and {end_date}")
        
          # export data from kinduct
        data = apihelper.export_data_from_kinduct(
            endpoint=end_point,
            clientname=clientname,
            client_id=client_id,
            client_secret=client_secret,
            start_date=start_date,
            end_date=end_date,
            organizations = org_uids,
            metrics=metrics
        )


        # =========== script calculation START===========================

        data = data[['player_name', 'date', 'Max VJ', 'Max RSI']]

        data_max_vj = data.groupby(['player_name'], as_index=False)['Max VJ'].max()
        data_max_vj = data_max_vj.rename(columns={"Max VJ" : "Max VJ This Season"})

        data_max_rsi = data.groupby(['player_name'], as_index=False)['Max RSI'].max()
        data_max_rsi = data_max_rsi.rename(columns={"Max RSI" : "Max RSI This Season"})

        data = data.merge(data_max_vj, left_on='player_name', right_on='player_name')
        data = data.merge(data_max_rsi, left_on='player_name', right_on='player_name')

        data["% Max VJ"] = (data["Max VJ"]/data["Max VJ This Season"])*100
        data["% Max RSI"] = (data["Max RSI"]/data["Max RSI This Season"])*100
        data["source"] = "calculation"
        data = data.round(2)
        data_final = data[["player_name", "date", "source", "Max VJ This Season", "Max RSI This Season", "% Max VJ", "% Max RSI"]]

        # =========== script calculation  END===========================  

        # == Load data to Kinduct API ===================
        response = apihelper.import_data_to_kinduct(
            endpoint=end_point,
            clientname=clientname,
            client_id=client_id,
            client_secret=client_secret,
            data=data_final,
            source=None,
        )

        logging.info(response)
    except Exception as e:
        post_in_slack(str(e))
        logging.error(e)
    pass
