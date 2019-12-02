import json
import requests
import logging
import sys
import numpy as np
from math import isnan
import pandas as pd
import simplejson

from pandas.io.json import json_normalize

logger = logging.getLogger()
logger.setLevel("DEBUG")

"""
Function to import data from Kinduct API 

Input ::
  -> endpoint       | string  | Required |
  -> clientname     | string  | Required |
  -> client_id      | string  | Required |
  -> client_secret  | string  | Required |
  -> start_date     | string  | Required |
  -> end_date       | string  | Required |
  -> leagues        | List    | Optional |
  -> segments       | List    | Optional |
  -> organizations  | List    | Optional |
  -> metrics        | List    | Optional |

Output :: Returns the data in the for of a neatly formatted pandas dataframe
     

"""


def export_data_from_kinduct(
    endpoint,
    clientname,
    client_id,
    client_secret,
    start_date,
    end_date,
    leagues=None,
    segments=None,
    organizations=None,
    metrics=None,
):

    ###############################
    # get outh token   ------------
    ###############################
    logger.info("Getting Auth token ..")
    api_auth_endpoint = f"https://{clientname}.{endpoint}/api/oauth/token"
    grant_type = "client_credentials"
    json_payload = {
        "grant_type": grant_type,
        "client_secret": client_secret,
        "client_id": client_id,
    }
    try:
        auth_request_results = requests.post(api_auth_endpoint, data=json_payload)
    except requests.exceptions.RequestException as e:
        logger.error(e)
        sys.exit(1)
    token_type = auth_request_results.json()["token_type"]
    access_token = auth_request_results.json()["access_token"]

    # metrics to remove
    unwanted_metrics = [
        "First name",
        "first name",
        "Last name",
        "last name",
        "position",
    ]

    ###############################
    # Generate query  ------------
    ###############################
    logger.info("Generating query ..")
    # check if the start date and end dates are present
    if start_date is not None and end_date is not None:
        query = f"https://{clientname}.{endpoint}/api/export/export_json?start_date={start_date}&end_date={end_date}"

        # ------------ League Filter -----------
        if leagues is not None and type(leagues) is str:
            query = f"{query}&leagues={leagues}"
        elif leagues is not None and type(leagues) is list:
            seperator = "|"
            join_leagues = seperator.join(leagues)
            query = f"{query}&leagues={join_leagues}"

        # ------------- Segments Filter-----------
        if segments is not None and type(segments) is str:
            query = f"{query}&segments={segments}"
        elif segments is not None and type(segments) is list:
            seperator = "|"
            join_segments = seperator.join(segments)
            query = f"{query}&segments={join_segments}"

        # -------------- Organizations Filter -------
        if organizations is not None and type(organizations) is str:
            query = f"{query}&organizations={organizations}"
        elif organizations is not None and type(organizations) is list:
            seperator = "|"
            join_organizations = seperator.join(organizations)
            query = f"{query}&organizations={join_organizations}"

        ###############################
        # ----- get data from api-------
        ###############################
        authorization = f"{token_type} {access_token}"
        headers = {"Authorization": authorization, "Content-Type": "application/json"}
        if metrics is None:
            try:
                export_results = requests.post(query, headers=headers)
            except requests.exceptions.RequestException as e:
                logger.error(f"Request exception ..  {e}")
                sys.exit(1)

        elif type(metrics) is list:
            api_payload = json.dumps({"metrics": metrics})
            try:
                export_results = requests.post(query, headers=headers, data=api_payload)
            except requests.exceptions.RequestException as e:
                logger.error(f"Request exception ..  {e}")
                sys.exit(1)
        
        data_json = export_results.json()
        data_json = data_json["results"]
        
        # check if any records exit
        if len(data_json) > 0:
            data = pd.DataFrame.from_dict(data_json, orient="index")

            data["player_name"] = data["First Name"] + " " + data["Last Name"]

            data = data[
                [
                    "Date",
                    "Source",
                    "Segment",
                    "User UID",
                    "player_name",
                    "External UID",
                    "Organization",
                    "League",
                    "metrics",
                ]
            ]

            data = data.reset_index(drop=True)
            data = data.rename(
                columns={
                    "Date": "date",
                    "Source": "source",
                    "Segment": "segment",
                    "User UID": "user_uid",
                    "External UID": "external_uid",
                    "Organization": "organization",
                    "League": "league",
                    "metrics": "metrics",
                }
            )
            data["unique_index"] = data["date"] + data["user_uid"]
            data_metrics = pd.DataFrame(
                data.metrics.values.tolist(), index=data.unique_index
            ).stack()
            data_metrics = data_metrics.reset_index([0, "unique_index"])
            data_metrics.columns = ["unique_index", "metrics_new"]

            data_metrics = data.merge(data_metrics, how="left", on="unique_index")

            data_metrics = data_metrics[
                [
                    "date",
                    "source",
                    "segment",
                    "user_uid",
                    "external_uid",
                    "player_name",
                    "organization",
                    "league",
                    "metrics_new",
                    "unique_index",
                ]
            ]
            data_metrics_split = pd.DataFrame()
            data_metrics_split[["name", "value", "type"]] = pd.DataFrame(
                data_metrics["metrics_new"].values.tolist(), index=data_metrics.unique_index
            )
            data_metrics_split = data_metrics_split.reset_index([0, "unique_index"])
            data_metrics_split = data_metrics_split[
                ["unique_index", "name", "value", "type"]
            ]
            data_metrics_split = data_metrics_split.pivot_table(
                index="unique_index", columns="name", values="value", aggfunc="first"
            ).reset_index()

            data = data[
                [
                    "date",
                    "source",
                    "segment",
                    "user_uid",
                    "external_uid",
                    "player_name",
                    "organization",
                    "league",
                    "unique_index",
                ]
            ]

            # remove unwanted metrics before merging into main data frame
            for metric in unwanted_metrics:
                if metric in data_metrics_split.columns:
                    data_metrics_split = data_metrics_split.drop(metric, axis=1)

            data = data.merge(data_metrics_split, how="left", on="unique_index")

        else:
            logger.error("No data found !!!")
            exit(0)

    else:
        logger.error(
            "No date range defined. Date parameters cannot be NULL. Please define a start and end date."
        )

    return data


"""
Function to import data to Kinduct API 

Input ::
  -> endpoint       | string     | Required |
  -> clientname     | string     | Required |
  -> client_id      | string     | Required |
  -> client_secret  | string     | Required |
  -> data           | dataframe  | Required |

Data Format (Columns):
    -> source   |  source of the metrics
    -> date
    -> player_name     |   name of the athlete in "Fistname Lastname" format
    -> metrics...


Output :: Import success/failure message


"""


def reformat_data(data_row):
    data = {
        "type": data_row["source"],
        "date": data_row["date"],
        "name": data_row["player_name"],
    }
    del data_row["date"]
    del data_row["player_name"]
    del data_row["source"]
    for metric in data_row.copy():
        if np.isnan(data_row[metric]):
            data_row.pop(metric)
    data["data"] = data_row
    return data


def import_data_to_kinduct(
    endpoint, clientname, client_id, client_secret, data, source=None
):
    ###############################
    # get outh token   ------------
    ###############################
    logger.info("Getting Auth token ..")
    api_auth_endpoint = f"https://{clientname}.{endpoint}/api/oauth/token"
    grant_type = "client_credentials"
    json_payload = {
        "grant_type": grant_type,
        "client_secret": client_secret,
        "client_id": client_id,
    }
    try:
        auth_request_results = requests.post(api_auth_endpoint, data=json_payload)
    except requests.exceptions.RequestException as e:
        logger.error(e)
        sys.exit(1)
    token_type = auth_request_results.json()["token_type"]
    access_token = auth_request_results.json()["access_token"]

    # metrics to remove
    logger.info("Reformatting import data ..")
    data = data.dropna(how="all")
    data.dropna(axis=1, how="all")
    data = data.to_dict(orient="records")
    data_payload = []

    for row in data:
        data_row = reformat_data(data_row=row)
        data_payload.append(data_row)
    api_payload = json.dumps({"records": data_payload})
    
    api_import_endpoint = f"https://{clientname}.{endpoint}/api/import/import_data"
    authorization = f"{token_type} {access_token}"
    headers = {"Authorization": authorization, "Content-Type": "application/json"}
    logger.info(f"Import Endpoint ..  {api_import_endpoint}")

    try:
        import_results = requests.post(
            api_import_endpoint, headers=headers, data=api_payload
        )
    except requests.exceptions.RequestException as e:
        logger.error(f"Request exception ..  {e}")
    logger.info(f"Import results ..  {import_results.json()}")
    return import_results.json()
