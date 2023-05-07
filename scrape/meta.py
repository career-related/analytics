"""
Website: https://www.metacareers.com/jobs
"""

import pandas as pd
import requests

url = "https://www.metacareers.com/graphql"
headers = {
    "content-type": "application/x-www-form-urlencoded"
}
data = {
    "access_token": 'null',
    "__user": 0,
    "__a": 1,
    "__req": 2,
    "__hs": "19484.BP:DEFAULT.2.0..0.0",
    "dpr": 1,
    "__ccg": "EXCELLENT",
    "__rev": 1007448871,
    "__s": "t13xjt:lv7qb4:e1cmo8",
    "__hsi": 7230300520636843925,
    "__dyn": "7xeUmwkHgmwn8K2WnFwn84a2i5U4e1Fx-ewSAwHxW4E5S2WdwJw5ux60Vo1upE4W0OE2WxO0FE662y1nzU1vohwnU6a0HU9k2C220mC3S0H8-0EolKawcK1iwmE2ewnE2Lx-dw4vwJwSxy4E3PwbS1bwzwqo2YwMw9m0x8",
    "__csr": 'null',
    "lsd": "AVo30Kggm6Q",
    "jazoest": 2886,
    "__spin_r": 1007448871,
    "__spin_b": "trunk",
    "__spin_t": 1683435524,
    "__jssesw": 1,
    "fb_api_caller_class": "RelayModern",
    "fb_api_req_friendly_name": "CareersJobSearchResultsQuery",
    "variables": {"search_input":{"q":"","divisions":[],"offices":[],"roles":[],"leadership_levels":[],"saved_jobs":[],"saved_searches":[],"sub_teams":[],"teams":[],"is_leadership":False,"is_remote_only":False,"sort_by_new":False,"page":1,"results_per_page":"null"}},
    "server_timestamps": True,
    "doc_id": 9114524511922157
}


resp = requests.post(url, headers=headers, data=data)

print(resp)
print(resp.text)


