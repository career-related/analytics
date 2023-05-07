"""
Website: https://jobs.careers.microsoft.com/global/en/search?l=en_us&pg=1&pgSz=20&o=Relevance&flt=true&utm_source=cms
"""

import ast
import json
from datetime import date

import pandas as pd
import requests

COMPANY = "microsoft"
PAGE_SIZE = 20

# dict key from response body - ['searchId', 'totalJobs', 'filters', 'jobs', 'id']

def scrape_single(page: int):
    """Scrape a single page of microsoft career website"""
    url = f"https://gcsservices.careers.microsoft.com/search/api/v1/search?l=en_us&pg={page}&pgSz={PAGE_SIZE}&o=Relevance&flt=true"
    resp = requests.get(url)
    resp_json = resp.json()

    # result that we are interested at is in operationalResult and result
    key_dict = {}
    for key in resp_json["operationResult"]["result"].keys():
        key_dict[key] = resp_json["operationResult"]["result"][key]
    return key_dict

def get_total_page():
    """Get the total number of pages to scrape based on total jobs and page size"""
    key_dict = scrape_single(1)
    print(f"Total jobs: {key_dict['totalJobs']}")
    return key_dict["totalJobs"] // PAGE_SIZE + 1 

def get_filter():
    """Get all available filters"""
    key_dict = scrape_single(1)
    return key_dict["filters"]

def get_all_page():
    """Scrape all page and append to a dataframe"""
    total_page = get_total_page()
    jobs = []
    for page in range(1, total_page + 1):
        key_dict = scrape_single(page)
        jobs.extend(key_dict["jobs"])
    return jobs


if __name__ == "__main__":
    # save various filters
    with open(f"data/{COMPANY}-filter.json", "w") as file:
        json.dump(get_filter(), file, indent=4, sort_keys=True)
    
    # save the jobs description
    # df = pd.DataFrame(get_all_page())
    # df.to_csv(f"data/{COMPANY}-{date.today()}-unprocessed.csv", index=False)

    # process the json column in jobs description
    # df = pd.read_csv("data/microsoft-2023-05-07-unprocessed.csv")
    # df["properties"] = df["properties"].apply(lambda x: ast.literal_eval(x))
    # df = pd.concat([df.drop('properties', axis=1), pd.DataFrame(df['properties'].tolist())], axis=1)
    # df.to_csv(f"data/{COMPANY}-{date.today()}.csv", index=False)

# fields of jobs - jobId, title, postingDate, properties (description, locations, primaryLocation, workSiteFlexibility, profession, discipline, jobType, roleType, employmentType, educationLevel)
