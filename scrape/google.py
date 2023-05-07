"""
Website: https://careers.google.com/jobs/results/?distance=50&hl=en_US&jlo=en_US&q=
"""

import asyncio
import json
from datetime import date

import aiohttp
import pandas as pd
import requests

COMPANY = "google"
PAGE_SIZE = 20

url = f"https://careers.google.com/api/v3/search/?distance=50&hl=en_US&jlo=en_US&page=1&q="


def scrape_single(page: int):
    """Scrape a single page of microsoft career website"""
    url = f"https://careers.google.com/api/v3/search/?distance=50&hl=en_US&jlo=en_US&page={page}&q="
    resp = requests.get(url)
    resp_json = resp.json()
    return resp_json

async def scrape_single_async(session, page: int):
    """Scrape a single page of microsoft career website"""
    url = f"https://careers.google.com/api/v3/search/?distance=50&hl=en_US&jlo=en_US&page={page}&q="
    async with session.get(url) as resp:
        resp_json = await resp.json()
        return resp_json

def get_total_page():
    """Get the total number of pages to scrape based on total jobs and page size"""
    key_dict = scrape_single(1)
    print(f"Total jobs: {key_dict['count']}")
    return key_dict["count"] // PAGE_SIZE + 1 

# def get_filter():
#     """Get all available filters"""
#     key_dict = scrape_single(1)
#     return key_dict["facets"]

# def get_all_page():
#     """Scrape all page and append to a dataframe"""
#     total_page = get_total_page()
#     jobs = []
#     for page in range(1, 3):
#         res_json = scrape_single(page)
#         jobs.extend(res_json["jobs"])
#     return jobs

async def get_all_page():
    """Scrape all page and append to a dataframe"""
    total_page = get_total_page()
    tasks = []
    async with aiohttp.ClientSession() as session:
        for page in range(1, total_page + 1):
            task = scrape_single_async(session, page)
            tasks.append(task)
        result = await asyncio.gather(*tasks)
    result = [item for sublist in result for item in sublist["jobs"]]
    return result


if __name__ == "__main__":
    # save various filters
    # with open(f"data/{COMPANY}-filter.json", "w") as file:
    #     json.dump(get_filter(), file, indent=4, sort_keys=True)
    
    # save the jobs description
    result = asyncio.run(get_all_page())
    pd.DataFrame(result).to_csv(f"data/{COMPANY}-{date.today()}.csv", index=False)
