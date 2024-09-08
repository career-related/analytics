"""
Website: https://jobs.netflix.com/
URL: https://explore.jobs.netflix.net/api/apply/v2/jobs?domain=netflix.com&domain=netflix.com&profile=&sort_by=relevance
Response body fields - ['record_count', 'records', 'info', 'errors']
"""

import asyncio
import json
import math
import os
from datetime import date

import aiohttp
import pandas as pd
import requests


COMPANY = "netflix"
PAGE_SIZE = 20
NETFLIX_URL = "https://jobs.netflix.com/api/search?page={page}"


def scrape_single(page: int):
    """Scrape a single page of microsoft career website"""
    url = NETFLIX_URL.format(page=page)
    with requests.get(url) as resp:
        resp_json = resp.json()
        return resp_json


async def scrape_single_async(session, page: int):
    """Scrape a single page of microsoft career website"""
    url = NETFLIX_URL.format(page=page)
    async with session.get(url) as resp:
        resp_json = await resp.json()
        return resp_json


def get_total_records():
    """Get the total number of jobs"""
    key_dict = scrape_single(1)
    total = key_dict["info"]["postings"]["total_result_count"]
    print(f"Total jobs: {total}")
    return total


async def get_all_pages_async():
    """Scrape all page and append to a dataframe"""
    total_record = get_total_records()
    total_page = math.ceil(total_record / PAGE_SIZE)
    tasks = []
    async with aiohttp.ClientSession() as session:
        for page in range(1, total_page + 1):
            task = scrape_single_async(session, page)
            tasks.append(task)
        result = await asyncio.gather(*tasks)
    result = [item for sublist in result for item in sublist["records"]["postings"]]
    return result


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)

    # save the jobs description
    result = asyncio.run(get_all_pages_async())
    pd.DataFrame(result).to_csv(
        f"data/{COMPANY}-{date.today()}.csv", index=False, encoding="utf-8-sig"
    )
