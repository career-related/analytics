"""
Website: https://jobs.apple.com/en-us/search?sort=relevance
URL: https://jobs.apple.com/api/role/search
Response body fields: ['searchResults', 'totalRecords']
  - 'searchResults' is the list of jobs
"""

import asyncio
import json
import math
import os
import time
from datetime import date

import aiohttp
import pandas as pd
import requests

COMPANY = "apple"
PAGE_SIZE = 20
APPLE_URL = "https://jobs.apple.com/api/role/search"
BATCH_SIZE_1 = 15
APPLE_CSRF_URL = "https://jobs.apple.com/api/csrfToken"
APPLE_JOB_DETAIL_URL = (
    "https://jobs.apple.com/api/role/detail/{job_id}?languageCd=en-us"
)


def get_headers():
    with requests.get(APPLE_CSRF_URL) as resp:
        headers = {
            "cookie": "; ".join(
                [cookie.name + "=" + cookie.value for cookie in resp.cookies]
            ),
            "X-Apple-CSRF-Token": resp.headers["X-Apple-CSRF-Token"],
            "host": "jobs.apple.com",
            "Accept": "*/*",
            "Content-Type": "application/json",
        }
    return headers


def scrape_single(page: int, headers):
    """Scrape a single page of apple career website"""
    url = APPLE_URL
    data = json.dumps(
        {
            "query": "",
            "filters": {"range": {"standardWeeklyHours": {"start": None, "end": None}}},
            "page": page,
            "locale": "en-us",
            "sort": "relevance",
        }
    )
    with requests.post(url, data=data, headers=headers) as resp:
        try:
            resp_json = resp.json()
            return resp_json
        except requests.exceptions.JSONDecodeError:
            print(f"Failed to scrape page {page}")
            return {}


async def scrape_single_async(session, page: int, headers):
    """Scrape a single page of apple career website"""
    url = APPLE_URL
    data = json.dumps(
        {
            "query": "",
            "filters": {"range": {"standardWeeklyHours": {"start": None, "end": None}}},
            "page": page,
            "locale": "en-us",
            "sort": "relevance" "page",
        }
    )
    async with session.post(url, data=data, headers=headers) as resp:
        try:
            resp_json = await resp.json()
            return resp_json
        except aiohttp.client_exceptions.ContentTypeError:
            print(f"Failed to scrape page {page}")
            return {}


async def scrape_multiple_async(pages, headers):
    """Scrape more job details of multiple jobs using job ids"""
    conn = aiohttp.TCPConnector(limit=20)
    tasks = []
    async with aiohttp.ClientSession(connector=conn) as session:
        for page in pages:
            task = scrape_single_async(session, page, headers)
            tasks.append(task)
        jobs = await asyncio.gather(*tasks)
        return jobs


def get_total_records():
    headers = get_headers()
    resp = scrape_single(1, headers)
    total = resp["totalRecords"]
    print(f"Total jobs: {total}")
    return total


def get_all_pages():
    """Scrape all page and append to a dataframe"""
    total_record = get_total_records()
    total_page = math.ceil(total_record / PAGE_SIZE)
    headers = get_headers()
    jobs = []
    for page in range(1, total_page + 1):
        res_json = scrape_single(page, headers)
        if "searchResults" in res_json:
            jobs.extend(res_json["searchResults"])
    return jobs


async def get_all_pages_async():
    """Scrape all page and append to a dataframe"""
    total_record = get_total_records()
    total_page = math.ceil(total_record / PAGE_SIZE)
    headers = get_headers()

    ls_df = []
    batches = [range(i, i + BATCH_SIZE_1) for i in range(0, total_page, BATCH_SIZE_1)]
    for idx, batch in enumerate(batches):
        if idx != 0:
            sleep_duration = 60  # 5 * random.randrange(4, 7)
            print(f"Sleep for {sleep_duration} seconds")
            time.sleep(sleep_duration)
        print(idx)
        print("Input length:", len(batch))
        jobs = await scrape_multiple_async(batch, headers)

        jobs = [job["searchResults"] for job in jobs if job != {}]
        print("Output length:", len(jobs))
        df_job = pd.DataFrame(jobs)
        ls_df.append(df_job)
    return pd.concat(ls_df)

    # tasks = []
    # async with aiohttp.ClientSession() as session:
    #     for page in range(1, total_page + 1):
    #         task = scrape_single_async(session, page, headers)
    #         tasks.append(task)
    #     jobs = await asyncio.gather(*tasks)
    # result = [job["searchResults"] for job in jobs if job != {}]
    # result = [job for single_page in result for job in single_page]
    # return result


async def scrape_single_by_id(session, job_id):
    """Scrape more job details using job id"""
    url = APPLE_JOB_DETAIL_URL.format(job_id=job_id)
    async with session.get(url) as resp:
        try:
            resp_json = await resp.json()
            return resp_json
        except aiohttp.client_exceptions.ContentTypeError:
            print(f"Failed to scrape {job_id}")
            return {}


async def scrape_multiple_by_id(job_ids):
    """Scrape more job details of multiple jobs using job ids"""
    conn = aiohttp.TCPConnector(limit=20)
    tasks = []
    async with aiohttp.ClientSession(connector=conn) as session:
        for job_id in job_ids:
            task = scrape_single_by_id(session, job_id)
            tasks.append(task)
        jobs = await asyncio.gather(*tasks)
        return jobs


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)

    # run 1: scrape the job cards
    df = asyncio.run(get_all_pages_async())
    # df = pd.DataFrame(get_all_pages())
    df.to_csv(
        f"data/{COMPANY}-{date.today()}-run1.csv", index=False, encoding="utf-8-sig"
    )

    # # run 2: scrape each job by job ID
    # job_ids = df["id"]
    # # df_job = pd.DataFrame(asyncio.run(scrape_multiple_by_id(job_ids)))
    # df_job = pd.DataFrame(asyncio.run(scrape_multiple_by_id(job_ids)))
    # df_job.to_csv(
    #     f"data/{COMPANY}-{date.today()}-run2.csv", index=False, encoding="utf-8-sig"
    # )
