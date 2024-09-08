"""
Website: https://jobs.careers.microsoft.com/global/en/search?l=en_us&pg=1&pgSz=20&o=Relevance&flt=true&utm_source=cms
URL
1. Job cards, with limited details: https://gcsservices.careers.microsoft.com/search/api/v1/search?l=en_us&pg=1&pgSz=20&o=Relevance&flt=true
  - Response body fields: ['operationResult', 'errorInfo']
    - 'operationResult' fields: ['result', 'status', 'quality', 'errorCode']
      - 'result' fields: ['searchId', 'totalJobs', 'filters', 'jobs', 'id']
2. Detailed job description by ID: https://gcsservices.careers.microsoft.com/search/api/v1/job/1696314?lang=en_us
  - Response body fields: ['operationResult', 'errorInfo']
    - 'operationResult' fields: ['result', 'status', 'quality', 'errorCode']
      - 'result' fields: ['jobId', 'title', 'category', 'roleType', 'travelPercentage', 'posted', 'unposted', 
      'jobType', 'subcategory', 'employmentType', 'description', 'qualifications', 'responsibilities', 
      'primaryWorkLocation', 'workLocations', 'educationLevel', 'workSiteFlexibility', 'jobStatus', 'closedDate']

The code will take about 15 minutes to run as we are scraping by batch
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

COMPANY = "microsoft"
PAGE_SIZE = 20
BATCH_SIZE = 200
MICROSOFT_URL = "https://gcsservices.careers.microsoft.com/search/api/v1/search?l=en_us&pg={page}&pgSz={page_size}&o=Relevance&flt=true"
MICROSOFT_JOB_DETAIL_URL = (
    "https://gcsservices.careers.microsoft.com/search/api/v1/job/{job_id}?lang=en_us"
)

# dict key from response body - ['searchId', 'totalJobs', 'filters', 'jobs', 'id']


def scrape_single(page: int):
    """Scrape a single page of microsoft career website"""
    url = MICROSOFT_URL.format(page=page, page_size=PAGE_SIZE)
    with requests.get(url) as resp:
        resp_json = resp.json()

    # result that we are interested at is in operationalResult and result
    key_dict = resp_json["operationResult"]["result"]
    return key_dict


async def scrape_single_async(session, page: int):
    """Scrape a single page of microsoft career website"""
    url = MICROSOFT_URL.format(page=page, page_size=PAGE_SIZE)
    async with session.get(url) as resp:
        resp_json = await resp.json()

    # result that we are interested at is in operationalResult and result
    key_dict = resp_json["operationResult"]["result"]
    return key_dict


def get_total_record():
    """Get the total number of jobs"""
    key_dict = scrape_single(1)
    total = key_dict["totalJobs"]
    print(f"Total jobs: {total}")
    return total


def get_filter():
    """Get all available filters"""
    key_dict = scrape_single(1)
    return key_dict["filters"]


async def get_all_pages_async():
    """Scrape all page and append to a dataframe"""
    total_record = get_total_record()
    total_page = math.ceil(total_record / PAGE_SIZE)
    tasks = []
    async with aiohttp.ClientSession() as session:
        for page in range(1, total_page + 1):
            task = scrape_single_async(session, page)
            tasks.append(task)
        jobs = await asyncio.gather(*tasks)
    result = [job["jobs"] for job in jobs]
    result = [job for single_page in result for job in single_page]
    return result


async def scrape_single_by_id(session, job_id):
    """Scrape more job details using job id"""
    url = MICROSOFT_JOB_DETAIL_URL.format(job_id=job_id)
    async with session.get(url) as resp:
        try:
            resp_json = await resp.json()
            return resp_json["operationResult"]["result"]
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


async def batch_scrape_by_id(job_ids, batch_size=BATCH_SIZE):
    """Running in batches to avoid running into error 403"""
    ls_df = []
    batches = [job_ids[i : i + batch_size] for i in range(0, len(job_ids), batch_size)]
    for idx, batch in enumerate(batches):
        if idx != 0:
            sleep_duration = 60  # 5 * random.randrange(4, 7)
            print(f"Sleep for {sleep_duration} seconds")
            time.sleep(sleep_duration)
        print(idx)
        print("Input length:", len(batch))
        jobs = await scrape_multiple_by_id(batch)
        jobs = [job for job in jobs if job != {} and job is not None]
        print("Output length:", len(jobs))
        df_job = pd.DataFrame(jobs)
        ls_df.append(df_job)
    return pd.concat(ls_df)


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)

    # save various filters
    with open(f"data/{COMPANY}-filter-{date.today()}.json", "w") as file:
        json.dump(get_filter(), file, indent=4, sort_keys=True)

    # run 1: scrape the job cards
    df = pd.DataFrame(asyncio.run(get_all_pages_async()))
    df.to_csv(
        f"data/{COMPANY}-{date.today()}-run1.csv", index=False, encoding="utf-8-sig"
    )

    # run 2: scrape each job by job ID
    job_ids = df["jobId"]
    # df_job = pd.DataFrame(asyncio.run(scrape_multiple_by_id(job_ids)))
    df_job = asyncio.run(batch_scrape_by_id(job_ids))
    print("Final number of jobs: ", df_job.shape)
    df_job.to_csv(
        f"data/{COMPANY}-{date.today()}-run2.csv", index=False, encoding="utf-8-sig"
    )

# fields of jobs - jobId, title, postingDate, properties (description, locations, primaryLocation, workSiteFlexibility, profession, discipline, jobType, roleType, employmentType, educationLevel)
