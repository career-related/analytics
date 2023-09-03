"""
Website: https://jobs.careers.microsoft.com/global/en/search?l=en_us&pg=1&pgSz=20&o=Relevance&flt=true&utm_source=cms
"""

import ast
import asyncio
import json
import math
import time
from datetime import date

import aiohttp
import pandas as pd
import requests

COMPANY = "microsoft"
PAGE_SIZE = 20
BATCH_SIZE = 200

# dict key from response body - ['searchId', 'totalJobs', 'filters', 'jobs', 'id']


def scrape_single(page: int):
    """Scrape a single page of microsoft career website"""
    url = f"https://gcsservices.careers.microsoft.com/search/api/v1/search?l=en_us&pg={page}&pgSz={PAGE_SIZE}&o=Relevance&flt=true"
    resp = requests.get(url)
    resp_json = resp.json()

    # result that we are interested at is in operationalResult and result
    key_dict = resp_json["operationResult"]["result"]
    return key_dict

async def scrape_single_async(session, page: int):
    """Scrape a single page of microsoft career website"""
    url = f"https://gcsservices.careers.microsoft.com/search/api/v1/search?l=en_us&pg={page}&pgSz={PAGE_SIZE}&o=Relevance&flt=true"
    async with session.get(url) as resp:
        resp_json = await resp.json()

    # result that we are interested at is in operationalResult and result
    key_dict = resp_json["operationResult"]["result"]
    return key_dict

def get_total_record():
    """Get the total number of pages to scrape based on total jobs and page size"""
    key_dict = scrape_single(1)
    print(f"Total jobs: {key_dict['totalJobs']}")
    return key_dict["totalJobs"] 

def get_filter():
    """Get all available filters"""
    key_dict = scrape_single(1)
    return key_dict["filters"]

async def get_all_pages():
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
    url = f"https://gcsservices.careers.microsoft.com/search/api/v1/job/{job_id}?lang=en_us"
    async with session.get(url) as resp:
        try:
            resp_json = await resp.json()
            return resp_json["operationResult"]["result"]
        except aiohttp.client_exceptions.ContentTypeError:
            return {}

async def scrape_multiple_by_id(job_ids):
    """Scrape more job details of multiple jobs using job ids"""
    tasks = []
    async with aiohttp.ClientSession() as session:
        for job_id in job_ids:
            task = scrape_single_by_id(session, job_id)
            tasks.append(task)
        jobs = await asyncio.gather(*tasks)
    return jobs


async def batch_scrape_by_id(job_ids, batch_size=BATCH_SIZE):
    """Running in batches to avoid running into error 403"""
    ls_df = []
    batches = [job_ids[i:i + batch_size] for i in range(0, len(job_ids), BATCH_SIZE)]
    for idx, batch in enumerate(batches):
        print(idx)
        df_job = await scrape_multiple_by_id(batch)
        df_job = [job for job in df_job if job != {}]
        df_job = pd.DataFrame(df_job)
        ls_df.append(df_job)
        time.sleep(5 * idx)
    return pd.concat(ls_df)


if __name__ == "__main__":
    # save various filters
    with open(f"data/{COMPANY}-filter.json", "w") as file:
        json.dump(get_filter(), file, indent=4, sort_keys=True)
    
    # run 1
    df = pd.DataFrame(asyncio.run(get_all_pages()))
    df.to_csv(f"data/{COMPANY}-{date.today()}-run1.csv", index=False, encoding="utf-8-sig")

    # run 2
    job_ids = df["jobId"]
    # df_job = pd.DataFrame(asyncio.run(scrape_multiple_by_id(job_ids)))
    df_job = asyncio.run(batch_scrape_by_id(job_ids))
    print("Final number of jobs: ", df_job.shape)
    df_job.to_csv(f"data/{COMPANY}-{date.today()}-run2.csv", index=False, encoding="utf-8-sig")

# fields of jobs - jobId, title, postingDate, properties (description, locations, primaryLocation, workSiteFlexibility, profession, discipline, jobType, roleType, employmentType, educationLevel)
