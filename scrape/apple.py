"""
Website: https://jobs.apple.com/en-us/search?sort=relevance
"""

import ast
import asyncio
import time
from datetime import date

import aiohttp
import pandas as pd
import requests
from bs4 import BeautifulSoup

COMPANY = "apple"
PAGE_SIZE = 20
BATCH_SIZE = 50


def scrape_single(page: int):
    """Scrape a single page of apple career website"""
    url = f"https://jobs.apple.com/en-us/search?sort=relevance&page={page}"
    resp = requests.get(url)
    resp_text = resp.text
    return resp_text


async def scrape_single_async(session, page: int):
    """Scrape a single page of apple career website"""
    url = f"https://jobs.apple.com/en-us/search?sort=relevance&page={page}"
    async with session.get(url) as resp:
        resp_text = await resp.text()
    return resp_text


def get_total_page():
    resp = scrape_single(1)
    soup = BeautifulSoup(resp, "html.parser")
    element_page = soup.find_all(class_="pageNumber")
    if len(element_page) >= 2:
        page_number = element_page[1].get_text()
        return int(page_number)


def get_content(resp):
    soup = BeautifulSoup(resp, "html.parser")
    try:
        element_script = soup.find_all("script")[2].get_text()
    except IndexError as e:
        print(resp)
        exit(0)
    search_result = element_script.split('"searchResults":')[1]
    search_result = search_result.split('"totalRecords"')[0].strip(" ,")
    search_result = search_result.replace("false", "False")
    search_result = search_result.replace("true", "True")
    search_result = search_result.replace("null", "None")
    try:
        res = ast.literal_eval(search_result)
    except:
        print(search_result)
        print(type(search_result))
        exit(0)
    return res


def get_all_pages():
    total_page = get_total_page()
    jobs = []
    for page in range(1, total_page + 1):
        resp = scrape_single(page)
        jobs.extend(get_content(resp))
    return jobs


async def get_all_pages_async():
    """Scrape all page an append to a dataframe"""
    conn = aiohttp.TCPConnector(limit=20)
    total_page = get_total_page()
    tasks = []
    async with aiohttp.ClientSession(connector=conn) as session:
        for page in range(1, total_page + 1):
            task = scrape_single_async(session, page)
            tasks.append(task)
        jobs = await asyncio.gather(*tasks)
    result = [get_content(job) for job in jobs]
    result = [job for single_page in result for job in single_page]
    return result


async def parse_html(html):
    """Parse HTML using beautifulsoup"""
    soup = BeautifulSoup(html, "html.parser")
    result_dict = {}
    result_dict["Title"] = soup.find(id="jd__header--title").text
    result_dict["Location"] = soup.find(id="job-location-name").text
    result_dict["Team"] = soup.find(id="job-team-name").text
    key_qualifications = soup.find(id="jd-key-qualifications")
    result_dict["Key_qualifications"] = [
        li.get_text() for li in key_qualifications.find_all("li")
    ]
    result_dict["Description"] = soup.find(id="jd-description").text()
    result_dict["Education_Experience"] = soup.find(id="jd-education-experience").text()
    return result_dict


async def scrape_single_by_id(session, job_id):
    """Scrape more job details using job id"""
    url = f"https://jobs.apple.com/en-us/details/{job_id}"
    async with session.get(url) as resp:
        try:
            html = await resp.text()
            resp = await parse_html(html)
            return resp
        except aiohttp.client_exceptions.ContentTypeError:
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
    """Running in batches to avoid running into error 429"""
    ls_df = []
    batches = [job_ids[i : i + batch_size] for i in range(0, len(job_ids), BATCH_SIZE)]
    for idx, batch in enumerate(batches):
        print(idx)
        df_job = await scrape_multiple_by_id(batch)
        df_job = [job for job in df_job if job != {}]
        df_job = pd.DataFrame(df_job)
        ls_df.append(df_job)
        time.sleep(5 * idx)
    return pd.concat(ls_df)


if __name__ == "__main__":
    # run 1
    start = time.time()
    # df = pd.DataFrame(get_all_pages())  # without sychronuous takes about 6 minutes to run
    df = pd.DataFrame(
        asyncio.run(get_all_pages_async())
    )  # with async takes about 40 seconds
    df.to_csv(
        f"data/{COMPANY}-{date.today()}-run1.csv", index=False, encoding="utf-8-sig"
    )
    print(f"Time taken: {time.time() - start}")

    # run 2
    job_ids = df["positionId"]
    # df_job = pd.DataFrame(asyncio.run(scrape_multiple_by_id(job_ids)))
    df_job = asyncio.run(batch_scrape_by_id(job_ids))
    df_job.to_csv(
        f"data/{COMPANY}-{date.today()}-run2.csv", index=False, encoding="utf-8-sig"
    )
