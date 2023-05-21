"""
Website: https://www.amazon.jobs/en/search?base_query=&loc_query=
"""

import asyncio
import json
from datetime import date

import aiohttp
import pandas as pd
import requests

# dict key from response body - ['error', 'hits', 'facets', 'content', 'jobs']

COMPANY = "amazon"
PAGE_SIZE = 10


def scrape_single(page: int):
    """Scrape a single page of microsoft career website"""
    url = f"https://www.amazon.jobs/en/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={page*PAGE_SIZE}&result_limit={PAGE_SIZE}&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&"
    resp = requests.get(url)
    resp_json = resp.json()
    return resp_json

async def scrape_single_async(session, page: int):
    """Scrape a single page of microsoft career website"""
    url = f"https://www.amazon.jobs/en/search.json?radius=24km&facets%5B%5D=normalized_country_code&facets%5B%5D=normalized_state_name&facets%5B%5D=normalized_city_name&facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location&facets%5B%5D=job_function_id&facets%5B%5D=is_manager&facets%5B%5D=is_intern&offset={page*PAGE_SIZE}&result_limit={PAGE_SIZE}&sort=relevant&latitude=&longitude=&loc_group_id=&loc_query=&base_query=&city=&country=&region=&county=&query_options=&"
    async with session.get(url) as resp:
        resp_json = await resp.json()
        return resp_json

def get_total_page():
    """Get the total number of pages to scrape based on total jobs and page size"""
    key_dict = scrape_single(1)
    print(f"Total jobs: {key_dict['hits']}")
    return key_dict["hits"] // PAGE_SIZE + 1 

def get_filter():
    """Get all available filters"""
    key_dict = scrape_single(1)
    return key_dict["facets"]

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
    with open(f"data/{COMPANY}-filter-{date.today()}.json", "w") as file:
        json.dump(get_filter(), file, indent=4, sort_keys=True)
    
    # save the jobs description
    result = asyncio.run(get_all_page())
    pd.DataFrame(result).to_csv(f"data/{COMPANY}-{date.today()}.csv", index=False, encoding="utf-8-sig")
