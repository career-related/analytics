"""
Website: https://www.metacareers.com/jobs
"""

import asyncio
from datetime import date

import aiohttp
import pandas as pd
import requests
from bs4 import BeautifulSoup

COMPANY = "meta"

# fields - id, title, locations, teams, sub_teams


def scrape_all():
    url = "https://www.metacareers.com/graphql"

    payload = "av=170756762778504&__user=0&__a=1&__req=2&__hs=19653.BP%3ADEFAULT.2.0..0.0&dpr=1&__ccg=EXCELLENT&__rev=1009410666&__s=i6f2ns%3Admoynm%3Apox7s6&__hsi=7293178564309851056&__dyn=7xeUmwkHgmwn8K2WnFwn84a2i5U4e1Fx-ewSwMxW4E5S2WdwJw5ux60Vo1upE4W0OE2WxO2O1Vwooa85ufw5Zx61vw4iwBgao881FU2IzXw9S5ryE3bwkE5G0zE5W0HUvzo17U6i68iwfe0Lo6-1FwbO0NE24xG0PE&__csr=&fb_dtsg=NAcNn1SBMZusGftSHn4feEuDV27hVm8I7wOaWvRMeTvZ2uqv9HS95Vg%3A12%3A1696561357&jazoest=25469&lsd=_vlCdw8SblgGjzztJySetV&__spin_r=1009410666&__spin_b=trunk&__spin_t=1698075459&__jssesw=1&fb_api_caller_class=RelayModern&fb_api_req_friendly_name=CareersJobSearchResultsQuery&variables=%7B%22search_input%22%3A%7B%22q%22%3A%22%22%2C%22divisions%22%3A%5B%5D%2C%22offices%22%3A%5B%5D%2C%22roles%22%3A%5B%5D%2C%22leadership_levels%22%3A%5B%5D%2C%22saved_jobs%22%3A%5B%5D%2C%22saved_searches%22%3A%5B%5D%2C%22sub_teams%22%3A%5B%5D%2C%22teams%22%3A%5B%5D%2C%22is_leadership%22%3Afalse%2C%22is_remote_only%22%3Afalse%2C%22sort_by_new%22%3Afalse%2C%22page%22%3A1%2C%22results_per_page%22%3Anull%7D%7D&server_timestamps=true&doc_id=9114524511922157"
    headers = {
        "authority": "www.metacareers.com",
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/x-www-form-urlencoded",
        "cookie": "datr=Po9WZFZGr-9NqKIniImYc70V; cp_sess=FpDugYOt000WGBgOTnA4czdrdVdmUjctcEEWmuP70QwA",
        "dnt": "1",
        "origin": "https://www.metacareers.com",
        "referer": "https://www.metacareers.com/jobs",
        "sec-ch-ua": '"Chromium";v="118", "Microsoft Edge";v="118", "Not=A?Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36 Edg/118.0.2088.57",
        "x-asbd-id": "129477",
        "x-fb-friendly-name": "CareersJobSearchResultsQuery",
        "x-fb-lsd": "_vlCdw8SblgGjzztJySetV",
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()["data"]["job_search"]


async def parse_html(html):
    soup = BeautifulSoup(html, "html.parser")
    result_dict = {}
    result_dict["Title"] = soup.find(class_="_9ata").text
    result_dict["Location"] = soup.find(class_="_6hy-").text
    result_dict["Team_description"] = soup.find(class_="_1n-_ _6hy- _94t2").text
    div_element = soup.findAll(class_="_h46 _8lfy _8lfy")
    result_dict["Responsibilites"] = [
        li.get_text() for li in div_element[0].find_all("li")
    ]
    result_dict["Minimum_qualifications"] = [
        li.get_text() for li in div_element[1].find_all("li")
    ]
    result_dict["Preferred_qualifications"] = (
        [li.get_text() for li in div_element[2].find_all("li")]
        if len(div_element) > 2
        else []
    )
    return result_dict


async def scrape_single_by_id(session, job_id):
    """Scrape more job details using job id"""
    url = f"https://www.metacareers.com/jobs/{job_id}/"
    async with session.get(url) as resp:
        try:
            html = await resp.text()
            resp = await parse_html(html)
            return resp
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


if __name__ == "__main__":
    # run 1
    df = pd.DataFrame(scrape_all())
    df.to_csv(
        f"data/{COMPANY}-{date.today()}-run1.csv", index=False, encoding="utf-8-sig"
    )

    # run 2
    job_ids = df["id"]
    jobs = asyncio.run(scrape_multiple_by_id(job_ids))
    df_job = pd.DataFrame(jobs)
    df_job.to_csv(
        f"data/{COMPANY}-{date.today()}-run2.csv", index=False, encoding="utf-8-sig"
    )
