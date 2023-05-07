"""
Website: https://jobs.apple.com/en-us/search?sort=relevance
"""

import pandas as pd
import requests

COMPANY = "apple"
PAGE_SIZE = 20

body = {
  "query": "",
  "filters": {
    "range": {
      "standardWeeklyHours": {
        "start": "null",
        "end": "null"
      }
    }
  },
  "page": 1,
  "locale": "en-us",
  "sort": "relevance"
}

with requests.Session() as s:
    resp1 = s.get("https://jobs.apple.com/api/csrfToken")
    # print(dir(resp1))
    csrf_token = resp1.headers["X-Apple-CSRF-Token"]
    # cookies = resp1.cookies

    print(resp1.headers)
    # s.cookies = cookies

    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36 Edg/112.0.1722.68",
        "origin": "https://jobs.apple.com",
        "referer": "https://jobs.apple.com/en-us/search?sort=relevance&location=united-states-USA",
        "countrycode": "USA",
        "roveremaillocalecode": "en_US",
        # "cookie": resp1.headers["Set-Cookie"],
        "x-apple-csrf-token": csrf_token
    }
    url = "https://jobs.apple.com/api/role/search"
    resp = s.post(url, json=body, headers=headers)
    response_json = resp

    print(resp.status_code)
    print(resp.text)


