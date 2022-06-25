# -pN0sBgUNTgwcwDMNTboUV22Eaa3MsLVfLn7NZ1LY0AmAQP0ZNQBygec1Gz016mShof-Pdt5

# rtIEtL4cawD-FJ4o0ig1sYGQ5YnqgckcYCAOU9T2ZewGzG38CbbpNcc3EidKID6WrAxEH1D6HI2S-LQESYgxjw


import requests
from requests.structures import CaseInsensitiveDict
import json
import asyncio
import aiohttp

# crawls:
['4658127', '4579209']

# url = "https://api.deepcrawl.com/accounts/22841/projects/325380"

# url = "https://api.deepcrawl.com/accounts/22841/projects/325380/crawls"
# url = "https://api.deepcrawl.com/accounts/22841/projects/325380/crawls/4579206/reports/non_indexable_pages_basic"
# url = "https://api.deepcrawl.com/accounts/22841/projects/325380/crawls/4579206/reports/4579206:non_indexable_pages:basic"

# url = "https://api.deepcrawl.com/accounts/22841/projects/391739/crawls/4658127/changes"
url = 'https://api.deepcrawl.com/accounts/22841/projects/325380/crawls/4579206/reports/4579206:non_301_redirects:basic/report_rows'
# url = 'https://api.deepcrawl.com/accounts/22841/projects/391739/crawls/4658127/reports/4658127/report_rows'
headers = CaseInsensitiveDict()
headers["X-Auth-Token"] = "uYzwMgBjQtw3f3Hq_dmAfTSYZoXKNaLmVy-jC6jMhhwWpSJ3KuIqJIFJIFPO-MKpYLcV23SpEcajql4ZfA0ncA"

account_id = 22841
project_id = 325380
crawl_id = 4579206
report_id = '4579206:non_301_redirects:basic'
# resp = requests.get(url, headers=headers)


# print(resp.status_code)
counter = 0
send_req = True
per_page = 100
page_number = 0
params = {
    'per_page': per_page,
    'page': page_number
}

BASE_URL = 'https://api.deepcrawl.com'




async def do_async_request(BASE_URL,headers, report_id,  account_id, project_id, crawl_id, per_page=100):
    page_number = 0
    send_req = True
    count = 0
    status = 0
    delay_per_request = 0.1
    path = f'/accounts/{account_id}/projects/{project_id}/crawls/{crawl_id}/reports/{report_id}/report_rows'
    params = {
        'per_page': per_page,
        'page': page_number
    }

    while send_req:
        count_invalid_responses = 0
        status = 0

        async with aiohttp.ClientSession(headers= headers) as session:
            # print(f'PAGE NUMBER 1 {page_number}')
            response = session.get(BASE_URL+path, params=params)
            resp = await response.json()
            print(resp)

        break

asyncio.run(do_async_request(BASE_URL,headers, report_id, account_id, project_id, crawl_id))
# if not resp:
#     print('not resp')
#     print(resp)
#     exit(0)

# json_object = json.dumps(resp.json(), indent=4)

# print(json_object)
# with open("projectreportrows4.json", "w") as outfile:
#     outfile.write(json_object)
# item_count = 0
# session = requests.Session()
# while True:
#     resp = session.get(url, headers=headers, params={'page':counter, 'per_page':100})
#     json_object = resp.json()
#
#     if not resp:
#         break
#     print('writing')
#
#     item_count = item_count + len(json_object)
#
#     print(item_count)
#     # with open("projectnonindexreports.json", "a") as outfile:
#     #     outfile.write(json_object)
#
#
#     counter = counter + 1

# params = {
#
# }
#
# resp = requests.get(url, params=params, headers=headers)
#
#
# print(resp.status_code)
# json_object = json.dumps(resp.json(), indent = 4)
#
#
#
# with open("301redirectsbasicsrow.json", "w") as outfile:
#     outfile.write(json_object)
