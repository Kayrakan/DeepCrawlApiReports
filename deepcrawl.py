import asyncio

import aiohttp
import requests
from requests.structures import CaseInsensitiveDict
import json

import settings


class DeepCrawl():

    BASE_URL = "https://api.deepcrawl.com"

    def __init__(self, secret, session = None):
        self.secret = secret
        self.session = requests.Session() if session is None else session
        self.test_token_exist()

    def test_token_exist(self):

        print('tested token')
        if 'X-Auth-Token' in self.session.headers:
            print('tested')
            return True
        else:
            return self.authorize()


    def authorize(self):
        path = "/sessions"

        headers = CaseInsensitiveDict()
        headers["Authorization"] = "Basic " + self.secret
        headers["Content-Length"] = "0"

        resp = self.do_request(path, method = "POST", headers = headers )

        if resp.status_code == 201:
            resp = resp.json()
            self.session.headers.update({'X-Auth-Token': resp['token']})
            return True

        else:
            return False


    def get_tasks(self,session, reports, account_id, project_id, crawl_id, per_page = 100, page_number = 1):
        tasks = []

        for report, value in reports.items():
            report_id = value['report_id']
            path = f'/accounts/{account_id}/projects/{project_id}/crawls/{crawl_id}/reports/{report_id}/report_rows'

            params = {
                'per_page': per_page,
                'page': page_number
            }
            tasks.append(session.get(self.BASE_URL + path, params=params))
        return tasks


    async def do_async_request(self, reports, account_id, project_id, crawl_id, per_page = 100):
        """ do async requests and yield every report with its data."""

        page_number = 1  #  first data coming from page number 1. page number 0 is duplicate
        send_req = True
        per_page = settings.PER_PAGE
        reports_length = len(reports)  # length of reports are many as targets. Every target includes 1 report.
        is_last_page = False # when last page is written it switches to True

        while send_req == True:
            print(send_req)
            print(f"PAGE NUMBER {page_number}")

            #  When requests gathered and send,  responses coming of reports of targets.
            #  if every response is unprocessable then it means there is no data in the reports we are requesting.
            #  for example : If total length of reports of targets 20 and there are 20 unprocessable responses then stop requesting since
            #  there is no available data.
            count_invalid_responses= 0

            # append to requested data to dict and yield it with its report name.
            result = {}

            async with aiohttp.ClientSession(headers=self.session.headers) as session:
                tasks =  self.get_tasks(session, reports, account_id, project_id, crawl_id, per_page, page_number=page_number)
                try:
                    responses = await asyncio.gather(*tasks)

                except Exception as e:
                    print(f"excepiton occured: {str(e)}")
                    await asyncio.sleep(30)  # if something unexpected happened like , losing internet connection , wait for 30 seconds and try again
                    continue


                # append every  data to its report and tield
                for report_name, response in zip(reports, responses):

                    if response.status == 200:
                        response = await response.json()

                        result.update({report_name: response})
                    else:
                        if response.status == 403 or response.status == 500:

                            page_number = page_number - 1 # if it encounters with a server error or request limit error then try page again.
                            result = {}
                            await asyncio.sleep(30)
                            break

                        elif response.status == 422:

                            count_invalid_responses = count_invalid_responses + 1 # no data comes from report.
                            if count_invalid_responses >= reports_length: # if all reports  don't provide any data then stop requesting.
                                print(f'too many invalid responses {count_invalid_responses}')
                                send_req = False
                                is_last_page = True
                                break

                        result.update({report_name: ''}) # if response is not 200, append empty  value to its key.
                yield [result,is_last_page]
            page_number = page_number + 1 # incrase the page number to get paginated data.


    def do_request(self, path, method="GET", params={}, headers={}):

        if method == "POST":
            try:
                response = self.session.post(

                    self.BASE_URL + path, params=params, headers=headers
                )
                response.raise_for_status()
            except requests.exceptions.HTTPError as errh:
                return errh.response
            except requests.exceptions.ConnectionError as errc:
                return errc.response
            except requests.exceptions.Timeout as errt:
                return errt.response
            except requests.exceptions.RequestException as err:
                return err.response

        else:
            try:
                response = self.session.get(

                    self.BASE_URL + path, headers=self.session.headers, params=params
                )
                response.raise_for_status()
            except requests.exceptions.HTTPError as errh:
                return errh.response
            except requests.exceptions.ConnectionError as errc:
                return errc.response
            except requests.exceptions.Timeout as errt:
                return errt.response
            except requests.exceptions.RequestException as err:
                return err.response


        return response


    def get_all_projects(self, account_id):

        path = f'/accounts/{account_id}/projects'

        resp = self.do_request(path, method = "GET")
        if resp:
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 401:
                self.authorize()
                resp = self.do_request(path, method = "GET")

            elif resp.status_code == 422:
                self.test_token_exist()
                resp = self.do_request(path, method = "GET")

            return resp

        else:
            return False



    def get_project_details(self, account_id, project_id):

        path = f'/accounts/{account_id}/projects/{project_id}'

        resp = self.do_request(path, method="GET")
        if resp:
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 401:
                self.authorize()
                resp = self.do_request(path, method="GET")

            elif resp.status_code == 422:
                self.test_token_exist()
                resp = self.do_request(path, method="GET")

            return resp

        else:
            return False

    def get_project_crawls(self, account_id, project_id):

        path = f'/accounts/{account_id}/projects/{project_id}/crawls'

        resp = self.do_request(path, method="GET")
        if resp:
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 401:
                self.authorize()
                resp = self.do_request(path, method="GET")

            elif resp.status_code == 422:
                self.test_token_exist()
                resp = self.do_request(path, method="GET")

            return resp

        else:
            return False



    def get_project_reports(self, account_id, project_id, crawl_id, target):

        """get project report based on target, example target: 301_redirects"""
        path = f'/accounts/{account_id}/projects/{project_id}/crawls/{crawl_id}/reports/{target}'

        resp = self.do_request(path, method="GET")
        if resp:
            if resp.status_code == 200:
                return resp
            elif resp.status_code == 401:
                self.authorize()
                resp = self.do_request(path, method="GET")
            elif resp.status_code == 422:
                self.test_token_exist()
                resp = self.do_request(path, method="GET")

            return resp

        else:
            return False


    def get_project_report_data(self, account_id, project_id, crawl_id, report_id, per_page = 100, page_number = 0):

        """get project report based on target, example target: 301_redirects"""
        path = f'/accounts/{account_id}/projects/{project_id}/crawls/{crawl_id}/reports/{report_id}/report_rows'

        params = {
            'per_page': per_page,
            'page': page_number
        }

        resp = self.do_request(path, method="GET", params = params)


        if resp.status_code == 401:
            self.authorize()
            resp = self.do_request(path, method="GET", params = params)
            return resp
        elif resp.status_code == 422:
            return resp.status_code
        elif resp.status_code == 400:
            return resp.status_code

        return resp
