
import copy
from datetime import datetime
from deepcrawl import DeepCrawl
import settings

def get_report_per_target( deepcrawl: DeepCrawl, account_id, project_id, crawl_id, targets= []) -> dict:

    reports = {}

    for target in targets:

        resp = deepcrawl.get_project_reports(account_id, project_id, crawl_id, target)

        resp = resp.json()

        # there are overall information related to a target report
        reports[target] = {
            'report_id': resp['id'],
            'total_rows': resp['total_rows'],
            'basic_total': resp['basic_total'],
            'removed_total': resp['removed_total'],
            'added_tottal': resp['added_total'],
            'missing_total': resp['missing_total'],
            'change_weight': resp['change_weight'],
            'total_weight': resp['total_weight'],
            'change': resp['change']


        }

    return reports


# esssential report data
async def get_target_report_data(deepcrawl: DeepCrawl, account_id, project_id, crawl_id, reports) -> list:

    per_page = settings.PER_PAGE # the results are paginated. So it gives only 100 item per_page.
    report_data = []

    for i in range(per_page):
        report_data.append({})

    async for result in deepcrawl.do_async_request(reports,account_id, project_id, crawl_id ):

        url_list = copy.deepcopy(report_data)


        for report, value in result[0].items():
            rep = report

            if report == '301_redirects_basic':
                rep = 'redirects_301_basic'
            elif report == '4xx_errors_basic':
                rep = 'errors_4xx_basic'
            elif report == '5xx_errors_basic':
                rep = 'errors_5xx_basic'
            elif report == 'non-200_pages_basic':
                rep = 'non_200_pages_basic'

            for v,p in zip(value, range(per_page)):


                if 'data' in v:

                    #  we are extracting the urls from Deepcrawl reports. There are two type url key. It is either 'url' or  'url_from'.
                    # They serve the same purpose
                    if 'url' in v['data']:

                        url_list[p].update({rep:v['data']['url']})
                    elif 'url_from' in v['data']:
                        url_list[p].update({rep:v['data']['url_from']})
                    else:
                        print(f'url is not url {v["data"]}')
                        with open("eeror.txt", "a") as outfile:
                            outfile.write(f'url is not url {v["data"]} date {datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")}')
                        url_list[p].update({rep:None})

                else:
                    url_list[p].update({rep: None })

        # yield results to be written in the csv file.
        yield [url_list, result[1]]




























        # with open('test.csv', 'w', encoding='UTF8', newline='') as f:
        #     writer = csv.DictWriter(f, fieldnames=targets_bigquery)
        #     writer.writeheader()
        #     writer.writerows(i)

        # print('writing')
        # big_query_writer2()
    #
        # for report,value in reports.items():
        #     report_id = value['report_id']
        #     rep = report
        #
        #
        #     resp = deepcrawl.get_project_report_data(account_id, project_id, crawl_id, report_id, per_page = per_page, page_number = page_number)
        #
            # if report == '301_redirects_basic':
            #     rep= 'redirects_301_basic'
            # elif report == '4xx_errors_basic':
            #     rep = 'errors_4xx_basic'
            # elif report == '5xx_errors_basic':
            #     rep = 'errors_5xx_basic'
            # elif report == 'non-200_pages_basic':
            #     rep = 'non_200_pages_basic'
        #
        #
        #     if resp == 422:
        #         count = count+1
        #         obj_counter = 0
        #         print(f'for {rep} end of all pages')
        #         for p in range(per_page):
        #             url_list[p].update({rep: None})
        #         continue
        #     elif resp != 200:
        #         with open("eeror.txt", "a") as outfile:
        #             outfile.write(f'error, resp not 200 , date {datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")} error {str(resp)}')
        #
        #
        #
        #
        #
        #     if not resp:
        #         with open("eeror.txt", "a") as outfile:
        #             outfile.write(f'errpr, resp is false, date {datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")} and error {str(resp)}')
        #
        #
        #     else:
        #         resp = resp.json()
        #
        #
        #     for r,p in zip(resp, range(per_page)):
        #
        #         if 'data' in r:
        #
        #             print('append')
        #             if 'url' in r['data']:
        #
        #                 url_list[p].update({rep:r['data']['url']})
        #             elif 'url_from' in r['data']:
        #                 url_list[p].update({rep:r['data']['url_from']})
        #             else:
        #                 print(f'url is not url {r["data"]}')
        #                 with open("eeror.txt", "a") as outfile:
        #                     outfile.write(f'url is not url {r["data"]} date {datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")}')
        #                 url_list[p].update({rep:None})
        #
        #         else:
        #             url_list[p].update({rep: None })
        #
        #
        #     continue
        # print('yielding')
        # yield url_list
#end



# def get_target_report_data_csv(deepcrawl, account_id, project_id, crawl_id, reports) -> list:
#
#     per_page = 100
#     report_data = []
#     count = 0
#     send_req = True
#     print(f'reports lenght: {len(reports)}')
#     length = len(reports)
#     page_number = 0
#
#     for i in range(per_page):
#         report_data.append([])
#
#     while send_req:
#
#         if count == length:
#             send_req = False
#             break
#         page_number = page_number + 1
#         url_list = copy.deepcopy(report_data)
#
#         count = 0
#
#         for report,value in reports.items():
#             report_id = value['report_id']
#
#             # total_pages = math.ceil((int(value['total_rows']) / 100))
#
#             # while send_req:
#
#             # if page_number > 2:
#             #     break
#             resp = deepcrawl.get_project_report_data(account_id, project_id, crawl_id, report_id, per_page = per_page, page_number = page_number)
#
#             if not resp:
#                 count = count+1
#                 obj_counter = 0
#                 for p in range(per_page):
#                     url_list[p].append(None)
#                 continue
#
#             # print(len(report_data))
#             # report_data.append({report:resp.json()})
#
#             resp = resp.json()
#
#             print('lenlist')
#             print(len(url_list))
#
#             for r,p in zip(resp, range(per_page)):
#
#                 if 'data' in r:
#                     # print(r)
#                     if 'url' in r['data']:
#                         url_list[p].append(r['data']['url'])
#                     elif 'url_from' in r['data']:
#                         url_list[p].append(r['data']['url_from'])
#                     else:
#                         url_list[p].append(None)
#
#                 else:
#                     url_list[p].append( None )
#
#
#                         # list_url[counter] = v['data']['url']
#
#
#             continue
#         print('writiing')
#         yield url_list


def link_chunk(chunk, columns = []):
    link_chunk = compose_url_chunk(chunk[1].json())

    return link_chunk



def compose_url_chunk(chunk) -> list:

    url_list = []
    chunk = chunk.json()
    # print(chunk)

    for i in chunk:
        url_list.append(i['data']['url'])

    return url_list
