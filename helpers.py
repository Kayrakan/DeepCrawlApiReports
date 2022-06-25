import asyncio

from bigquery import big_query_writer, big_query_writer2
import utils
import settings
import csv
from deepcrawl import DeepCrawl

# The reason targets for deepcrawl and targets for bigquery is different, bigquery doesnt accept the  format.
# For example bigquery will not accept the target as column header if it starts with numbers or non-alphabetic characters.
targets = ['non_301_redirects_basic', '301_redirects_basic', '4xx_errors_basic', '5xx_errors_basic', 'http_pages_basic', 'https_pages_basic',
           'non_indexable_pages_basic','indexable_pages_basic', 'unlinked_canonical_pages_basic', 'unique_pages_basic', 'duplicate_pages_basic',
           'paginated_pages_basic', 'non-200_pages_basic', 'mobile_alternates_basic', 'failed_urls_basic', 'external_urls_crawled_basic',
           'pages_without_social_markup_basic', 'pages_with_social_markup_basic','double_slash_urls_basic',
           'max_url_length_basic', 'all_sitemaps_links_basic'

           ]


targets_bigquery = ['non_301_redirects_basic', 'redirects_301_basic', 'errors_4xx_basic', 'errors_5xx_basic', 'http_pages_basic', 'https_pages_basic',
           'non_indexable_pages_basic','indexable_pages_basic', 'unlinked_canonical_pages_basic', 'unique_pages_basic', 'duplicate_pages_basic',
           'paginated_pages_basic', 'non_200_pages_basic', 'mobile_alternates_basic', 'failed_urls_basic', 'external_urls_crawled_basic',
           'pages_without_social_markup_basic', 'pages_with_social_markup_basic','double_slash_urls_basic',
           'max_url_length_basic', 'all_sitemaps_links_basic'

           ]

def write_headers_first_time(targets_bigquery,csv_name):

    """
    creates a csv file with a
    unique name and writes headers first. If it is for the first time,
    it triggers to reset bigquery table and write headers
    """

    with open(f't{csv_name}est.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=targets_bigquery)
        writer.writeheader()

    big_query_writer(csv_name)

def write_headers(targets_bigquery,csv_name):
    """
    creates a csv file with a
    unique name and writes headers first
    """

    with open(f't{csv_name}est.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=targets_bigquery)
        writer.writeheader()



async def write_to_csv(chunks, total_row,csv_name, is_last_page):

    with open(f't{csv_name}est.csv', 'a', encoding='UTF8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=targets_bigquery)
        writer.writerows(chunks)

    #if total row 10k, start off bigquery writing
    if total_row == 10000:
        big_query_writer2(csv_name)
        # pass

    #if total row  is not 10k but it is the last page, start off bigquery writing for the rest
    elif is_last_page == True:
        big_query_writer2(csv_name)
        print('Writing lastt page')

async def start_deepcrawl():
    """ this is the function that triggers getting the reports and
     sending it to csv writer functions as the data is yielded here."""

    csv_name = 0 # as increases csv names changes as 1,2, 3,4. .. Every 10k result from request is written to a csv file and sent to bigquery
    write_headers_first_time(targets_bigquery, csv_name) # write headers first.
    deepcrawl = DeepCrawl(secret=settings.SECRET)

    # get reports from a project
    reports = utils.get_report_per_target(deepcrawl, settings.ACCOUNT_ID, settings.PROJECT_ID, settings.CRAWL_ID,
                                          targets)
    total_rows = 0

    # get reports' data as chunks, append to a csv and write every 10k rows to bigquery as it accumulates.
    async for chunks in utils.get_target_report_data(deepcrawl, settings.ACCOUNT_ID, settings.PROJECT_ID, settings.CRAWL_ID, reports):

            is_last_page = chunks[1]
            total_rows = total_rows + len(chunks[0])

            asyncio.create_task(write_to_csv(chunks[0],total_rows, csv_name, is_last_page))
            if total_rows == 10000:
                total_rows = 0
                csv_name = csv_name + 1  #adding to csv file name a number so  every csv file's name would be different.
                write_headers(targets_bigquery, csv_name)

            #if we are in last page and row accumulation in csv file doesn't reach 10k.  Write the rest.
            elif is_last_page == True:
                total_rows = 0
                csv_name = csv_name + 1
                write_headers(targets_bigquery, csv_name)


