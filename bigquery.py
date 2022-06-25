import os


rows_to_insert = []

def big_query_writer(csv_name):
    from google.cloud import bigquery


    client = bigquery.Client()
    table_id = 'scraper-326120.hbSeo.deepcrwl'

    # create configuration and schema for bigquery table.
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
                schema=[
                    bigquery.SchemaField("non_301_redirects_basic", "STRING"),
                    bigquery.SchemaField("redirects_301_basic", "STRING"),
                    bigquery.SchemaField("errors_basic4xx", "STRING"),
                    bigquery.SchemaField("errors_basic5xx", "STRING"),
                    bigquery.SchemaField("http_pages_basic", "STRING"),
                    bigquery.SchemaField("https_pages_basic", "STRING"),
                    bigquery.SchemaField("non_indexable_pages_basic", "STRING"),
                    bigquery.SchemaField("indexable_pages_basic", "STRING"),
                    bigquery.SchemaField("unlinked_canonical_pages_basic", "STRING"),
                    bigquery.SchemaField("unique_pages_basic", "STRING"),

                    bigquery.SchemaField("duplicate_pages_basic", "STRING"),
                    bigquery.SchemaField("paginated_pages_basic", "STRING"),
                    bigquery.SchemaField("non_200_pages_basic", "STRING"),
                    bigquery.SchemaField("mobile_alternates_basic", "STRING"),

                    bigquery.SchemaField("failed_urls_basic", "STRING"),
                    bigquery.SchemaField("external_urls_crawled_basic", "STRING"),
                    bigquery.SchemaField("pages_without_social_markup_basic", "STRING"),
                    bigquery.SchemaField("pages_with_social_markup_basic", "STRING"),
                    bigquery.SchemaField("double_slash_urls_basic", "STRING"),

                    bigquery.SchemaField("max_url_length_basic", "STRING"),
                    bigquery.SchemaField("all_sitemaps_links_basic", "STRING"),

                ],
    )


    # read data and load to bigquery table
    with open(f't{csv_name}est.csv', "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Header writer Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )


def big_query_writer2(csv_name):
    from google.cloud import bigquery

    client = bigquery.Client()
    table_id = 'scraper-326120.hbSeo.deepcrwl'
    table = client.get_table(table_id)
    original_schema = table.schema
    new_schema = original_schema[:]

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
        schema = new_schema
    )

    with open(f't{csv_name}est.csv', "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
    os.remove(f't{csv_name}est.csv') # remove the after after loading the data to bigquery.