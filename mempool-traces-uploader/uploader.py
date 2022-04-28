from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()
table_id = "bitcoin-data-analysis-320014.transaction_analysis.mempool_traces"
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=0,
    field_delimiter=" ",
    autodetect=True,
)

with open("times.lst", "rb") as source_file:
    job = client.load_table_from_file(
        source_file, table_id, job_config=job_config
    )

job.result()  # Waits for the job to complete.

table = client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)
