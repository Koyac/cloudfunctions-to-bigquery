from google.cloud import bigquery
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

def gcs_to_bq(data, context):
    client = bigquery.Client()

    bucket_name = data['bucket']
    file_name = data['name']
    file_end = file_name.split('.')[-1]

    if file_end != 'csv':
        logger.info("エラー:CSVファイルで格納してください。")
        return 

    uri = f'gs://{bucket_name}/{file_name}'
    table_id = 'goods_sales'
    dataset_id = 'sales_calculation'
    partiton_date = file_name.rsplit("/")[1]

    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.skip_leading_rows = 1
    job_config.schema = [
        bigquery.SchemaField("Date", "STRING"),
        bigquery.SchemaField("Name", "STRING"),
        bigquery.SchemaField("Amount", "INTEGER"),
    ]
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.time_partitioning = bigquery.table.TimePartitioning(type_=bigquery.TimePartitioningType.DAY,require_partition_filter=True)
    job_config.write_disposition = 'WRITE_TRUNCATE'

    bq_dest = dataset_ref.table(f'{table_id}${partiton_date}')
    try:
        load_job = client.load_table_from_uri(uri,bq_dest,job_config=job_config)
        load_job.result()
        logger.info(f"処理が完了しました:{load_job.output_rows} 行挿入しました。")
        
    except Exception as e:
        logger.error(f"データを挿入する際に、エラーが発生しました: {e}")
