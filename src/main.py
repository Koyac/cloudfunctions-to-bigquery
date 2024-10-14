import functions_framework
from google.cloud import bigquery
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

@functions_framework.cloud_event
def gcs_to_bq(cloud_event):
    data = cloud_event.data
    bucket_name = data['bucket']
    file_name = data['name']
    file_end = file_name.split('.')[-1]

    if file_end != 'csv':
        logger.info("エラー:CSVファイルで格納してください。")
        return 

    uri = f'gs://{bucket_name}/{file_name}'
    dataset_id = 'sales_calculation'
    table_id = 'goods_sales'
    partiton_date = file_name.rsplit("/")[1]

    client = bigquery.Client()
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
