import boto3
import time
import os
from dotenv import load_dotenv
from .logger import NuclioLogger



class NuclioAthena:

    def __init__(self):
        load_dotenv()

        self.database                = os.getenv('SYNC_DATABASE_ATHENA')
        self.bucket_s3               = os.getenv('SYNC_BUCKET_S3')
        self.query_output_prefix     = os.getenv('SYNC_QUERY_OUTPUT_PREFIX')
        self.local_download_path     = os.getenv('SYNC_LOCAL_DOWNLOAD_PATH')

        self.aws_access_key_id       = os.getenv('SYNC_AWS_ACCESS_KEY_ID')
        self.aws_secret_access_key   = os.getenv('SYNC_AWS_SECRET_ACCESS_KEY')
        self.region_name             = os.getenv('SYNC_REGION_NAME')

        self.logger = NuclioLogger(service="pipeline-athena", min_level="INFO")

        self.athena_client = boto3.client('athena',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )

        self.s3_client = boto3.client('s3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name
        )

    def executar_consulta(self, query):
        response = self.athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': self.database},
            ResultConfiguration={
                'OutputLocation': f's3://{self.bucket_s3}/{self.query_output_prefix}'
            }
        )
        return response['QueryExecutionId']

    def esperar_conclusao_consulta(self, query_execution_id):
        while True:
            response = self.athena_client.get_query_execution(QueryExecutionId=query_execution_id)

            status = response['QueryExecution']['Status']['State']

            if status in ['SUCCEEDED']:
                return response['QueryExecution']['ResultConfiguration']['OutputLocation']

            if status in ['FAILED']:
                self.logger.error("Ocorreu uma falha", context={"erro": str(response)})
                break

            self.logger.info("Aguardando a conclusão da consulta...", context={})
            time.sleep(10)

    def download_query_results(self, s3_location, filename):

        # Definindo o diretório destino para o arquivo
        diretorio_destino =  filename + '.csv'

        bucket_name = s3_location.replace('s3://', '').split('/')[0]
        key = '/'.join(s3_location.replace('s3://', '').split('/')[1:])

        # Baixar o arquivo do S3 para o diretório local
        try:
            self.logger.info(f"bucket_name {bucket_name} key {key} diretorio_destino {diretorio_destino}", context={})
            self.s3_client.download_file(bucket_name, key, diretorio_destino)
            self.logger.info(f"Arquivo de resultados baixado de {s3_location} para {diretorio_destino}", context={})
        except Exception as e:
            self.logger.error(f"Ocorreu um erro ao tentar baixar o arquivo do S3:", context={"error": str(e)})