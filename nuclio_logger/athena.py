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

    def athena_query_to_dataframe(
        query: str,
        database: str = "postgresdb",
        output_location: str = "s3://backup-dronline/athena-results/",
        region_name: str = "us-west-2",
        sleep_time: int = 2,
    ) -> pd.DataFrame:
        """
        Executa uma query no Athena e retorna um DataFrame.

        :param query: SQL a ser executado
        :param database: Database do Athena
        :param output_location: S3 onde o Athena salva os resultados (ex: s3://bucket/path/)
        :param region_name: Região AWS
        :param sleep_time: Intervalo de espera entre checks
        :return: pandas.DataFrame
        """

        athena = boto3.client("athena", region_name=region_name)

        # 1️⃣ Executar query
        response = athena.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_location},
        )

        query_execution_id = response["QueryExecutionId"]

        # 2️⃣ Esperar finalizar
        while True:
            result = athena.get_query_execution(QueryExecutionId=query_execution_id)
            status = result["QueryExecution"]["Status"]["State"]

            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break

            time.sleep(sleep_time)

        if status != "SUCCEEDED":
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "")
            raise Exception(f"Query falhou: {status} - {reason}")

        # 3️⃣ Buscar resultados com paginação
        paginator = athena.get_paginator("get_query_results")
        page_iterator = paginator.paginate(QueryExecutionId=query_execution_id)

        columns = None
        rows = []

        for page in page_iterator:
            if columns is None:
                columns = [
                    col["Label"]
                    for col in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
                ]

            for row in page["ResultSet"]["Rows"]:
                rows.append([col.get("VarCharValue") for col in row["Data"]])

        # Remove header duplicado (primeira linha)
        rows = rows[1:]

        df = pd.DataFrame(rows, columns=columns)

        return df


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