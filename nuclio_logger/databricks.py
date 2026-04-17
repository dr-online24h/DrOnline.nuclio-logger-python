import os
from dotenv import load_dotenv
from databricks import sql
from databricks.sdk.core import Config, oauth_service_principal
import pandas as pd
from .logger import NuclioLogger


class NuclioDatabricks:
    """Cliente Databricks padronizado para funcoes Nuclio com suporte a queries SQL."""

    def __init__(self):
        load_dotenv()

        self.server_hostname = os.getenv('DATABRICKS_SERVER_HOSTNAME', '')
        self.http_path = os.getenv('DATABRICKS_HTTP_PATH', '')
        self.client_id = os.getenv('DATABRICKS_CLIENT_ID', '')
        self.client_secret = os.getenv('DATABRICKS_CLIENT_SECRET', '')

        self.logger = NuclioLogger(service="nuclio-databricks", min_level="INFO")
        self._connection = None

    def _credential_provider(self):
        """
        Configura o provedor de credenciais OAuth para autenticacao com Databricks.

        Returns:
            Funcao de autenticacao OAuth service principal
        """
        config = Config(
            host=self.server_hostname,
            client_id=self.client_id,
            client_secret=self.client_secret
        )
        return oauth_service_principal(config)

    def _connect(self):
        """
        Estabelece conexao com o Databricks.

        Returns:
            bool: True se a conexao foi estabelecida com sucesso, False caso contrario
        """
        try:
            if self._connection is None or not self._connection:
                self.logger.info("Iniciando conexao com Databricks...", context={
                    "server_hostname": self.server_hostname,
                    "http_path": self.http_path
                })

                self._connection = sql.connect(
                    server_hostname=self.server_hostname,
                    http_path=self.http_path,
                    credentials_provider=self._credential_provider
                )

                self.logger.info("Conexao com Databricks estabelecida", context={
                    "server_hostname": self.server_hostname
                })
            return True
        except Exception as e:
            self.logger.error("Erro ao conectar ao Databricks", context={"erro": str(e)})
            return False

    def _close(self):
        """Fecha a conexao com o Databricks."""
        try:
            if self._connection:
                self._connection.close()
                self._connection = None
                self.logger.debug("Conexao com Databricks fechada", context={})
        except Exception as e:
            self.logger.error("Erro ao fechar conexao com Databricks", context={"erro": str(e)})

    def execute_query(self, query):
        """
        Executa uma query SQL no Databricks e retorna o resultado como DataFrame.

        Args:
            query: String SQL a ser executada

        Returns:
            dict com status, message e data (DataFrame se sucesso, None se falha)
            Exemplo de retorno sucesso:
            {
                'status': True,
                'message': 'Query executada com sucesso',
                'data': DataFrame,
                'rows_count': 100
            }
            Exemplo de retorno erro:
            {
                'status': False,
                'message': 'Erro ao executar query',
                'data': None,
                'context': {'query': '...', 'erro': '...'}
            }
        """
        try:
            if not self._connect():
                return {
                    'status': False,
                    'message': 'Erro ao conectar ao Databricks',
                    'data': None
                }

            with self._connection.cursor() as cursor:
                self.logger.info("Executando query...", context={
                    "query_preview": query[:200] if len(query) > 200 else query
                })

                cursor.execute(query)
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(result, columns=columns)

                rows_count = len(df)

                self.logger.info("Query executada com sucesso", context={
                    "rows_count": rows_count,
                    "columns_count": len(columns)
                })

                self._close()

                return {
                    'status': True,
                    'message': 'Query executada com sucesso',
                    'data': df,
                    'rows_count': rows_count
                }

        except Exception as e:
            self.logger.error("Erro ao executar query no Databricks", context={
                "query": query[:200] if len(query) > 200 else query,
                "erro": str(e)
            })
            self._close()
            return {
                'status': False,
                'message': 'Erro ao executar query',
                'data': None,
                'context': {"query": query[:200], "erro": str(e)}
            }
