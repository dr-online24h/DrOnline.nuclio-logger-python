import psycopg2
from psycopg2.extras import execute_values
from .logger import NuclioLogger

class NuclioDatabase:

    def __init__(self):
        self.logger = NuclioLogger(service="pipeline-database", min_level="INFO")

    def consultar_coluna(host, database, port, user, password, tabela, coluna):
        try:
            # Estabelece a conexão com o banco de dados
            conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port,
            )

            # Cria um cursor para executar as consultas
            cur = conn.cursor()

            # Define a consulta SQL
            consulta_sql = f"SELECT {coluna} FROM {tabela} order by {coluna}::integer desc limit 1;"

            # Executa a consulta
            cur.execute(consulta_sql)

            # Obtém todos os resultados
            resultados = cur.fetchall()

            # Fecha o cursor e a conexão
            cur.close()
            conn.close()

            # Retorna a coluna (como uma lista de valores)
            return [resultado[0] for resultado in resultados]  # Extrai os valores da coluna

        except Exception as e:
            self.logger.error("Erro ao consultar o banco de dados", context={"erro": str(e)})
            return []

    def insert(host, database, port, user, password, query, values):
        try:
            # Estabelece a conexão com o banco de dados
            conn = psycopg2.connect(
                host=host,
                database=database,
                user=user,
                password=password,
                port=port,
            )

            # Cria um cursor para executar as consultas
            cur = conn.cursor()
            execute_values(cur, query, values)
            conn.commit()
            cur.close()
            conn.close()

            return {
                'status': True,
                'message': f'Carga {tabela} inicial atualizada com sucesso!'
            }

        except Exception as e:
            self.logger.error("Erro ao inserir dados o banco de dados", context={"query": query, "error": str(e)})
            return {
                'status': False,
                'message': 'Erro ao inserir dados o banco de dados',
                "context":  {"query": query, "error": str(e)}
            }
