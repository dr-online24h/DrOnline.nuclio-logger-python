"""
Exemplo de uso da nuclio-logger em uma funcao Nuclio (Python).

Instalar localmente:
    pip install -e .

Instalar pelo GitHub:
    pip install git+https://github.com/dronline/nuclio-logger-python.git
"""

from nuclio_logger import NuclioLogger

# instancia global -- reuse entre invocacoes do container
logger = NuclioLogger(service="minha-funcao", min_level="DEBUG")


def handler(event, context):
    """Entry-point da funcao Nuclio."""

    logger.info("Requisicao recebida", context={"path": event.get("path")})

    try:
        payload = event.get("body", {})
        logger.debug("Payload recebido", context={"payload": payload})

        # --- sua logica aqui ---
        resultado = {"status": "ok", "dados": payload}

        logger.info("Processamento concluido", context={"resultado": resultado})
        return resultado

    except Exception as exc:
        logger.error("Erro no processamento", context={"erro": str(exc)})
        raise


# -------------------------------------------------------------------------
# teste rapido fora do Nuclio
# -------------------------------------------------------------------------
if __name__ == "__main__":
    logger.debug("mensagem debug")
    logger.info("mensagem info")
    logger.warning("mensagem warning", context={"campo": "valor"})
    logger.error("mensagem erro", context={"excecao": "ValueError"})
    logger.critical("mensagem critica")
