# nuclio-logger

Logger JSON padronizado para funcoes Nuclio serverless.
Mesmo formato de saída em **Python** — facilita o parsing no Loki/Grafana.

---

## Formato de log (padrão em todas as linguagens)

```json
{
  "timestamp": "2026-02-04T10:30:00.123Z",
  "level":     "INFO",
  "service":   "nome-da-funcao",
  "message":   "mensagem descritiva",
  "context":   { "chave": "valor" }
}
```

| Campo       | Descrição                                                        |
|-------------|------------------------------------------------------------------|
| `timestamp` | UTC, formato ISO 8601 com milissegundos                          |
| `level`     | `DEBUG` / `INFO` / `WARNING` / `ERROR` / `CRITICAL`             |
| `service`   | Nome da funcao ou servico (definido na instanciação)             |
| `message`   | Texto livre da mensagem                                          |
| `context`   | Objeto opcional com dados extras                                 |

---

## Estrutura do repo

```
nuclio-logger/
├── python/
│   ├── pyproject.toml          # config do pacote pip
│   ├── nuclio_logger/
│   │   ├── __init__.py
│   │   └── logger.py           # implementação
│   └── example.py              # exemplo de uso

---

## Instalação

### Python

```bash
# pelo GitHub diretamente
pip install git+https://github.com/dronline/nuclio-logger-python.git

# ou localmente (dentro da pasta python/)
pip install -e .
```

---

## Uso rapido

### Python

```python
from nuclio_logger import NuclioLogger

logger = NuclioLogger(service="minha-funcao", min_level="INFO")

logger.info("Requisicao recebida", context={"path": "/api/dados"})
logger.error("Falha ao conectar", context={"erro": "timeout"})
```
---

## Niveis de log

| Nivel      | Uso                                          |
|------------|----------------------------------------------|
| `DEBUG`    | Dados internos para diagnóstico              |
| `INFO`     | Fluxo normal da aplicação                    |
| `WARNING`  | Situações inesperadas mas não críticas       |
| `ERROR`    | Falhas que impedem uma operação              |
| `CRITICAL` | Falhas graves que exigem atenção imediata    |

O parâmetro `min_level` na instanciação filtra tudo abaixo do nível definido.
Ex: `min_level="WARNING"` -> apenas WARNING, ERROR e CRITICAL são emitidos.

---

## Publicar no GitHub (cada linguagem em repo separado)

Estrutura sugerida de repos:

| Repo                                    | Conteúdo             |
|-----------------------------------------|--------------------------------------|
| `github.com/dronline/nuclio-logger-python` | pasta `python/`   |
| `github.com/dronline/nuclio-logger-nodejs` | pasta `nodejs/`   |
| `github.com/dronline/nuclio-logger-php`    | pasta `php/`      |

Cada repo recebe apenas o conteúdo da pasta correspondente na raiz.
