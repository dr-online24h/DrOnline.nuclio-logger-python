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
│   ├── requirements.txt        # dependências
│   ├── .env.example            # exemplo de variáveis de ambiente
│   ├── nuclio_logger/
│   │   ├── __init__.py
│   │   ├── logger.py           # logger JSON estruturado
│   │   ├── athena.py           # cliente AWS Athena
│   │   ├── database.py         # cliente PostgreSQL
│   │   └── rabbitmq.py         # cliente RabbitMQ
│   └── example/
│       └── example.py          # exemplos de uso
```

---

## Instalação

### Python

```bash
# pelo GitHub diretamente
pip install git+https://github.com/dronline/nuclio-logger-python.git

# ou localmente (dentro da pasta python/)
pip install -e .

# Instalar dependências adicionais
pip install -r requirements.txt
```

**Dependências:**
- `pika>=1.3.0` - Cliente RabbitMQ (para usar NuclioRabbitMQ)
- `boto3>=1.26.0` - Cliente AWS (para usar NuclioAthena)
- `psycopg2-binary>=2.9.0` - Cliente PostgreSQL (para usar NuclioDatabase)
- `python-dotenv>=1.0.0` - Gerenciamento de variáveis de ambiente

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

## NuclioRabbitMQ

Cliente RabbitMQ padronizado com suporte a mensagens **imediatas** e **com delay**.

### Configuração

Crie um arquivo `.env` baseado no `.env.example`:

```bash
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5672
RABBITMQ_USERNAME=guest
RABBITMQ_PASSWORD=guest
RABBITMQ_VIRTUAL_HOST=/
```

### Uso

```python
from nuclio_logger import NuclioRabbitMQ

rabbitmq = NuclioRabbitMQ()

# 1. Publicar mensagem imediata
resultado = rabbitmq.publish_immediate(
    queue_name="notificacoes",
    message={"usuario_id": 123, "mensagem": "Bem-vindo!"}
)

# 2. Publicar mensagem com delay de 5 segundos
resultado = rabbitmq.publish_delayed(
    queue_name="lembretes",
    message={"usuario_id": 456, "mensagem": "Lembrete importante"},
    delay_ms=5000  # 5 segundos
)

# 3. Método unificado (delay_ms=0 para envio imediato)
resultado = rabbitmq.publish(
    queue_name="eventos",
    message={"evento": "login"},
    delay_ms=0  # 0 = imediato, >0 = com delay
)

# 4. Publicar com delay de 30 segundos
resultado = rabbitmq.publish(
    queue_name="tarefas",
    message={"tarefa": "processar_relatorio"},
    delay_ms=30000  # 30 segundos
)
```

### Como funciona o delay

O mecanismo de delay usa **TTL (Time To Live) + Dead Letter Exchange** do RabbitMQ:

1. Uma fila temporária é criada com TTL configurado: `{queue_name}.delayed.{delay_ms}ms`
2. A mensagem é publicada nesta fila temporária
3. Após o TTL expirar, a mensagem é automaticamente redirecionada para a fila final
4. O consumidor recebe a mensagem da fila final após o delay

**Vantagens:**
- Não requer plugin adicional no RabbitMQ
- Mensagens persistentes (sobrevivem a reinicializações)
- Delay preciso em milissegundos

### Retorno dos métodos

Todos os métodos retornam um dicionário com:

```python
{
    'status': True,  # ou False em caso de erro
    'message': 'Mensagem descritiva do resultado'
}
```

Em caso de erro, também inclui:

```python
{
    'status': False,
    'message': 'Erro ao publicar mensagem',
    'context': {'queue': 'nome_da_fila', 'erro': 'detalhes do erro'}
}
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


## Serverless

`pip install \
  git+https://github.com/dr-online24h/DrOnline.nuclio-logger-python.git`