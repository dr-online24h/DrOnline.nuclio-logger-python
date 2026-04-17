import pika
import json
import os
from dotenv import load_dotenv
from .logger import NuclioLogger


class NuclioRabbitMQ:
    """Cliente RabbitMQ padronizado para funcoes Nuclio com suporte a mensagens imediatas e com delay."""

    def __init__(self):
        load_dotenv()

        self.host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.port = int(os.getenv('RABBITMQ_PORT', '5672'))
        self.username = os.getenv('RABBITMQ_USERNAME', 'guest')
        self.password = os.getenv('RABBITMQ_PASSWORD', 'guest')
        self.virtual_host = os.getenv('RABBITMQ_VIRTUAL_HOST', '/')

        # Configuracoes de exchange e queue
        self.image_docker = os.getenv('IMAGE_DOCKER', 'default')
        self.exchange_name = os.getenv('RABBITMQ_EXCHANGE_NAME', f'exchange-{self.image_docker}')
        self.exchange_type = os.getenv('RABBITMQ_EXCHANGE_TYPE', 'direct')
        self.durable = os.getenv('RABBITMQ_DURABLE', 'true').lower() == 'true'
        self.auto_ack = os.getenv('RABBITMQ_AUTO_ACK', 'false').lower() == 'true'
        self.on_error = os.getenv('RABBITMQ_ON_ERROR', 'nack')
        self.queue_name = os.getenv('RABBITMQ_QUEUE_NAME', f'queue-{self.image_docker}')

        self.logger = NuclioLogger(service="nuclio-rabbitmq", min_level="INFO")
        self._connection = None
        self._channel = None

    def _connect(self):
        """Estabelece conexao com o RabbitMQ."""
        try:
            if self._connection is None or self._connection.is_closed:
                credentials = pika.PlainCredentials(self.username, self.password)
                parameters = pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    virtual_host=self.virtual_host,
                    credentials=credentials,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
                self._connection = pika.BlockingConnection(parameters)
                self._channel = self._connection.channel()
                self.logger.info("Conexao com RabbitMQ estabelecida", context={
                    "host": self.host,
                    "port": self.port
                })
            return True
        except Exception as e:
            self.logger.error("Erro ao conectar ao RabbitMQ", context={"erro": str(e)})
            return False

    def _close(self):
        """Fecha a conexao com o RabbitMQ."""
        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
            if self._connection and self._connection.is_open:
                self._connection.close()
            self.logger.debug("Conexao com RabbitMQ fechada", context={})
        except Exception as e:
            self.logger.error("Erro ao fechar conexao com RabbitMQ", context={"erro": str(e)})

    def _setup_delayed_queue(self, queue_name, delay_ms):
        """
        Configura fila com delay usando TTL e Dead Letter Exchange.

        Args:
            queue_name: nome da fila de destino final
            delay_ms: tempo de delay em milissegundos
        """
        try:
            # Nome da fila temporaria com delay
            delay_queue_name = f"{queue_name}.delayed.{delay_ms}ms"

            # Declara a fila de destino final
            self._channel.queue_declare(queue=queue_name, durable=True)

            # Declara a fila temporaria com TTL que envia para a fila final
            self._channel.queue_declare(
                queue=delay_queue_name,
                durable=True,
                arguments={
                    'x-message-ttl': delay_ms,  # TTL em milissegundos
                    'x-dead-letter-exchange': '',  # Exchange padrao
                    'x-dead-letter-routing-key': queue_name  # Routing key = fila final
                }
            )

            return delay_queue_name
        except Exception as e:
            self.logger.error("Erro ao configurar fila com delay", context={
                "queue": queue_name,
                "delay_ms": delay_ms,
                "erro": str(e)
            })
            raise

    def publish_immediate(self, queue_name, message, exchange='', routing_key=None, properties=None):
        """
        Publica uma mensagem imediatamente na fila.

        Args:
            queue_name: nome da fila
            message: mensagem a ser enviada (dict sera convertido para JSON)
            exchange: nome do exchange (default: '' para exchange padrao)
            routing_key: routing key (default: queue_name)
            properties: propriedades adicionais da mensagem (pika.BasicProperties)

        Returns:
            dict com status e message
        """
        try:
            if not self._connect():
                return {
                    'status': False,
                    'message': 'Erro ao conectar ao RabbitMQ'
                }

            # Declara a fila (idempotente)
            self._channel.queue_declare(queue=queue_name, durable=True)

            # Converte mensagem para JSON se for dict
            if isinstance(message, dict):
                body = json.dumps(message, ensure_ascii=False)
            else:
                body = str(message)

            # Define propriedades padrao
            if properties is None:
                properties = pika.BasicProperties(
                    delivery_mode=2,  # Mensagem persistente
                    content_type='application/json'
                )

            # Publica a mensagem
            routing_key = routing_key or queue_name
            self._channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=body,
                properties=properties
            )

            self.logger.info("Mensagem publicada imediatamente", context={
                "queue": queue_name,
                "exchange": exchange,
                "routing_key": routing_key,
                "message_size": len(body)
            })

            self._close()

            return {
                'status': True,
                'message': f'Mensagem publicada com sucesso na fila {queue_name}'
            }

        except Exception as e:
            self.logger.error("Erro ao publicar mensagem imediata", context={
                "queue": queue_name,
                "erro": str(e)
            })
            self._close()
            return {
                'status': False,
                'message': 'Erro ao publicar mensagem',
                'context': {"queue": queue_name, "erro": str(e)}
            }

    def publish_delayed(self, queue_name, message, delay_ms, exchange='', properties=None):
        """
        Publica uma mensagem com delay (usando TTL + Dead Letter Exchange).

        Args:
            queue_name: nome da fila de destino final
            message: mensagem a ser enviada (dict sera convertido para JSON)
            delay_ms: tempo de delay em milissegundos
            exchange: nome do exchange (default: '' para exchange padrao)
            properties: propriedades adicionais da mensagem (pika.BasicProperties)

        Returns:
            dict com status e message
        """
        try:
            if not self._connect():
                return {
                    'status': False,
                    'message': 'Erro ao conectar ao RabbitMQ'
                }

            # Configura filas para delay
            delay_queue_name = self._setup_delayed_queue(queue_name, delay_ms)

            # Converte mensagem para JSON se for dict
            if isinstance(message, dict):
                body = json.dumps(message, ensure_ascii=False)
            else:
                body = str(message)

            # Define propriedades padrao
            if properties is None:
                properties = pika.BasicProperties(
                    delivery_mode=2,  # Mensagem persistente
                    content_type='application/json'
                )

            # Publica na fila temporaria com delay
            self._channel.basic_publish(
                exchange=exchange,
                routing_key=delay_queue_name,
                body=body,
                properties=properties
            )

            self.logger.info("Mensagem publicada com delay", context={
                "queue_final": queue_name,
                "queue_delay": delay_queue_name,
                "delay_ms": delay_ms,
                "delay_seconds": delay_ms / 1000,
                "message_size": len(body)
            })

            self._close()

            return {
                'status': True,
                'message': f'Mensagem publicada com delay de {delay_ms}ms na fila {queue_name}'
            }

        except Exception as e:
            self.logger.error("Erro ao publicar mensagem com delay", context={
                "queue": queue_name,
                "delay_ms": delay_ms,
                "erro": str(e)
            })
            self._close()
            return {
                'status': False,
                'message': 'Erro ao publicar mensagem com delay',
                'context': {"queue": queue_name, "delay_ms": delay_ms, "erro": str(e)}
            }

    def validate_queue_and_exchange(self, queue_name, exchange=''):
        """
        Valida se a queue e o exchange existem no RabbitMQ.

        Args:
            queue_name: nome da fila a ser validada
            exchange: nome do exchange a ser validado (default: '' para exchange padrao)

        Returns:
            dict com status, queue_exists, exchange_exists e message
        """
        try:
            if not self._connect():
                return {
                    'status': False,
                    'queue_exists': False,
                    'exchange_exists': False,
                    'message': 'Erro ao conectar ao RabbitMQ'
                }

            queue_exists = False
            exchange_exists = False
            validation_errors = []

            # Valida se a queue existe
            try:
                self._channel.queue_declare(queue=queue_name, passive=True)
                queue_exists = True
                self.logger.info("Queue validada com sucesso", context={
                    "queue": queue_name
                })
            except Exception as e:
                validation_errors.append(f"Queue '{queue_name}' nao existe: {str(e)}")
                self.logger.warning("Queue nao encontrada", context={
                    "queue": queue_name,
                    "erro": str(e)
                })

            # Valida se o exchange existe (apenas se nao for o exchange padrao)
            if exchange != '':
                try:
                    self._channel.exchange_declare(exchange=exchange, passive=True)
                    exchange_exists = True
                    self.logger.info("Exchange validado com sucesso", context={
                        "exchange": exchange
                    })
                except Exception as e:
                    validation_errors.append(f"Exchange '{exchange}' nao existe: {str(e)}")
                    self.logger.warning("Exchange nao encontrado", context={
                        "exchange": exchange,
                        "erro": str(e)
                    })
            else:
                # Exchange padrao sempre existe
                exchange_exists = True

            self._close()

            # Monta a resposta
            if queue_exists and exchange_exists:
                return {
                    'status': True,
                    'queue_exists': True,
                    'exchange_exists': True,
                    'message': 'Queue e exchange validados com sucesso'
                }
            else:
                return {
                    'status': False,
                    'queue_exists': queue_exists,
                    'exchange_exists': exchange_exists,
                    'message': '; '.join(validation_errors)
                }

        except Exception as e:
            self.logger.error("Erro ao validar queue e exchange", context={
                "queue": queue_name,
                "exchange": exchange,
                "erro": str(e)
            })
            self._close()
            return {
                'status': False,
                'queue_exists': False,
                'exchange_exists': False,
                'message': f'Erro ao validar: {str(e)}'
            }

    def publish(self, queue_name, message, delay_ms=0, exchange='', routing_key=None, properties=None):
        """
        Publica uma mensagem com opcao de delay ou imediato.

        Args:
            queue_name: nome da fila
            message: mensagem a ser enviada (dict sera convertido para JSON)
            delay_ms: tempo de delay em milissegundos (0 = imediato)
            exchange: nome do exchange (default: '' para exchange padrao)
            routing_key: routing key (default: queue_name, usado apenas para envio imediato)
            properties: propriedades adicionais da mensagem (pika.BasicProperties)

        Returns:
            dict com status e message
        """
        if delay_ms > 0:
            return self.publish_delayed(queue_name, message, delay_ms, exchange, properties)
        else:
            return self.publish_immediate(queue_name, message, exchange, routing_key, properties)
