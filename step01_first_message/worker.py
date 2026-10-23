"""Шаг 1. Получатель: «отправляет письмо» по каждому заказу из очереди orders.

Запуск:  python worker.py   (остановка: Ctrl+C)
"""

import asyncio
import json
import sys

import aio_pika
from aio_pika.exceptions import AMQPConnectionError

RABBIT_URL = "amqp://guest:guest@localhost/"
QUEUE_NAME = "orders"


async def main() -> None:
    connection = await aio_pika.connect_robust(RABBIT_URL)

    async with connection:
        channel = await connection.channel()

        # Объявляем ту же очередь: вдруг worker запустился раньше отправителя
        #    durable=True — обязательно для RabbitMQ 4 (временные очереди запрещены)
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)

        print("Жду заказы. Ctrl+C для выхода")

        # no_ack=True: «считай сообщение обработанным сразу, как только получил».
        # Это временное упрощение. Что такое ack и чем опасен этот режим, будет на шаге 2
        async with queue.iterator(no_ack=True) as messages:
            async for message in messages:
                order = json.loads(message.body.decode())
                items = ", ".join(order["items"])
                print(
                    f"Письмо отправлено на {order['email']}: "
                    f"заказ #{order['order_id']}, товары: {items}"
                )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nWorker остановлен")
    except (AMQPConnectionError, OSError):
        print("Не удалось подключиться к RabbitMQ. Он запущен? Проверь: docker ps")
        sys.exit(1)
