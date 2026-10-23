"""Шаг 1. Отправитель: «создаёт заказ» и кладёт его в очередь orders.

Запуск:  python send_order.py [номер_заказа]
"""

import asyncio
import json
import sys

import aio_pika
from aio_pika.exceptions import AMQPConnectionError

RABBIT_URL = "amqp://guest:guest@localhost/"  # протокол://логин:пароль@хост/
QUEUE_NAME = "orders"


async def main() -> None:
    order_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    order = {
        "order_id": order_id,
        "email": "anna@example.com",
        "items": ["Кофе в зёрнах", "Фильтры V60"],
    }

    # 1. Соединение: одно TCP-подключение к RabbitMQ (дорогое, поэтому одно на программу)
    connection = await aio_pika.connect_robust(RABBIT_URL)

    async with connection:
        # 2. Канал: «линия» внутри соединения, через неё идут все команды
        channel = await connection.channel()

        # 3. Очередь: создаётся, если её ещё нет. Повторный вызов безопасен
        #    durable=True — обязательно для RabbitMQ 4 (временные очереди запрещены)
        await channel.declare_queue(QUEUE_NAME, durable=True)

        # 4. Тело сообщения в RabbitMQ всегда байты: словарь -> JSON-строка -> байты
        body = json.dumps(order, ensure_ascii=False).encode()
        message = aio_pika.Message(body=body)

        # 5. Пока читаем так: «положить сообщение в очередь с именем orders».
        #    Что такое exchange, разберём на шаге 4
        await channel.default_exchange.publish(message, routing_key=QUEUE_NAME)

        print(f"Заказ #{order_id} отправлен")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (AMQPConnectionError, OSError):
        print("Не удалось подключиться к RabbitMQ. Он запущен? Проверь: docker ps")
        sys.exit(1)
