"""Шаг 2. Отправитель: тот же кассир, что на шаге 1.

Запуск:  python send_order.py [номер_заказа]

На стороне отправителя ничего не изменилось — это важный урок:
Producer не знает и не должен знать, как Consumer обрабатывает сообщения.
Изменения на шаге 2 только в worker.py.
"""

import asyncio
import json
import sys

import aio_pika
from aio_pika.exceptions import AMQPConnectionError

# Адрес RabbitMQ. Читается как «протокол://логин:пароль@хост/».
# guest:guest — логин и пароль по умолчанию, localhost — RabbitMQ на этом же компьютере.
RABBIT_URL = "amqp://guest:guest@localhost/"

# Имя очереди. Отправитель и получатель должны использовать одно и то же имя:
# по нему они находят общую «стойку заказов».
QUEUE_NAME = "orders"


async def main() -> None:
    # Номер заказа берём из аргумента командной строки: python send_order.py 5 → 5.
    # Если не передали — будет 1.
    order_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    order = {
        "order_id": order_id,           # номер заказа
        "email": "anna@example.com",    # куда «отправить письмо»
        "items": ["Кофе в зёрнах", "Фильтры V60"],  # что заказали
    }

    # 1. Соединение: одно TCP-подключение к RabbitMQ (дорогое, поэтому одно на программу)
    connection = await aio_pika.connect_robust(RABBIT_URL)

    async with connection:
        # 2. Канал: «линия» внутри соединения, через неё идут все команды
        channel = await connection.channel()

        # 3. Объявляем очередь. durable=True — обязательно для RabbitMQ 4.
        #    Параметры должны совпадать с теми, что в worker.py.
        await channel.declare_queue(QUEUE_NAME, durable=True)

        # 4. Тело сообщения всегда байты: словарь → JSON-строка → байты
        body = json.dumps(order, ensure_ascii=False).encode()
        message = aio_pika.Message(body=body)

        # 5. Кладём сообщение в очередь через default_exchange
        await channel.default_exchange.publish(message, routing_key=QUEUE_NAME)

        print(f"Заказ #{order_id} отправлен")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (AMQPConnectionError, OSError):
        print("Не удалось подключиться к RabbitMQ. Он запущен? Проверь: docker ps")
        sys.exit(1)
