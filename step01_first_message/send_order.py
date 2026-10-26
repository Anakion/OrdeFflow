"""Шаг 1. Отправитель: «создаёт заказ» и кладёт его в очередь orders.

Запуск:  python send_order.py [номер_заказа]
"""

import asyncio
import json
import sys

import aio_pika
from aio_pika.exceptions import AMQPConnectionError

# Адрес RabbitMQ. Читается как «протокол://логин:пароль@хост/».
# guest:guest это логин и пароль по умолчанию, localhost значит «RabbitMQ на этом же компьютере».
# Вынесен в константу, чтобы при смене адреса править одну строку, а не весь код.
RABBIT_URL = "amqp://guest:guest@localhost/"

# Имя очереди. Отправитель и получатель должны использовать ОДНО И ТО ЖЕ имя:
# по нему они находят друг друга, больше они ничего друг о друге не знают.
QUEUE_NAME = "orders"


async def main() -> None:
    # Номер заказа берём из командной строки: `python send_order.py 5` даст 5.
    # Если номер не передали, будет 1. Нужен, чтобы отличать заказы друг от друга в опытах.
    order_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    # Сам заказ: обычный словарь Python. Это то, что мы хотим доставить получателю.
    # Он увидит те же поля, поэтому worker берёт из него email, items и order_id.
    order = {
        "order_id": order_id,                           # номер заказа
        "email": "anna@example.com",                    # куда «отправить письмо»
        "items": ["Кофе в зёрнах", "Фильтры V60"],      # что заказали
    }

    # 1. Соединение: одно TCP-подключение к RabbitMQ (дорогое, поэтому одно на программу)
    connection = await aio_pika.connect_robust(RABBIT_URL)

    async with connection:
        # 2. Канал: «линия» внутри соединения, через неё идут все команды
        channel = await connection.channel()

        # 3. Очередь: создаётся, если её ещё нет. Повторный вызов безопасен.
        #    durable=True: очередь переживает перезапуск RabbitMQ. В RabbitMQ 4.3+ без этого
        #    объявить очередь нельзя: брокер закроет соединение с ошибкой.
        #    Параметры очереди у отправителя и получателя должны совпадать.
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