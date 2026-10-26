"""Шаг 1. Получатель: «отправляет письмо» по каждому заказу из очереди orders.

Запуск:  python worker.py   (остановка: Ctrl+C)
"""

import asyncio
import json
import sys

import aio_pika
from aio_pika.exceptions import AMQPConnectionError

# Адрес RabbitMQ: «протокол://логин:пароль@хост/». Тот же, что у отправителя.
RABBIT_URL = "amqp://guest:guest@localhost/"

# Имя очереди должно совпадать с именем в send_order.py: по нему worker находит заказы.
QUEUE_NAME = "orders"


async def main() -> None:
    # 1. Соединение с RabbitMQ: одно на всю программу, как и у отправителя
    connection = await aio_pika.connect_robust(RABBIT_URL)

    async with connection:
        # 2. Канал: «линия» внутри соединения, через неё идут все команды
        channel = await connection.channel()

        # 3. Объявляем ту же очередь: вдруг worker запустился раньше отправителя.
        #    durable=True как у отправителя: параметры очереди должны совпадать,
        #    иначе RabbitMQ вернёт ошибку PRECONDITION_FAILED.
        #    В RabbitMQ 4.3+ временные очереди запрещены, поэтому durable=True обязателен.
        #    queue это объект очереди, из него мы будем читать сообщения
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)

        print("Жду заказы. Ctrl+C для выхода")

        # 4. Читаем сообщения. queue.iterator() это бесконечный цикл «жди следующее
        #    сообщение»: скрипт работает, пока ты его не остановишь.
        #    no_ack=True: «считай сообщение обработанным сразу, как только получил».
        #    Это временное упрощение. Что такое ack и чем опасен этот режим, будет на шаге 2
        async with queue.iterator(no_ack=True) as messages:
            async for message in messages:
                # message.body это байты. Превращаем обратно: байты -> JSON-строка -> словарь.
                # Это то же самое, что делал отправитель, только в обратном порядке
                order = json.loads(message.body.decode())

                # Склеиваем список товаров в одну строку: "Кофе в зёрнах, Фильтры V60"
                items = ", ".join(order["items"])

                # В настоящем проекте здесь была бы отправка письма, а у нас просто print

                print(f"  ⏳ Начинаю обработку заказа #{order['order_id']}...")

                # Пауза для демонстрации: у тебя есть 4 секунды нажать Ctrl+C.
                # Заметь: заказ уже удалён из очереди (no_ack=True удалил при получении).
                # Ctrl+C сейчас — и заказ пропадёт навсегда. Проверь в веб-интерфейсе: Ready=0
                await asyncio.sleep(4)

                print(
                    f"  Письмо отправлено на {order['email']}: "
                    f"заказ #{order['order_id']}, товары: {items}"
                )



if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Ctrl+C: останавливаем скрипт спокойно, без длинного traceback
        print("\nWorker остановлен")
    except (AMQPConnectionError, OSError):
        print("Не удалось подключиться к RabbitMQ. Он запущен? Проверь: docker ps")
        sys.exit(1)