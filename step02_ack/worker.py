"""Шаг 2. Получатель с ручным подтверждением обработки (ack).

Запуск:  python worker.py   (остановка: Ctrl+C)

Главное изменение по сравнению с шагом 1:
  было:  queue.iterator(no_ack=True)  → RabbitMQ удаляет сообщение сразу при отдаче
  стало: queue.iterator()             → RabbitMQ ждёт ack перед удалением
         async with message.process() → ack отправляется автоматически в конце блока,
                                        а если внутри случилась ошибка — nack,
                                        и сообщение возвращается в очередь.
"""

import asyncio
import json
import sys

import aio_pika
from aio_pika.exceptions import AMQPConnectionError

RABBIT_URL = "amqp://guest:guest@localhost/"
QUEUE_NAME = "orders"

# Сколько секунд «готовим кофе» — намеренно долго, чтобы успеть нажать Ctrl+C в опытах
PROCESS_TIME = 3


async def main() -> None:
    # 1. Соединение с RabbitMQ
    connection = await aio_pika.connect_robust(RABBIT_URL)

    async with connection:
        # 2. Канал
        channel = await connection.channel()

        # 3. Объявляем ту же очередь. Параметры должны совпадать с send_order.py:
        #    если worker объявит очередь с другими параметрами, RabbitMQ вернёт
        #    ошибку PRECONDITION_FAILED и закроет соединение.
        queue = await channel.declare_queue(QUEUE_NAME, durable=True)

        print("Жду заказы. Ctrl+C для выхода")

        # 4. Читаем сообщения БЕЗ no_ack:
        #    теперь RabbitMQ считает сообщение «в процессе» (Unacked), пока мы не подтвердим.
        async with queue.iterator() as messages:
            async for message in messages:

                # message.process() управляет ack автоматически:
                #   • блок завершился без ошибки → ack()  → RabbitMQ удаляет сообщение
                #   • внутри блока исключение   → nack()  → что дальше зависит от requeue:
                #       requeue=True  → сообщение возвращается в очередь  ✅ (наш вариант)
                #       requeue=False → сообщение отбрасывается (для DLQ, шаг 5)
                async with message.process(requeue=True):

                    order = json.loads(message.body.decode())
                    items = ", ".join(order["items"])

                    print(f"  ⏳ Начинаю обработку заказа #{order['order_id']}...")

                    # Имитируем «приготовление кофе» — {PROCESS_TIME} секунды.
                    # За это время ты успеешь нажать Ctrl+C в опыте 2.
                    await asyncio.sleep(PROCESS_TIME)

                    print(
                        f"  ✅ Письмо отправлено на {order['email']}: "
                        f"заказ #{order['order_id']}, товары: {items}"
                    )
                # ← сюда программа дошла без ошибки → ack отправлен → заказ удалён из очереди


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Ctrl+C: если нажали во время обработки — message.process() поймает прерывание,
        # отправит nack, и сообщение вернётся в очередь. Проверь это в опыте 2.
        print("\nWorker остановлен")
    except (AMQPConnectionError, OSError):
        print("Не удалось подключиться к RabbitMQ. Он запущен? Проверь: docker ps")
        sys.exit(1)
