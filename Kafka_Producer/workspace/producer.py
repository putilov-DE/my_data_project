import csv
import json
import time
from datetime import datetime
from confluent_kafka import Producer

# Настройки подключения
KAFKA_BROKER = 'kafka-server:9092'
TOPIC_NAME = 'tweets'
# Путь к файлу внутри контейнера (как мы прописали в Dockerfile)
FILE_PATH = '/app/tweets.csv'
# Скорость: 12 сообщений в секунду (входит в диапазон 10-15)
MESSAGES_PER_SECOND = 12

# Инициализация Продюсера
conf = {'bootstrap.servers': KAFKA_BROKER}
producer = Producer(conf)


def delivery_report(err, msg):
    """ Отчет о доставке сообщения """
    if err is not None:
        print(f"Ошибка доставки: {err}")
    else:
        # Печатаем только время для чистоты логов
        now = datetime.now().strftime('%H:%M:%S')
        print(f"[{now}] Сообщение отправлено в топик {msg.topic()}")


def main():
    print(f"Запуск Продюсера. Чтение файла: {FILE_PATH}")

    try:
        with open(FILE_PATH, mode='r', encoding='utf-8') as file:
            reader = csv.DictReader(file)

            for row in reader:
                # Формируем JSON с нужными полями и текущим временем
                tweet_payload = {
                    "author_id": row.get("author_id", "unknown"),
                    "text": row.get("text", ""),
                    "created_at": datetime.now().isoformat()
                }

                # Отправка в Kafka
                producer.produce(
                    topic=TOPIC_NAME,
                    value=json.dumps(tweet_payload).encode('utf-8'),
                    callback=delivery_report
                )

                # Опрашиваем очередь событий для вызова callback
                producer.poll(0)

                # Контроль скорости (1 сек / 12 сообщений)
                time.sleep(1 / MESSAGES_PER_SECOND)

    except FileNotFoundError:
        print(f"Критическая ошибка: Файл {FILE_PATH} не найден внутри контейнера!")
    except KeyboardInterrupt:
        print("\nОстановка пользователем...")
    finally:
        # Ждем завершения отправки всех оставшихся сообщений
        producer.flush()
        print("Работа Продюсера завершена.")


if __name__ == "__main__":
    main()