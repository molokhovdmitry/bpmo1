import pika
import json
import os
import csv
import pandas as pd
import numpy as np

try:
    # Создаём подключение к серверу на локальном хосте
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()

    # Объявляем очередь y_true
    channel.queue_declare(queue='y_true')
    # Объявляем очередь y_pred
    channel.queue_declare(queue='y_pred')

    # Создаём функцию callback для обработки данных из очереди
    def callback(ch, method, properties, body):
        message = json.loads(body)
        message_id = message['id']
        value = float(message['body'])

        # Логирование полученных сообщений
        answer_string = (
            f'Из очереди {method.routing_key} '
            f'получено значение {value} '
            f'с message_id: {message_id}'
        )
        print(answer_string)
        with open('./logs/labels_log.txt', 'a') as log:
            log.write(answer_string + '\n')

        # Логирование метрик
        path = './logs/metric_log.csv'
        if not os.path.exists(path):
            headers = ['id', 'y_true', 'y_pred', 'absolute_error']
            with open(path, mode='w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(headers)

        df = pd.read_csv(path, dtype={'id': str})
        if df['id'].str.contains(message_id).any():
            # Обновить существующую строку
            df.loc[df['id'] == message_id, method.routing_key] = value
            y_true = df.loc[df['id'] == message_id, 'y_true'].iloc[0]
            y_pred = df.loc[df['id'] == message_id, 'y_pred'].iloc[0]
            if pd.notna(y_true) and pd.notna(y_pred):
                # Расчитать ошибку
                error = np.abs(y_true - y_pred)
                df.loc[df['id'] == message_id, 'absolute_error'] = error
        else:
            # Добавить новую строку
            if method.routing_key == 'y_true':
                df.loc[len(df)] = [message_id, value, np.nan, np.nan]
            else:
                df.loc[len(df)] = [message_id, np.nan, value, np.nan]

        df.to_csv(path, index=False)

    # Извлекаем сообщение из очереди y_true
    channel.basic_consume(
        queue='y_true',
        on_message_callback=callback,
        auto_ack=True
    )
    # Извлекаем сообщение из очереди y_pred
    channel.basic_consume(
        queue='y_pred',
        on_message_callback=callback,
        auto_ack=True
    )

    # Запускаем режим ожидания прихода сообщений
    print('...Ожидание сообщений, для выхода нажмите CTRL+C')
    channel.start_consuming()
except Exception as e:
    print(e)
    print('Не удалось подключиться к очереди')
