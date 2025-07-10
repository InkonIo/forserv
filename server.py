# -*- coding: utf-8 -*-
import os
from flask import Flask, request, jsonify
import psycopg2
from datetime import datetime, timedelta

app = Flask(__name__)

# --- НАСТРОЙКИ POSTGRESQL НА RAILWAY ---
# Эти переменные окружения автоматически предоставляются Railway вашему приложению.
# НЕ МЕНЯЙТЕ ИХ ЗДЕСЬ. Railway сам заполнит их значения.
PG_HOST = os.getenv("PGHOST")
PG_DATABASE = os.getenv("PGDATABASE")
PG_USER = os.getenv("PGUSER")
PG_PASSWORD = os.getenv("PGPASSWORD")
PG_PORT = os.getenv("PGPORT")

# Функция для подключения к PostgreSQL
def connect_db():
    conn = None
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            database=PG_DATABASE,
            user=PG_USER,
            password=PG_PASSWORD,
            port=PG_PORT,
            sslmode="require" # Обязательно для подключения к Railway PostgreSQL
        )
        print("Successfully connected to Railway PostgreSQL database!")
        return conn
    except Exception as e:
        print(f"Error connecting to Railway PostgreSQL: {e}")
        # Добавим больше деталей в лог, если переменные окружения не установлены
        if not PG_HOST: print("PGHOST environment variable is not set.")
        if not PG_DATABASE: print("PGDATABASE environment variable is not set.")
        if not PG_USER: print("PGUSER environment variable is not set.")
        if not PG_PASSWORD: print("PGPASSWORD environment variable is not set.")
        if not PG_PORT: print("PGPORT environment variable is not set.")
        return None

# Определение маршрута для приема данных с датчика (POST-запросы от NodeMCU)
@app.route('/data', methods=['POST'])
def receive_data():
    if request.is_json:
        data = request.get_json()
        sensor_id = data.get("sensor_id")
        water_percentage = data.get("water_percentage")
        current_time = datetime.now() # Текущее время получения данных

        if sensor_id and water_percentage is not None:
            conn = connect_db()
            if conn:
                try:
                    cursor = conn.cursor()

                    # 1. Определяем временное окно для поиска существующей записи
                    # Ищем записи, которые были созданы/обновлены в течение последней минуты
                    one_minute_ago = current_time - timedelta(minutes=1)
                    
                    # 2. Попытка найти последнюю запись для этого датчика в пределах временного окна
                    select_query = """
                    SELECT id FROM sensor_data
                    WHERE sensor_id = %s AND timestamp >= %s
                    ORDER BY timestamp DESC
                    LIMIT 1;
                    """
                    cursor.execute(select_query, (sensor_id, one_minute_ago))
                    existing_record = cursor.fetchone()

                    if existing_record:
                        # 3. Если запись найдена (в пределах последней минуты), обновляем ее
                        record_id = existing_record[0]
                        update_query = """
                        UPDATE sensor_data
                        SET water_percentage = %s, timestamp = %s
                        WHERE id = %s;
                        """
                        cursor.execute(update_query, (water_percentage, current_time, record_id))
                        # ИСПРАВЛЕНО: Убраны лишние фигурные скобки из f-строки
                        print(f"Updated record ID {record_id} for Sensor ID: {sensor_id}, Water: {water_percentage}%, Timestamp: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    else:
                        # 4. Если запись не найдена (не было обновлений в последнюю минуту), вставляем новую
                        insert_query = """
                        INSERT INTO sensor_data (sensor_id, water_percentage, timestamp)
                        VALUES (%s, %s, %s);
                        """
                        cursor.execute(insert_query, (sensor_id, water_percentage, current_time))
                        # ИСПРАВЛЕНО: Убраны лишние фигурные скобки из f-строки
                        print(f"Inserted new record for Sensor ID: {sensor_id}, Water: {water_percentage}%, Timestamp: {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    conn.commit() # Подтверждаем изменения в базе данных
                    return jsonify({"message": "Data received and processed (upserted) in DB"}), 200

                except Exception as e:
                    print(f"Error processing data in database: {e}")
                    conn.rollback() # Откатываем изменения, если произошла ошибка
                    return jsonify({"error": "Failed to process data in DB"}), 500
                finally:
                    # Закрываем курсор и соединение с БД в любом случае
                    if cursor:
                        cursor.close()
                    if conn:
                        conn.close()
            else:
                return jsonify({"error": "Database connection failed"}), 500
        else:
            print("Received invalid data format. Missing sensor_id or water_percentage.")
            return jsonify({"error": "Invalid data format. Missing sensor_id or water_percentage."}), 400
    else:
        print("Request was not JSON.")
        return jsonify({"error": "Request must be JSON"}), 400

# Главная точка входа для Flask-приложения
if __name__ == '__main__':
    # Railway автоматически устанавливает порт для Flask-приложения в переменной окружения PORT.
    # Используем его, иначе по умолчанию 5000 для локального запуска.
    port = int(os.environ.get("PORT", 5000))
    # Запускаем Flask-приложение, доступное извне (host='0.0.0.0')
    app.run(host='0.0.0.0', port=port, debug=False) # debug=False для продакшн
