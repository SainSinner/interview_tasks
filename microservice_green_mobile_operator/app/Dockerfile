# Возьмем за основу данный образ
# Данная команда установит уже готвоый образ с python:3.13.7
# После чего поверх него установит дополнительные 
# команды которые мы укажем дальше
FROM python:3.13.7

# Устанавливаем рабочую директорию
WORKDIR /app
ENV PYTHONPATH=/app

# Устанавливаем PYTHONPATH
# ENV PYTHONPATH=/root/airflow/plugins

# Ставим библиотеки которые нам понадобятся
RUN pip install --upgrade pip
RUN pip install fastapi==0.118.0
RUN pip install uvicorn==0.37.0
RUN pip install psycopg2-binary==2.9.10
RUN pip install SQLAlchemy==2.0.43
RUN pip install clickhouse-driver==0.2.9
RUN pip install pandas==2.3.3

# Копируем приложение
COPY . .

# Стартуем приложение

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]