-- region DDL создания схемы customer с заполнением ее данными
-- region Создаем схему customer
DROP SCHEMA IF EXISTS customer;

CREATE SCHEMA customer;

-- endregion
-- region Создаем таблицу city
DROP TABLE IF EXISTS customer.city;

CREATE TABLE customer.city
(
    city_id serial PRIMARY KEY,
    name    text NOT NULL UNIQUE
);

-- endregion
-- region Заполняем таблицу городов
TRUNCATE TABLE customer.city;

INSERT INTO
    customer.city (name)
VALUES
    (
      'Москва')
  , (
      'Санкт-Петербург')
  , (
      'Новосибирск')
  , (
      'Екатеринбург')
  , (
      'Казань');

-- endregion
-- region Создаем таблицу customer
DROP TABLE IF EXISTS customer.customer;

CREATE TABLE customer.customer
(
    customer_id serial PRIMARY KEY,
    city_id     int REFERENCES customer.city (city_id) NOT NULL,
    first_name  text                                   NOT NULL,
    last_name   text                                   NOT NULL,
    UNIQUE (city_id, first_name, last_name)
);

-- endregion
-- region Заполняем таблицу customer
DO
$$
    DECLARE
        -- variable_0 - коэффициент на который будем умножать единицы при формировании тестовых данных, для масштабирования
        variable_0 integer := 10;
    BEGIN
        FOR i IN 1..5
            LOOP
                RAISE NOTICE 'Current value: %', i;
                INSERT INTO
                    customer.customer (
                                        city_id
                                      , first_name
                                      , last_name)
                SELECT
                    (i)::int                  AS city_id -- Случайный city_id от 1 до 5
                  , md5(random()::text)::text AS first_name -- Случайное имя
                  , md5(random()::text)::text AS last_name -- Случайная фамилия
                FROM
                    generate_series(1,
                                    CASE
                                        WHEN i = 1
                                            THEN 80 * variable_0
                                        WHEN i = 2
                                            THEN 10 * variable_0
                                        WHEN i = 3
                                            THEN 6 * variable_0
                                        WHEN i = 4
                                            THEN 3 * variable_0
                                        WHEN i = 5
                                            THEN 1 * variable_0
                                    END
                    );
            END LOOP;
    END
$$;

-- endregion
-- region Создаем таблицу metric
DROP TABLE IF EXISTS customer.metric;

CREATE TABLE customer.metric
(
    metric_id  serial PRIMARY KEY,
    name       text NOT NULL UNIQUE,
    explain    text NOT NULL,
    data_type  text NOT NULL,
    table_name text NOT NULL
);

-- endregion
-- Делим на количество метрик на (ограничеие Posgresql в 1600 столбцов - 10), лучше конечно было бы вынести наиболее часто запрашиваемые метрики в отдельную, например 3 или 4 таблицу, но здесь об этом ничего не сказано
-- В будущем можно было бы улучшить момент описанный выше
-- region Заполняем таблицу metric
DO
$$
    DECLARE
        -- variable_metric_quantity - количесвто метрик
        variable_metric_quantity             integer := 1700;
        variable_limitation_columns_quantity integer := :variable_limitation;
    BEGIN
        FOR i IN 1..variable_metric_quantity
            LOOP
            -- RAISE NOTICE 'Current value: %', i;
            -- RAISE NOTICE 'Current value: %', concat('metric_', variable_metric_quantity::text);
                INSERT INTO
                    customer.metric (
                                      name
                                    , explain
                                    , data_type
                                    , table_name)
                VALUES
                    (
                      concat('metric_', i::text)
                    , 'метрика'
                    , 'numeric'
                    , concat('customer_metric_', FLOOR(i / variable_limitation_columns_quantity)::text));
            END LOOP;
    END
$$;

-- endregion
-- Обычные таблицы для сравнения быстродейтсвия
-- region Создаем таблицы customer_metric_default
DO
$$
    DECLARE
        -- variable_metric_quantity - количесвто метрик
        variable_metric_table_quantity       numeric;
        variable_limitation_columns_quantity integer := :variable_limitation;
        variable_column_index                integer := 1;
        variable_sql_query                   text; -- Динамический SQL-запрос
    BEGIN
        SELECT
            CEIL(count(*)::numeric / variable_limitation_columns_quantity) AS metric_quantity
        INTO variable_metric_table_quantity
        FROM
            customer.metric;
        RAISE NOTICE 'variable_metric_quantity: %', variable_metric_table_quantity;
        FOR i IN 1..variable_metric_table_quantity
            LOOP
                SELECT
                    concat(
                            'CREATE TABLE customer.customer_metric_'
                        , i::text
                        , '_default'
                        , ' (
                        customer_id int REFERENCES customer.customer (customer_id) NOT NULL,
                        metric_timestamp TIMESTAMP NOT NULL, '
                        , string_agg(
                                    name::text || ' ' || data_type::text, ',' ORDER BY metric_id)
                        , ') PARTITION BY RANGE (metric_timestamp);')
                INTO
                    variable_sql_query
                FROM
                    customer.metric
                WHERE
                      metric_id >= variable_column_index
                  AND metric_id < (variable_column_index + variable_limitation_columns_quantity);

                variable_column_index := variable_column_index + variable_limitation_columns_quantity;

                EXECUTE variable_sql_query;
                -- RAISE NOTICE 'variable_sql_query: %', variable_sql_query;
            END LOOP;
    END
$$;

-- endregion
-- region Создаем индексы для customer_metric_default
DO
$$
    DECLARE
        -- variable_metric_quantity - количесвто метрик
        variable_metric_table_quantity       numeric;
        variable_limitation_columns_quantity integer := :variable_limitation;
        variable_column_index                integer := 1;
        variable_sql_query                   text; -- Динамический SQL-запрос
        variable_sql_query_1                 text; -- Динамический SQL-запрос
        variable_sql_query_2                 text; -- Динамический SQL-запрос
    BEGIN
        SELECT
            CEIL(count(*)::numeric / variable_limitation_columns_quantity) AS metric_quantity
        INTO variable_metric_table_quantity
        FROM
            customer.metric;
        FOR i IN 1..variable_metric_table_quantity
            LOOP
                SELECT
                    concat(
                            'CREATE INDEX customer_metric_idx_customer_id_metric_timestamp_' || i::text ||
                            ' ON customer.customer_metric_' || i::text || '_default (customer_id, metric_timestamp);'
                    )
                INTO
                    variable_sql_query;
                SELECT
                    concat(
                            'CREATE INDEX customer_metric_idx__timestamp_day_' || i::text ||
                            ' ON customer.customer_metric_' || i::text ||
                            '_default (date_trunc(''day'', metric_timestamp));'
                    )
                INTO
                    variable_sql_query_1;
                SELECT
                    concat(
                            'CREATE INDEX customer_metric_idx__timestamp_week_' || i::text ||
                            ' ON customer.customer_metric_' || i::text ||
                            '_default (date_trunc(''week'', metric_timestamp));'
                    )
                INTO
                    variable_sql_query_2;
                -- RAISE NOTICE 'variable_sql_query: %', variable_sql_query;
                EXECUTE variable_sql_query;
                EXECUTE variable_sql_query_1;
                EXECUTE variable_sql_query_2;
            END LOOP;
    END
$$;

-- endregion
-- region Функция для создания партиций по месяцам customer.create_monthly_partition_if_not_exists
DROP FUNCTION IF EXISTS customer.create_monthly_partition_if_not_exists(timestamp, text);

CREATE OR REPLACE FUNCTION customer.create_monthly_partition_if_not_exists(variable_metric_date timestamp, variable_table_name text)
    RETURNS void
AS
$$
DECLARE
    variable_partition_name  text;
    variable_partition_start date;
    variable_partition_end   date;
BEGIN
    -- Формируем имя партиции
    variable_partition_name := variable_table_name || '_' || to_char(variable_metric_date, 'YYYYMM');

    -- Определяем начало и конец месяца
    variable_partition_start := date_trunc('month', variable_metric_date);
    variable_partition_end := variable_partition_start + INTERVAL '1 month';

    -- Создаём партицию, если она ещё не существует
    IF NOT EXISTS
        (SELECT
             1
         FROM
             pg_class
         WHERE
             relname = variable_partition_name
        )
    THEN
        EXECUTE format('
            CREATE TABLE %I
            PARTITION OF customer.%I
            FOR VALUES FROM (%L) TO (%L);
        ', variable_partition_name, variable_table_name, variable_partition_start, variable_partition_end);
        END IF;
END;
$$ LANGUAGE plpgsql;
-- endregion
-- TODO: Правильнее было бы сделать триггер при вставке данных, но уже много времени потратил и необходимо отправить ответ
-- region Партиции по месяцам на 4 года вперед для таблицы
DO
$$
    DECLARE
        variable_start_date   date := date_trunc('year', CURRENT_DATE) - INTERVAL '1 years'; -- Начало с прошлого года
        variable_end_date     date := variable_start_date + INTERVAL '4 years'; -- Конец через 10 лет
        variable_current_date date := variable_start_date;
        variable_table_name   text := 'customer_metric_2_default';
    BEGIN
        -- Цикл для создания партиций
        WHILE variable_current_date < variable_end_date
            LOOP
                -- Вызываем функцию для создания партиции
                PERFORM customer.create_monthly_partition_if_not_exists(variable_current_date,
                                                                        variable_table_name);
                -- Переходим к следующему месяцу
                variable_current_date := variable_current_date + INTERVAL '1 month';
            END LOOP;
    END;
$$;
-- endregion
-- region Удаляем таблицы customer_metric_default
DO
$$
    DECLARE
        -- variable_metric_quantity - количесвто метрик
        variable_metric_table_quantity       numeric;
        variable_limitation_columns_quantity integer := :variable_limitation;
        variable_column_index                integer := 1;
        variable_sql_query                   text; -- Динамический SQL-запрос
    BEGIN
        SELECT
            CEIL(count(*)::numeric / variable_limitation_columns_quantity) AS metric_quantity
        INTO variable_metric_table_quantity
        FROM
            customer.metric;
        RAISE NOTICE 'variable_metric_quantity: %', variable_metric_table_quantity;
        FOR i IN 1..variable_metric_table_quantity
            LOOP
                SELECT
                    concat(
                            'DROP TABLE IF EXISTS customer.customer_metric_'
                        , i::text
                        , '_default'
                        , ';')
                INTO
                    variable_sql_query;
                EXECUTE variable_sql_query;
            END LOOP;
    END
$$;

-- endregion
-- Заполняем таблицы чем-нибудь
-- foreign
-- default
-- Заполнение таблицы foreign в 2-3 раза дольше чем обычной
-- region Заполняем таблицы customer_metric
DO
$$
    DECLARE
        -- variable_metric_quantity - количесвто метрик
        variable_metric_table_quantity       numeric;
        variable_limitation_columns_quantity integer := :variable_limitation;
        variable_column_index                integer := 1;
        variable_sql_query                   text; -- Динамический SQL-запрос
        variable_type_table                  text    := :variable_type_table;
    BEGIN
        SELECT
            CEIL(count(*)::numeric / variable_limitation_columns_quantity) AS metric_quantity
        INTO variable_metric_table_quantity
        FROM
            customer.metric;
        RAISE NOTICE 'variable_metric_quantity: %', variable_metric_table_quantity;
        FOR i IN 1..variable_metric_table_quantity
            LOOP
                SELECT
                    concat(
                            'INSERT INTO customer.customer_metric_'
                        , i::text
                        , '_'
                        , variable_type_table
                        , ' (
                       customer_id, metric_timestamp, '
                        , string_agg(
                                    name::text, ',' ORDER BY metric_id)
                        , ')'
                        , 'SELECT
                                c.customer_id,
                                time_series.metric_timestamp,'
                        , string_agg(
                                    'random()', ',' ORDER BY metric_id)
                        , ' FROM
    customer.customer c
CROSS JOIN LATERAL (
    SELECT generate_series(
        ''2024-01-01 00:00:00''::timestamp,
        ''2024-05-01 00:00:00''::timestamp,
        ''1 hour''::interval
    ) AS metric_timestamp
) time_series order by c.customer_id
LIMIT 1000000;'
                    )
                INTO
                    variable_sql_query
                FROM
                    customer.metric
                WHERE
                      metric_id >= variable_column_index
                  AND metric_id < (variable_column_index + variable_limitation_columns_quantity - 1500);

                variable_column_index := variable_column_index + variable_limitation_columns_quantity;

                EXECUTE variable_sql_query;
                -- RAISE NOTICE 'variable_sql_query: %', variable_sql_query;
            END LOOP;
    END
$$;

-- endregion
-- endregion
-- region Функции и их проверка
-- TODO: Возможно стоит поменять поиск на LIKE, чтобы это было более правдоподобно, но раз предполагаем, что мы четко вводим Фамилию и Имя, то оставляем так
-- region Создаем FUNCTION customer.find_customer
DROP FUNCTION customer.find_customer(text, text);

CREATE FUNCTION customer.find_customer(
    variable_search_last_name text,
    variable_search_first_name text DEFAULT NULL
)

    RETURNS table
            (
                city       text,
                last_name  text,
                first_name text,
                first_seen timestamp,
                last_seen  timestamp
            )
AS
$$
BEGIN
    RETURN QUERY
        SELECT
            cty.name
          , c.last_name
          , c.first_name
          , MIN(m.metric_timestamp) AS first_seen
          , MAX(m.metric_timestamp) AS last_seen
        FROM
            customer.customer c
                JOIN customer.city cty ON c.city_id = cty.city_id
                JOIN customer.customer_metric_1_default m ON c.customer_id = m.customer_id
        WHERE
            CASE
                WHEN variable_search_first_name IS NULL
                    THEN c.last_name = variable_search_last_name
                    ELSE c.last_name = variable_search_last_name AND c.first_name = variable_search_first_name
            END
        GROUP BY
            cty.name, c.last_name, c.first_name;
END;
$$ LANGUAGE plpgsql;

-- endregion
-- region Проверяем FUNCTION customer.find_customer
-- проверяем первую функцию на клиенте ниже
-- +-----------+-------+--------------------------------+--------------------------------+
-- |customer_id|city_id|first_name                      |last_name                       |
-- +-----------+-------+--------------------------------+--------------------------------+
-- |229        |1      |5fa64be1c2c6e16be7419e286c530129|f77dfd755e9f0e0f2c2cec7a05b46e4d|
-- +-----------+-------+--------------------------------+--------------------------------+
-- Поиск по сочетанию last_name first_name
SELECT
    city
  , last_name
  , first_name
  , first_seen
  , last_seen
FROM
    customer.find_customer('f77dfd755e9f0e0f2c2cec7a05b46e4d'::text, '5fa64be1c2c6e16be7419e286c530129'::text);
-- Поиск по last_name
SELECT
    city
  , last_name
  , first_name
  , first_seen
  , last_seen
FROM
    customer.find_customer('f77dfd755e9f0e0f2c2cec7a05b46e4d');

-- endregion

-- region Создаем FUNCTION customer.get_customer_metrics
DROP FUNCTION IF EXISTS customer.get_customer_metrics(int, text, timestamp, timestamp, text);

CREATE FUNCTION customer.get_customer_metrics(
    variable_customer_id int,
    variable_metric_names text,
    variable_start_time timestamp DEFAULT NULL,
    variable_end_time timestamp DEFAULT NULL,
    variable_aggregation_type text DEFAULT NULL
)
    RETURNS table
            (
                result_time timestamp,
                result      jsonb
            )
AS
$$
DECLARE
    variable_metric_list      text[];
    variable_metric_columns   text   := '';
    variable_tables_used      text[] := '{}';
    variable_sql_query_tables text;
    variable_sql_query        text;
    variable_type_table       text   := '_default';
    variable_group_by         text   := '';
BEGIN
    -- Разбираем список метрик в массив
    variable_metric_list := string_to_array(variable_metric_names, ',');
    -- Создаем временную таблицу чтобы дважды не сканировать таблицу метрик
    DROP TABLE IF EXISTS temp_table_metrics;

    CREATE TEMP TABLE temp_table_metrics AS
    SELECT
        name
      , table_name
    FROM
        customer.metric
    WHERE
        name = ANY (variable_metric_list);
    -- Определяем, какие таблицы использовать
    SELECT
        array_agg(DISTINCT table_name ORDER BY table_name)
    INTO
        variable_tables_used
    FROM
        temp_table_metrics;
    -- Собираем список метрик в SELECT
    SELECT
        CASE
            WHEN variable_aggregation_type IN ('DY', 'WK', 'MO')
                THEN string_agg(format('''%I_sum'' , SUM(%I)', name, name), ', ' ORDER BY name)
                ELSE string_agg(
                        format('''%I'' , %I', name, name), ', ' ORDER BY name
                     )
        END
    INTO
        variable_metric_columns
    FROM
        temp_table_metrics;
    -- Определяем добавляем ли мы GROUP BY
    IF variable_aggregation_type IN ('DY', 'WK', 'MO')
    THEN
        variable_group_by := 'GROUP BY result_time';
        END IF;
    -- Собираем список join в SELECT
    FOR i IN 1..array_length(variable_tables_used, 1)
        LOOP
            IF i = 1
            THEN
                variable_sql_query_tables := concat('customer.', variable_tables_used[i], variable_type_table, ' as t');
            ELSE
                variable_sql_query_tables :=
                        concat(variable_sql_query_tables, ' JOIN customer.', variable_tables_used[i],
                               variable_type_table, ' as t_', i::text,
                               ' USING (customer_id, metric_timestamp)');
                END IF;
        END LOOP;
    -- Собираем основной запрос
    variable_sql_query := format(
            'SELECT
                CASE
                    WHEN %L IS NULL THEN t.metric_timestamp
                    WHEN %L = ''DY'' THEN date_trunc(''day'', t.metric_timestamp)
                    WHEN %L = ''WK'' THEN date_trunc(''week'', t.metric_timestamp)
                    WHEN %L = ''MO'' THEN date_trunc(''month'', t.metric_timestamp)
                END AS result_time,
                jsonb_build_object(%s) AS result
            FROM %s
            WHERE t.customer_id = $1
              AND (
                    ($2 IS NULL AND $3 IS NULL) OR
                    (t.metric_timestamp BETWEEN COALESCE($2, ''-infinity''::timestamp)
                                          AND COALESCE($3, ''infinity''::timestamp))
                  )
            %s
            ORDER BY result_time;',
            variable_aggregation_type,
            variable_aggregation_type,
            variable_aggregation_type,
            variable_aggregation_type,
            variable_metric_columns,
            variable_sql_query_tables,
            variable_group_by);
    RAISE NOTICE 'variable_sql_query: %', variable_sql_query;
    -- -- Выполняем запрос
    RETURN QUERY EXECUTE variable_sql_query USING variable_customer_id, variable_start_time, variable_end_time;
END;
$$ LANGUAGE plpgsql;

-- endregion
-- region Проверяем FUNCTION customer.get_customer_metrics
-- За весь период времени
SELECT
    result_time
  , result
FROM
    customer.get_customer_metrics(229, 'metric_1,metric_2,metric_3,metric_1610'::text, NULL::timestamp, NULL::timestamp,
                                  NULL::text);
-- За указанный период времени
SELECT
    result_time
  , result
FROM
    customer.get_customer_metrics(229, 'metric_1,metric_2,metric_3,metric_1610'::text,
                                  '2024-01-01 00:00:00.000000'::timestamp, '2024-01-01 05:00:00.000000'::timestamp,
                                  NULL::text);
-- Агрегация по месяцам
SELECT
    result_time
  , result
FROM
    customer.get_customer_metrics(229, 'metric_1,metric_2,metric_3,metric_1610'::text, NULL::timestamp, NULL::timestamp,
                                  'DY'::text);
-- С ограничением с одной стороны
SELECT
    result_time
  , result
FROM
    customer.get_customer_metrics(229, 'metric_1,metric_2,metric_3,metric_1610'::text, NULL::timestamp,
                                  '2024-01-01 06:00:00.000000'::timestamp,
                                  'DY'::text);

-- endregion

-- Здесь добавил возможность суммировать метрики за весь период по городам, флаг ALL для этого нужно использовать
-- region Создаем FUNCTION customer.get_city_metrics
DROP FUNCTION IF EXISTS customer.get_city_metrics(text, timestamp, timestamp, text);

CREATE FUNCTION customer.get_city_metrics(
    variable_metric_names text,
    variable_start_time timestamp DEFAULT NULL,
    variable_end_time timestamp DEFAULT NULL,
    variable_aggregation_type text DEFAULT NULL
)
    RETURNS table
            (
                city        text,
                result_time timestamp,
                result      jsonb
            )
AS
$$
DECLARE
    variable_metric_list      text[];
    variable_metric_columns   text   := '';
    variable_tables_used      text[] := '{}';
    variable_sql_query_tables text;
    variable_sql_query        text;
    variable_type_table       text   := '_default';
    variable_group_by         text   := '';
BEGIN
    -- Разбираем список метрик в массив
    variable_metric_list := string_to_array(variable_metric_names, ',');
    -- Создаем временную таблицу чтобы дважды не сканировать таблицу метрик
    DROP TABLE IF EXISTS temp_table_metrics;

    CREATE TEMP TABLE temp_table_metrics AS
    SELECT
        name
      , table_name
    FROM
        customer.metric
    WHERE
        name = ANY (variable_metric_list);
    -- Определяем, какие таблицы использовать
    SELECT
        array_agg(DISTINCT table_name ORDER BY table_name)
    INTO
        variable_tables_used
    FROM
        temp_table_metrics;
    -- Собираем список метрик в SELECT
    SELECT
        CASE
            WHEN variable_aggregation_type IN ('DY', 'WK', 'MO', 'ALL')
                THEN string_agg(format('''%I_sum'' , SUM(%I)', name, name), ', ' ORDER BY name)
                ELSE string_agg(
                        format('''%I'' , %I', name, name), ', ' ORDER BY name
                     )
        END
    INTO
        variable_metric_columns
    FROM
        temp_table_metrics;
    -- Определяем добавляем ли мы GROUP BY
    IF variable_aggregation_type IN ('DY', 'WK', 'MO', 'ALL')
    THEN
        variable_group_by := 'GROUP BY city, result_time';
        END IF;
    -- Собираем список join в SELECT
    FOR i IN 1..array_length(variable_tables_used, 1)
        LOOP
            IF i = 1
            THEN
                variable_sql_query_tables := concat(
                        'customer.city as ct JOIN customer.customer AS ctmr USING (city_id) '
                    , 'JOIN customer.'
                    , variable_tables_used[i]
                    , variable_type_table
                    , ' as t USING (customer_id)');
            ELSE
                variable_sql_query_tables :=
                        concat(variable_sql_query_tables, ' JOIN customer.', variable_tables_used[i],
                               variable_type_table, ' as t_', i::text,
                               ' USING (customer_id, metric_timestamp)');
                END IF;
        END LOOP;
    -- Собираем основной запрос
    variable_sql_query := format(
            'SELECT
                ct.name AS city,
                CASE
                    WHEN %L IS NULL THEN t.metric_timestamp
                    WHEN %L = ''ALL'' THEN ''infinity''::timestamp
                    WHEN %L = ''DY'' THEN date_trunc(''day'', t.metric_timestamp)
                    WHEN %L = ''WK'' THEN date_trunc(''week'', t.metric_timestamp)
                    WHEN %L = ''MO'' THEN date_trunc(''month'', t.metric_timestamp)
                END AS result_time,
                jsonb_build_object(%s) AS result
            FROM %s
            WHERE 1=1
              AND (
                    ($1 IS NULL AND $2 IS NULL) OR
                    (t.metric_timestamp BETWEEN COALESCE($1, ''-infinity''::timestamp)
                                          AND COALESCE($2, ''infinity''::timestamp))
                  )
            %s
            ORDER BY result_time;',
            variable_aggregation_type,
            variable_aggregation_type,
            variable_aggregation_type,
            variable_aggregation_type,
            variable_aggregation_type,
            variable_metric_columns,
            variable_sql_query_tables,
            variable_group_by);
    RAISE NOTICE 'variable_sql_query: %', variable_sql_query;
    -- -- Выполняем запрос
    RETURN QUERY EXECUTE variable_sql_query USING variable_start_time, variable_end_time;
END;
$$ LANGUAGE plpgsql;

-- endregion
-- region Проверяем FUNCTION customer.get_city_metrics
-- В разрезе городов за весь период времени
SELECT
    city
  , result_time
  , result
FROM
    customer.get_city_metrics('metric_1,metric_2,metric_3,metric_1610'::text, NULL::timestamp,
                              NULL::timestamp,
                              NULL::text);
-- В разрезе городов за ограниченный период времени
SELECT
    city
  , result_time
  , result
FROM
    customer.get_city_metrics('metric_1,metric_2,metric_3,metric_1610'::text, NULL::timestamp,
                              '2024-01-01 01:00:00.000000'::timestamp,
                              NULL::text);
-- В разрезе городов с агрегацией по месяцам
SELECT
    city
  , result_time
  , result
FROM
    customer.get_city_metrics('metric_1,metric_2,metric_3,metric_1610'::text, NULL::timestamp,
                              NULL::timestamp,
                              'MO'::text);
-- Агрегация данных по всем городам за временной промежуток заданный на входе
SELECT
    city
  , result_time
  , result
FROM
    customer.get_city_metrics('metric_1,metric_2,metric_3,metric_1610'::text, NULL::timestamp,
                              NULL::timestamp,
                              'ALL'::text);
-- endregion
-- endregion


-- Из-за большого числа столбцов рассматривал вариант применения FOREIGN TABLE, вероятнее всего мы не будем обращаться ко всем метрикам сразу, а при использовании columnstor таблицы это должно быть быстрее
-- Но для их использвания нужен citus и дополнение cstore_fdw
-- Т.к. все равно делал, то приложу здесь
-- Для того чтобы протестировать использовал следующую сборка контейнера, можно поставить себе командой ниже
-- docker run --name columnarpostgresql -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=postgres -p 5432:5432 abuckenhofer/columnarpostgresql:latest
-- Load extension
CREATE EXTENSION cstore_fdw;
-- create server object to access an external data resource
CREATE SERVER cstore_server FOREIGN DATA WRAPPER cstore_fdw;
-- region Создаем таблицы customer_metric FOREIGN TABLE
DO
$$
    DECLARE
        -- variable_metric_quantity - количесвто метрик
        variable_metric_table_quantity       numeric;
        variable_limitation_columns_quantity integer := :variable_limitation;
        variable_column_index                integer := 1;
        variable_sql_query                   text; -- Динамический SQL-запрос
    BEGIN
        SELECT
            CEIL(count(*)::numeric / variable_limitation_columns_quantity) AS metric_quantity
        INTO variable_metric_table_quantity
        FROM
            customer.metric;
        FOR i IN 1..variable_metric_table_quantity
            LOOP
                SELECT
                    concat(
                            'CREATE FOREIGN TABLE customer.customer_metric_'
                        , i::text
                        , '_foreign'
                        , ' (
                        customer_id int NOT NULL,
                        metric_timestamp TIMESTAMP NOT NULL, '
                        , string_agg(
                                    name::text || ' ' || data_type::text, ',' ORDER BY metric_id)
                        , ') SERVER cstore_server
OPTIONS(compression ''pglz'');')
                INTO
                    variable_sql_query
                FROM
                    customer.metric
                WHERE
                      metric_id >= variable_column_index
                  AND metric_id < (variable_column_index + variable_limitation_columns_quantity);

                variable_column_index := variable_column_index + variable_limitation_columns_quantity;

                EXECUTE variable_sql_query;
            END LOOP;
    END
$$;

-- endregion
-- region Удаляем таблицы customer_metric FOREIGN TABLE
DO
$$
    DECLARE
        -- variable_metric_quantity - количесвто метрик
        variable_metric_table_quantity       numeric;
        variable_limitation_columns_quantity integer := :variable_limitation;
        variable_column_index                integer := 1;
        variable_sql_query                   text; -- Динамический SQL-запрос
    BEGIN
        SELECT
            CEIL(count(*)::numeric / variable_limitation_columns_quantity) AS metric_quantity
        INTO variable_metric_table_quantity
        FROM
            customer.metric;
        RAISE NOTICE 'variable_metric_quantity: %', variable_metric_table_quantity;
        FOR i IN 1..variable_metric_table_quantity
            LOOP
                SELECT
                    concat(
                            'DROP FOREIGN TABLE IF EXISTS customer.customer_metric_'
                        , i::text
                        , '_foreign'
                        , ';')
                INTO
                    variable_sql_query;
                EXECUTE variable_sql_query;
            END LOOP;
    END
$$;

-- endregion
-- region Сравнение columnstore и default
-- Результаты следующие
-- Если мы собираемся чаще обращаться ко всей таблице без фильтра - эффективнее будет подход с columnstore
-- Но columnstore таблица проигрывает индексу, если мы хотим отфильтровать по проиндексированному столбцу
-- Если мы собираемся чаще писать запросы с фильтром по строкам по столбцу который входит в интекс - эффективнее будет подход с обычной таблицей
-- Но обычная таблица проигрывает колумнсторной, если мы хотим написать щапрос который будет читать все строки разом
SELECT
    t_1.customer_id
  , t_1.metric_timestamp
  , t_1.metric_1
  , t_1.metric_2
  , t_1.metric_3
  , t_2.metric_1591
  , t_2.metric_1592
  , t_2.metric_1593
FROM
    customer.customer_metric_1_default AS t_1
        JOIN customer.customer_metric_2_default AS t_2 USING (customer_id, metric_timestamp);

-- +---------------------------------------------------------------------------------------------------------------------------------------------------------------------+
-- |QUERY PLAN                                                                                                                                                           |
-- +---------------------------------------------------------------------------------------------------------------------------------------------------------------------+
-- |Gather  (cost=537594.02..654526.60 rows=996569 width=78) (actual time=5237.974..5931.854 rows=1000000 loops=1)                                                       |
-- |  Workers Planned: 2                                                                                                                                                 |
-- |  Workers Launched: 2                                                                                                                                                |
-- |  ->  Merge Join  (cost=536594.02..553869.70 rows=415237 width=78) (actual time=5196.347..5735.534 rows=333333 loops=3)                                              |
-- |        Merge Cond: ((t_2.metric_timestamp = t_1.metric_timestamp) AND (t_2.customer_id = t_1.customer_id))                                                          |
-- |        ->  Sort  (cost=198738.21..199779.88 rows=416669 width=45) (actual time=2537.571..2628.243 rows=333333 loops=3)                                              |
-- |              Sort Key: t_2.metric_timestamp, t_2.customer_id                                                                                                        |
-- |              Sort Method: external merge  Disk: 20744kB                                                                                                             |
-- |              Worker 0:  Sort Method: external merge  Disk: 18240kB                                                                                                  |
-- |              Worker 1:  Sort Method: external merge  Disk: 17904kB                                                                                                  |
-- |              ->  Parallel Seq Scan on customer_metric_2_default t_2  (cost=0.00..147024.69 rows=416669 width=45) (actual time=270.077..2299.379 rows=333333 loops=3)|
-- |        ->  Materialize  (cost=337855.08..342855.09 rows=1000002 width=45) (actual time=2658.734..2923.860 rows=999997 loops=3)                                      |
-- |              ->  Sort  (cost=337855.08..340355.08 rows=1000002 width=45) (actual time=2658.729..2805.071 rows=999997 loops=3)                                       |
-- |                    Sort Key: t_1.metric_timestamp, t_1.customer_id                                                                                                  |
-- |                    Sort Method: external merge  Disk: 57808kB                                                                                                       |
-- |                    Worker 0:  Sort Method: external merge  Disk: 57808kB                                                                                            |
-- |                    Worker 1:  Sort Method: external merge  Disk: 57808kB                                                                                            |
-- |                    ->  Seq Scan on customer_metric_1_default t_1  (cost=0.00..176667.02 rows=1000002 width=45) (actual time=0.172..2114.623 rows=1000000 loops=3)   |
-- |Planning Time: 0.261 ms                                                                                                                                              |
-- |JIT:                                                                                                                                                                 |
-- |  Functions: 45                                                                                                                                                      |
-- |  Options: Inlining true, Optimization true, Expressions true, Deforming true                                                                                        |
-- |  Timing: Generation 5.100 ms, Inlining 88.781 ms, Optimization 433.368 ms, Emission 287.253 ms, Total 814.502 ms                                                    |
-- |Execution Time: 5973.863 ms                                                                                                                                          |
-- +---------------------------------------------------------------------------------------------------------------------------------------------------------------------+
SELECT
    t_1.customer_id
  , t_1.metric_timestamp
  , t_1.metric_1
  , t_1.metric_2
  , t_1.metric_3
  , t_2.metric_1591
  , t_2.metric_1592
  , t_2.metric_1593
FROM
    customer.customer_metric_1_foreign AS t_1
        JOIN customer.customer_metric_2_foreign AS t_2 USING (customer_id, metric_timestamp);

-- +----------------------------------------------------------------------------------------------------------------------------------------------------------------+
-- |QUERY PLAN                                                                                                                                                      |
-- +----------------------------------------------------------------------------------------------------------------------------------------------------------------+
-- |Merge Join  (cost=457168.42..904668.42 rows=25000000 width=204) (actual time=1389.195..2014.608 rows=1000000 loops=1)                                           |
-- |  Merge Cond: ((t_1.customer_id = t_2.customer_id) AND (t_1.metric_timestamp = t_2.metric_timestamp))                                                           |
-- |  ->  Sort  (cost=226295.59..228795.59 rows=1000000 width=108) (actual time=816.041..915.927 rows=1000000 loops=1)                                              |
-- |        Sort Key: t_1.customer_id, t_1.metric_timestamp                                                                                                         |
-- |        Sort Method: external merge  Disk: 57808kB                                                                                                              |
-- |        ->  Foreign Scan on customer_metric_1_foreign t_1  (cost=0.00..10423.75 rows=1000000 width=108) (actual time=122.945..555.431 rows=1000000 loops=1)     |
-- |              CStore File: /var/lib/postgresql/data/cstore_fdw/13408/16565                                                                                      |
-- |              CStore File Size: 1105276518                                                                                                                      |
-- |  ->  Materialize  (cost=230872.82..235872.82 rows=1000000 width=108) (actual time=573.121..796.646 rows=1000000 loops=1)                                       |
-- |        ->  Sort  (cost=230872.82..233372.82 rows=1000000 width=108) (actual time=573.117..689.670 rows=1000000 loops=1)                                        |
-- |              Sort Key: t_2.customer_id, t_2.metric_timestamp                                                                                                   |
-- |              Sort Method: external merge  Disk: 56784kB                                                                                                        |
-- |              ->  Foreign Scan on customer_metric_2_foreign t_2  (cost=0.00..15000.98 rows=1000000 width=108) (actual time=11.157..252.849 rows=1000000 loops=1)|
-- |                    CStore File: /var/lib/postgresql/data/cstore_fdw/13408/16568                                                                                |
-- |                    CStore File Size: 917676622                                                                                                                 |
-- |Planning Time: 11.764 ms                                                                                                                                        |
-- |JIT:                                                                                                                                                            |
-- |  Functions: 13                                                                                                                                                 |
-- |  Options: Inlining true, Optimization true, Expressions true, Deforming true                                                                                   |
-- |  Timing: Generation 1.533 ms, Inlining 1.708 ms, Optimization 68.234 ms, Emission 40.226 ms, Total 111.702 ms                                                  |
-- |Execution Time: 2052.437 ms                                                                                                                                     |
-- +----------------------------------------------------------------------------------------------------------------------------------------------------------------+
SELECT
    t_1.customer_id
  , t_1.metric_timestamp
  , t_1.metric_1
  , t_1.metric_2
  , t_1.metric_3
  , t_2.metric_1591
  , t_2.metric_1592
  , t_2.metric_1593
FROM
    customer.customer_metric_1_default AS t_1
        JOIN customer.customer_metric_2_default AS t_2 USING (customer_id, metric_timestamp)
WHERE
    customer_id = 1;

-- +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
-- |QUERY PLAN                                                                                                                                                                                      |
-- +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
-- |Merge Join  (cost=0.85..10592.36 rows=2783 width=78) (actual time=0.041..3.558 rows=2905 loops=1)                                                                                               |
-- |  Merge Cond: (t_1.metric_timestamp = t_2.metric_timestamp)                                                                                                                                     |
-- |  ->  Index Scan using customer_metric_idx_customer_id_metric_timestamp_1 on customer_metric_1_default t_1  (cost=0.42..5314.54 rows=2853 width=45) (actual time=0.023..0.916 rows=2905 loops=1)|
-- |        Index Cond: (customer_id = 1)                                                                                                                                                           |
-- |  ->  Index Scan using customer_metric_idx_customer_id_metric_timestamp_2 on customer_metric_2_default t_2  (cost=0.42..5235.77 rows=2834 width=45) (actual time=0.013..0.945 rows=2905 loops=1)|
-- |        Index Cond: (customer_id = 1)                                                                                                                                                           |
-- |Planning Time: 0.294 ms                                                                                                                                                                         |
-- |Execution Time: 3.802 ms                                                                                                                                                                        |
-- +------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
SELECT
    t_1.customer_id
  , t_1.metric_timestamp
  , t_1.metric_1
  , t_1.metric_2
  , t_1.metric_3
  , t_2.metric_1591
  , t_2.metric_1592
  , t_2.metric_1593
FROM
    customer.customer_metric_1_foreign AS t_1
        JOIN customer.customer_metric_2_foreign AS t_2 USING (customer_id, metric_timestamp)
WHERE
    customer_id = 1;

-- +--------------------------------------------------------------------------------------------------------------------------------------------------+
-- |QUERY PLAN                                                                                                                                        |
-- +--------------------------------------------------------------------------------------------------------------------------------------------------+
-- |Merge Join  (cost=31039.12..32939.12 rows=125000 width=204) (actual time=18.139..19.656 rows=2905 loops=1)                                        |
-- |  Merge Cond: (t_1.metric_timestamp = t_2.metric_timestamp)                                                                                       |
-- |  ->  Sort  (cost=13230.94..13243.44 rows=5000 width=108) (actual time=10.755..10.924 rows=2905 loops=1)                                          |
-- |        Sort Key: t_1.metric_timestamp                                                                                                            |
-- |        Sort Method: quicksort  Memory: 505kB                                                                                                     |
-- |        ->  Foreign Scan on customer_metric_1_foreign t_1  (cost=0.00..12923.75 rows=5000 width=108) (actual time=4.623..10.303 rows=2905 loops=1)|
-- |              Filter: (customer_id = 1)                                                                                                           |
-- |              Rows Removed by Filter: 7095                                                                                                        |
-- |              CStore File: /var/lib/postgresql/data/cstore_fdw/13408/16565                                                                        |
-- |              CStore File Size: 1105276518                                                                                                        |
-- |  ->  Sort  (cost=17808.17..17820.67 rows=5000 width=108) (actual time=7.377..7.536 rows=2905 loops=1)                                            |
-- |        Sort Key: t_2.metric_timestamp                                                                                                            |
-- |        Sort Method: quicksort  Memory: 323kB                                                                                                     |
-- |        ->  Foreign Scan on customer_metric_2_foreign t_2  (cost=0.00..17500.98 rows=5000 width=108) (actual time=4.002..6.886 rows=2905 loops=1) |
-- |              Filter: (customer_id = 1)                                                                                                           |
-- |              Rows Removed by Filter: 7095                                                                                                        |
-- |              CStore File: /var/lib/postgresql/data/cstore_fdw/13408/16568                                                                        |
-- |              CStore File Size: 917676622                                                                                                         |
-- |Planning Time: 6.827 ms                                                                                                                           |
-- |Execution Time: 19.927 ms                                                                                                                         |
-- +--------------------------------------------------------------------------------------------------------------------------------------------------+
-- endregion