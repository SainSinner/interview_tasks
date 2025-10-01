-- У вас есть скрипт, который написан для схемы салонов видеопроката
-- Необходимо выполнить технический анализ кода, описать можно в любой свободной форме, но так, чтобы было понятно человеку, далекому от кода
-- Диагрмма схемы приложена в письме
-- Описание таблиц:
---- customer - таблица с клиентами
---- address  - таблица с адресами
---- city     - таблица с городами
---- rental   - таблица с арендами дисков
---- payment  - таблица с платежами за аренду фильма


with customer_country as
--     Этим подзапросом мы формируем данные по покупателям.
--     Объединяем их имя и фамилию c.first_name || ' ' || c.last_name customer_name.
--     Определяем из какой страны наш покупатель. Путем соединения с таблицей customer таблицы address, а уже address соединяем с city.
--     Это нужно потому что инфомрация о стране харнится только в city.
  (select c.customer_id, ct.country_id, c.first_name || ' ' || c.last_name customer_name
     from customer c
     join address a
       on a.address_id = c.address_id
     join city ct
       on ct.city_id = a.city_id),
 rental_params as
--     Этим подзапросом мы получаем количество аренд по каждому клиенту и дату его последней аренды.
--     Соединяем стаблицу rental с каждым покупателем ,потому что в rental содержится информация по заказам.
   (select cc.country_id, cc.customer_id, min(cc.customer_name) customer_name, count(r.rental_id) count_rent, max(rental_date) max_rental_date
      from customer_country cc
      left join rental r
        on r.customer_id = cc.customer_id
     group by cc.country_id, cc.customer_id),
 payment_params as
--     Этим подзапросом мы получаем суммарный доход который принес нам каждый клиент
--     Соединяем стаблицу payment с каждым заказом, чтобы узнать доход по каждому заказу.
   (select cc.country_id, cc.customer_id, min(cc.customer_name) customer_name, sum(p.amount) sum_amount
      from customer_country cc
      left join payment p
        on p.customer_id = cc.customer_id
     group by cc.country_id, cc.customer_id)
select c.country, countrent.customer_name, sumamount.customer_name, rentaldate.customer_name
  from country c
--   Этим подзапросом мы формируем список клиентов через ",", которые занимают первое место по количеству аренд в своей стране.
--   Так как у нескольких клиентов может быть одинаковое количество аренд выводим всех, кто разделяет первое место.
  left join (select t1.country_id, string_agg(t1.customer_name, ', ') customer_name
              from
--   Здесь ранжируем клиентов по количеству аренд, первый ранг отдаем тем у кого наибольшее количесвто аренд в своей стране.
                  (select r.country_id, r.customer_name, dense_rank() over(partition by country_id order by count_rent desc) rn
                      from rental_params r) t1
              where t1.rn = 1
              group by t1.country_id) countrent
         on countrent.country_id = c.country_id
--   Этим подзапросом мы формируем список клиентов через ",", которые совершили наиболее актуальную аренду в своей стране.
--   Так как у нескольких клиентов может совпасть дата аренды и она может быть наиболее актуальной в этой стране, выводим всех, кто разделяет первое место.
  left join (select t2.country_id, string_agg(t2.customer_name, ', ') customer_name
              from
--   Здесь ранжируем клиентов по наиболее свежему посещению салона аренды, первый ранг отдаем тем кто был в последнюю дату посещения в своей стране.
                  (select r.country_id, r.customer_name, dense_rank() over(partition by country_id order by max_rental_date desc) rn
                      from rental_params r) t2
              where t2.rn = 1
              group by t2.country_id) rentaldate
         on rentaldate.country_id = c.country_id
--   Этим подзапросом мы формируем список клиентов через "," которые принесли наибольший доход нашему прокату в своей стране.
--   Так как у нескольких клиентов может совпасть принесенный прокату доход, выводим всех, кто разделяет первое место.
  left join (select t3.country_id, string_agg(t3.customer_name, ', ') customer_name
--   Здесь ранжируем клиентов по наибольшей сумме дохода которую он принес нашему прокату, первый ранг отдаем тем кто принес наибольший доход в своей стране.
              from (select r.country_id, r.customer_name, dense_rank() over(partition by country_id order by sum_amount desc) rn
                      from payment_params r) t3
              where t3.rn = 1
              group by t3.country_id) sumamount
         on sumamount.country_id = c.country_id;
-- В итоге мы получаем имена наших клиентов у которых показатели по своей стране считаются выдащимися в трех категориях:
-- countrent.customer_name - в этом столбце представлены клиенты у которых количество аренд соответствует максимальному по их стране
-- sumamount.customer_name - в этом столбце представлены клиенты у которых сумма трат в прокате соответствует максимальной по их стране
-- rentaldate.customer_name - в этом столбце представлены клиенты посетившие салон проката в последний зафиксированный день в своей стране


