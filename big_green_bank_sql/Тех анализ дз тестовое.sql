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
  (select c.customer_id, ct.country_id, c.first_name || ' ' || c.last_name customer_name
     from customer c
     join address a
       on a.address_id = c.address_id
     join city ct
       on ct.city_id = a.city_id),
 rental_params as
   (select cc.country_id, cc.customer_id, min(cc.customer_name) customer_name, count(r.rental_id) count_rent, max(rental_date) max_rental_date
      from customer_country cc
      left join rental r
        on r.customer_id = cc.customer_id
     group by cc.country_id, cc.customer_id),
 payment_params as
   (select cc.country_id, cc.customer_id, min(cc.customer_name) customer_name, sum(p.amount) sum_amount
      from customer_country cc
      left join payment p
        on p.customer_id = cc.customer_id
     group by cc.country_id, cc.customer_id)
select c.country, countrent.customer_name, sumamount.customer_name, rentaldate.customer_name
  from country c
  left join (select t1.country_id, string_agg(t1.customer_name, ', ') customer_name
              from (select r.country_id, r.customer_name, dense_rank() over(partition by country_id order by count_rent desc) rn
                      from rental_params r) t1
              where t1.rn = 1
              group by t1.country_id) countrent
         on countrent.country_id = c.country_id
  left join (select t2.country_id, string_agg(t2.customer_name, ', ') customer_name
              from (select r.country_id, r.customer_name, dense_rank() over(partition by country_id order by max_rental_date desc) rn
                      from rental_params r) t2
              where t2.rn = 1
              group by t2.country_id) rentaldate
         on rentaldate.country_id = c.country_id
  left join (select t3.country_id, string_agg(t3.customer_name, ', ') customer_name
              from (select r.country_id, r.customer_name, dense_rank() over(partition by country_id order by sum_amount desc) rn
                      from payment_params r) t3
              where t3.rn = 1
              group by t3.country_id) sumamount
         on sumamount.country_id = c.country_id;


