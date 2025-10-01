-- region 1) Требуется написать скрипты создания таблиц и запрос, который выведет все открытые сделки (не в отмененном статусе) с общей суммой платежей, действительных на текущую дату, более 100 000
-- region DDL Deal
DROP TABLE IF EXISTS public.deal;

CREATE TABLE public.deal
(

    ID       serial PRIMARY KEY,
    Number   bigint,
    DealDate date,
    Status   VARCHAR(8) NOT NULL CHECK (Status IN ('NEW', 'VERIFIED', 'CANCELED'))

);

-- Комментарии к таблице
comment on table public.deal IS 'Информация по сделкам';
comment on column public.deal.ID IS 'ID сделки';
comment on column public.deal.Number IS 'Номер сделки';
comment on column public.deal.DealDate IS 'Дата заключения сделки';
comment on column public.deal.Status IS 'Статус сделки. Возможные значения Status: NEW, VERIFIED, CANCELED';

-- endregion
-- region DDL Payment
DROP TABLE IF EXISTS public.payment;

CREATE TABLE public.payment
(

    ID             serial PRIMARY KEY,
    DealID         bigint not null,
    PaymentDate    date,
    Qty            numeric,
    Effective_from date,
    Effective_to   date,
    CONSTRAINT fk_deal_id foreign key (DealID) references public.deal (id) on delete restrict

);

-- Комментарии к таблице
comment on table public.payment IS 'Информация по платежам';
comment on column public.payment.ID IS 'ID платежа';
comment on column public.payment.DealID IS 'ID сделки';
comment on column public.payment.PaymentDate IS 'Дата платежа';
comment on column public.payment.Qty IS 'Сумма платежа';
comment on column public.payment.Effective_from IS 'Дата, с которой платеж действителен';
comment on column public.payment.Effective_to IS 'Дата, до которой платеж действителен';
-- endregion
-- region Query "все открытые сделки (не в отмененном статусе) с общей суммой платежей, действительных на текущую дату, более 100 000"
select d.ID,
       d.Number,
       d.DealDate,
       d.Status,
       coalesce(sum(p.Qty), 0) as Qty_sum
from deal as d
         inner join public.payment as p on d.ID = p.DealID
where 1 = 1
  and d.Status <> 'CANCELED'
  and p.Effective_from <= current_date
  and p.Effective_to => current_date
group by d.ID,
         d.Number,
         d.DealDate,
         d.Status
having coalesce(sum(p.Qty), 0) > 100000
-- endregion
-- endregion

-- region 2) Оптимизировать запрос, без использования подзапроса, в котором есть ссылка на внешнюю таблицу.
-- исходный запрос
update DistStatus r
set IsDeleted = case
                    when StatusId = (select StatusId
                                     from DistStatus
                                     where DealId = r.DealId
                                       and RowNum < r.RowNum
                                       and isDeleted = 0::bit
                                     order by RowNum desc
                                     limit 1)
                        then 1::bit
                    else IsDeleted
    end;
-- region ddl на котором проверял как работает
DROP TABLE IF EXISTS public.DistStatus;

CREATE TABLE public.DistStatus
(
    IsDeleted bit(1),
    RowNum    serial PRIMARY KEY,
    StatusId  bigint,
    DealId    bigint
);

insert into public.DistStatus (IsDeleted, StatusId, DealId)
values (0::bit, 10, 100),
       (1::bit, 10, 100),
       (0::bit, 10, 100),
       (1::bit, 10, 100),
       (0::bit, 10, 100);
-- endregion
-- запрос без подзапроса, который работает похожим образом, дает тот же результат
UPDATE public.DistStatus AS r
SET IsDeleted = CASE
                    WHEN r.StatusId = p.StatusId THEN 1::bit
                    ELSE r.IsDeleted
    END
FROM public.DistStatus AS p
WHERE p.DealId = r.DealId
  AND p.RowNum < r.RowNum
  AND p.IsDeleted = 0::bit
;
-- endregion

-- region 3) Необходимо изучить структуру таблиц, прочитать условие задачи и написать все вопросы, возникшие по постановке. Запрос написать исходя из текущего понимания и предположений

-- Пока собирал запрос вспомнил что можно сделать такого плана запрос для выбора наиболее актуального представления строки,
-- но я бы постестировал что будет работать быстрее этот вариант или вариант который я в ответе указал.

-- region Вариант отбора наиболее актуальных сделок
with actual_trade AS (SELECT a_1.ID,
                             a_1.Product_fk,
                             a_1.Number,
                             a_1.Status,
                             a_1.MurexID,
                             a_1.DealDate,
                             a_1.CounterPartyID,
                             a_1.ActualDate,
                             a_1.FixDate,
                             a_1.PremiumType,
                             a_1.SettlementType,
                             a_1.Version
                      FROM public.trade a_1
                      WHERE NOT (EXISTS(SELECT 1
                                        FROM public.trade b
                                        WHERE a_1.Number = b.Number
                                          AND a_1.Version < b.Version))
                        -- Редко когда оставляют статичную дату фильтрации, поэтому предположу, что вместо a_1.ActualDate > '01.04.2021'::date
                        -- Здесь должна быть конструкция вроде той что я ниже привожу
                        and a_1.ActualDate > MAKE_DATE((EXTRACT( YEAR FROM now())::int - 4)::int, 4::int, 1::int)
                        and a_1.Status = 'VERIFIED');
-- endregion
-- Вопросы:

-- Правильно ли я понимаю, что атрибут "Number" и набильший "Version" дают строку актуального состояния сделки, а ID это
-- просто строка которая обозначает сущность в виде некой версии сделки?

-- Указано "При обновлении версии в таблице по сделкам автоматически обновляется версия и в остальных таблицах", получается, что
-- в Fee и Cashflow не добавляются (insert) строки с новой версией сделки, а именно обновляются (update)? Мне каежтся это не так,
-- если хранятся версии сделок в Trade, то почему не хранить вресии и в Fee и Cashflow, чтобы можно было воссоздать картину прошлого?

-- Связь между public.trade и public.Cashflow мне не до конца ясна, связать их собираюсь через
-- on public.trade.Product_fk = public.Cashflow.Product_fk AND public.trade.CounterPartyID = public.Cashflow.CounterPartyID AND public.trade.Version = public.Cashflow.Version
-- Верно ли выбран способ индентификации соответсвия Cashflow к trade?

-- public.Cashflow у нас выпадают платежи в USD, возможно их нужно конвертировать в RUB? В противном случае они просто не будут учтены.

-- Я не совсем понял что подразумевается под "актуальные на эту дату события"? Предполагаю что наши платежи должны как-то зависеть от того когда запрос выполняется.
-- Пусть будет так что мы хотим видеть предстоящие платежи, а не из прошлого.

-- Отсортированы как, от большего к меньшему и от наиболее свежего к наиболее старому?
-- Мне кажется нужно сортировать по сделке и плановой дате исполнения платежа, разве нет?
-- Так у нас будет какая-то упорядоченность сделок и платежей относящихся к ней

-- region Запрос без комментариев:
with
filtered_actual_trade AS (SELECT b.Number,
                                 MAX(b.Version) AS max_version
                          FROM public.trade as b
                          WHERE b.ActualDate > MAKE_DATE((EXTRACT( YEAR FROM now())::int - 4)::int, 4::int, 1::int)
                            AND b.Status = 'VERIFIED'
                          GROUP BY b.Number),
actual_trade AS (select a_1.ID,
                        a_1.Product_fk,
                        a_1.Number,
                        a_1.Status,
                        a_1.MurexID,
                        a_1.DealDate,
                        a_1.CounterPartyID,
                        a_1.ActualDate,
                        a_1.FixDate,
                        a_1.PremiumType,
                        a_1.SettlementType,
                        a_1.Version
                 from public.trade as a_1
                    inner join filtered_actual_trade as a_2 on
                        a_2.Number = a_1.Number
                    and a_2.max_version = a_1.Version
                     ),
main_query as (
SELECT
t.Number, -- -Номер сделки
t.DealDate, -- -Дата заключения сделки
t.ActualDate, -- -Бизнес дата события
-- f.ID,
-- f.Trade_fk,
-- null as Product_fk,
-- f.Notional,
f.Amount, -- -Сумма планового платежа
f.ValueDate, -- -Плановая дата исполнения платежа
f.FeeType as payment_type, -- -Тип планового платежа
-- f.CounterPartyID,
f.Currency -- -Валюта планового платежа
-- f.Version,
-- f.Input,
-- null as Rate
from actual_trade as t
         inner join public.fee as f on f.Trade_fk = t.ID
UNION ALL
SELECT
t.Number, -- -Номер сделки
t.DealDate, -- -Дата заключения сделки
t.ActualDate, -- -Бизнес дата события
-- c.ID,
-- null as Trade_fk,
-- c.Product_fk,
-- c.Notional,
c.Amount, -- -Сумма планового платежа
c.ValueDate, -- -Плановая дата исполнения платежа
c.TransferType as payment_type, -- -Тип планового платежа
-- c.CounterPartyID,
c.Currency -- -Валюта планового платежа
-- c.Version,
-- null as Input,
-- c.Rate
from actual_trade as t
         inner join public.Cashflow as c on
                 c.Product_fk = t.Product_fk
             and c.CounterPartyID = t.CounterPartyID
             and c.Version = t.Version
)
select distinct
    q.Number, -- -Номер сделки
    q.DealDate, -- -Дата заключения сделки
    q.ActualDate, -- -Бизнес дата события
    q.Amount, -- -Сумма планового платежа
    q.ValueDate, -- -Плановая дата исполнения платежа
    q.payment_type, -- -Тип планового платежа
    q.Currency -- -Валюта планового платежа
from main_query as q
where q.Currency = 'RUB'
and q.ValueDate >= now()
order by
    q.Number, q.ValueDate
;
-- endregion
-- region Запрос с комментариями:
with
-- отбираем акутальные сделки
filtered_actual_trade AS (SELECT b.Number,
                                 MAX(b.Version) AS max_version
                          FROM public.trade as b
-- Редко когда оставляют статичную дату фильтрации, поэтому предположу, что вместо b.ActualDate > '2021-04-01'::date
-- Здесь должна быть конструкция вроде той что я ниже привожу
                          WHERE b.ActualDate > MAKE_DATE((EXTRACT( YEAR FROM now())::int - 4)::int, 4::int, 1::int)
                            AND b.Status = 'VERIFIED'
                          GROUP BY b.Number),
-- получаем актуальное представление по сделкам
actual_trade AS (select a_1.ID,
                        a_1.Product_fk,
                        a_1.Number,
                        a_1.Status,
                        a_1.MurexID,
                        a_1.DealDate,
                        a_1.CounterPartyID,
                        a_1.ActualDate,
                        a_1.FixDate,
                        a_1.PremiumType,
                        a_1.SettlementType,
                        a_1.Version
                 from public.trade as a_1
                    inner join filtered_actual_trade as a_2 on
                        a_2.Number = a_1.Number
                    and a_2.max_version = a_1.Version
                     ),
-- получаем общую таблицу по Информация по плановым платежам

-- Указано "При обновлении версии в таблице по сделкам автоматически обновляется версия и в остальных таблицах", получается, что
-- в Fee и Cashflow не добавляются (insert) строки с новой версией сделки, а именно обновляются (update)? Мне каежтся это не так,
-- если хранятся версии сделок в Trade, то почему не хранить вресии и в Fee и Cashflow, чтобы можно было воссоздать картину прошлого?

-- Теперь нам надо собрать через Union ALL общую таблицу по плановым платежам, но тут проблема следующая
-- Под public.trade естественно понимается актуальное представление actual_trade, наши актуальные отобранные сделки
-- Связь между public.trade и public.fee очевидная, мы однозначно можем отнести "Информация по плановым платежам (комиссии)"
-- к "Информация по сделкам" через оn public.trade.ID = public.fee.Trade_fk
-- Связь между public.trade и public.Cashflow мне не до конца ясна, связать их собираюсь через
-- on public.trade.Product_fk = public.Cashflow.Product_fk AND public.trade.CounterPartyID = public.Cashflow.CounterPartyID AND public.trade.Version = public.Cashflow.Version
main_query as (
SELECT
t.Number, -- -Номер сделки
t.DealDate, -- -Дата заключения сделки
t.ActualDate, -- -Бизнес дата события
-- Столбцы Информации по плановым платежам
-- f.ID,
-- f.Trade_fk,
-- null as Product_fk,
-- f.Notional,
f.Amount, -- -Сумма планового платежа
f.ValueDate, -- -Плановая дата исполнения платежа
f.FeeType as payment_type, -- -Тип планового платежа
-- f.CounterPartyID,
f.Currency -- -Валюта планового платежа
-- f.Version,
-- f.Input,
-- null as Rate
from actual_trade as t
         inner join public.fee as f on f.Trade_fk = t.ID
UNION ALL
SELECT
t.Number, -- -Номер сделки
t.DealDate, -- -Дата заключения сделки
t.ActualDate, -- -Бизнес дата события
-- Столбцы Информации по плановым платежам
-- c.ID,
-- null as Trade_fk,
-- c.Product_fk,
-- c.Notional,
c.Amount, -- -Сумма планового платежа
c.ValueDate, -- -Плановая дата исполнения платежа
c.TransferType as payment_type, -- -Тип планового платежа
-- c.CounterPartyID,
c.Currency -- -Валюта планового платежа
-- c.Version,
-- null as Input,
-- c.Rate
from actual_trade as t
         inner join public.Cashflow as c on
                 c.Product_fk = t.Product_fk
             and c.CounterPartyID = t.CounterPartyID
             and c.Version = t.Version
)
select distinct
    q.Number, -- -Номер сделки
    q.DealDate, -- -Дата заключения сделки
    q.ActualDate, -- -Бизнес дата события
    q.Amount, -- -Сумма планового платежа
    q.ValueDate, -- -Плановая дата исполнения платежа
    q.payment_type, -- -Тип планового платежа
    q.Currency -- -Валюта планового платежа
from main_query as q
-- Тут у нас выпадают платежи в USD, возможно их нужно конвертировать в RUB? В противном случае они просто не будут учтены.
where q.Currency = 'RUB'
-- Я не совсем понял что подразумевается под "актуальные на эту дату события", предполагаю что наши платежи должны как-то зависеть от того когда запрос выполняется.
-- Пусть будет так что мы хотим видеть предстоящие платежи, а не из прошлого
and q.ValueDate >= now()
order by
-- Отсортированы как, от большего к меньшему и от наиболее свежего к наиболее старому?
-- Мне кажется нужно сортировать по сделке и плановой дате исполнения платежа, разве нет?
-- Так у нас будет какая-то упорядоченность сделок и платежей относящихся к ней
    q.Number, q.ValueDate
;
-- endregion
-- endregion