	

-----------------------Пункт 1-------------------------
select films.category_id, c2."name" as category_name, num  
from	
	((select category_id, count(film_id) as num 
	from public.film_category  
	group by category_id) as films
	left outer join 
	public.category c 
	on films.category_id = c.category_id)
left outer join 
public.category c2 
on films.category_id = c2.category_id 
order by num desc;
-----------------------Пункт 1-------------------------

----------------Пункт 2-----------------

--Под фразой "чьи фильмы больше всего арендовали" понимаю количество приобретений в аренду копий фильмов (не длительность аренды, а количество раз)
select actor_id, first_name, last_name
from
(
	select a.actor_id , a.first_name, a.last_name, count(r.rental_id) as rents
	from 
	public.film_actor fa 
	left outer join
	public.film f
	on f.film_id = fa.film_id 
	left outer join 
	public.inventory i 
	on f.film_id = i.film_id 
	left outer join 
	public.rental r 
	on i.inventory_id = r.inventory_id 
	left outer join 
	public.actor a 
	on a.actor_id  = fa.actor_id 
	where r.rental_id is not null
	group by a.actor_id , a.first_name, a.last_name
	order by rents desc
	limit 10
) as result;
----------------Пункт 2-----------------


----------------Пункт 3-----------------
select "name"
from
(select distinct  c3."name", sum(p.amount)  
	from public.film as f
	left outer join
	public.inventory i 
	on f.film_id = i.film_id --находим для каждого фильма номер inventory_id (его сдаваемой в аренду копии)
	left outer join 
	public.rental r 
	on i.inventory_id = r.inventory_id --джойним с таблицей rental, чтобы затем по rental_id найти платеж
	left outer join 
	public.payment p 
	on r.rental_id = p.rental_id -- по rental_id джойним с payments, чтобы достать оттуда сумму payment
	left outer join 
	public.film_category fc --вспоминаем, что нам нужны категории фильмов, джойним с таблицей film_category, чтобы найти category_id
	on f.film_id = fc.film_id 
	left outer join 
	public.category c3 
	on fc.category_id = c3.category_id  --и затем по category_id находим название категории
 group by c3."name"
 order by sum(p.amount) desc
 limit 1) result;
 ----------------Пункт 3-----------------

----------------Пункт 4-----------------
select f.film_id, f2.title 
from public.film as f
left outer join
public.inventory as inv
On f.film_id = inv.film_id 
left outer join 
public.film f2 
on f.film_id = f2.film_id 
Where inv.film_id is null;
----------------Пункт 4-----------------

-----------------Пункт 5------------------
--
select result2.actor_id, first_name, last_name
from
(
  --Здесь считаем какой актер по счету в списке с количеством фильмов
	select distinct actor_id, cnt,
	dense_rank() over (order by cnt desc) as pos
	from 
	(
    --Здесь считаем в скольких фильмах из категории 'Children' снялись актеры
		select actor_id, fa.film_id as film_id, 
		count(*) over(partition by actor_id) as cnt
		from public.film_actor fa 
		left outer join public.film_category fc 
		on fa.film_id = fc.film_id 
		where fc.category_id  in (select category_id from public.category where "name" = 'Children')
		
	) result1
	order by (dense_rank() over (order by cnt desc)) asc
) as result2
left outer join 
public.actor a 
on result2.actor_id = a.actor_id
where pos <=3
order by pos desc;
--Выводим актеров, у которых ранг <=3, по убыванию ранга

---------------Пункт 5---------------------

-----------------Пункт 6--------------------
--сa - количество активных клиентов
--cna - количествно неактивных клиенто

--сс или сa могут быть is NULL, если для города из таблицы city не нашлось соответствия адреса в таблице address, либо если по найденному адресу нет клиента в таблице customer

select distinct c.city_id as c_id, city,
sum (case when active is null then null when active = 1 then 1 else 0 end) over (partition by c.city_id) as ca,
sum (case when active is null then null when active = 0 then 1 else 0 end) over (partition by c.city_id) as cna
from 
public.city c
left outer join 
public.address a  
on c.city_id = a.city_id
left outer join 
public.customer c2
on a.address_id = c2.address_id 
order by sum (case when active is null then null when active = 0 then 1 else 0 end) over (partition by c.city_id) desc;

-----------------Пункт 6--------------------


-----------------Пункт 7----------------------

select title, "name"

from 
(
	(select distinct 'For cities started with a' as title, c3."name" , -- поле title исользую просто, чтобы разделить пункты задания
	sum(round(abs(extract(epoch from r.return_date - r.rental_date)/3600)::numeric,2)) over(partition by c3."name") as sum_rental 
	-- здесь посчитано кол-во часов аренды (return_date - rental_date) по каждой категории
	from public.film as f
	left outer join
	public.inventory i 
	on f.film_id = i.film_id --находим для каждого фильма номер inventory_id (его сдаваемой в аренду копии)
	left outer join 
	public.rental r 
	on i.inventory_id = r.inventory_id --далее через таблицу rental находим покупателя, который собственно взял в аренду копию inventory_id
	left outer join 
	public.customer c 
	on r.customer_id = c.customer_id -- находим покупателя customer_id
	left outer join 
	public.address a
	on c.address_id = a.address_id -- затем находим адрес покупателя
	left outer join 
	public.city c2 
	on a.city_id = c2.city_id -- через адрес находим город
	left outer join 
	public.film_category fc --вспоминаем, что нам нужны категории фильмов, джойним с таблицей film_category, чтобы найти category_id
	on f.film_id = fc.film_id 
	left outer join 
	public.category c3 
	on fc.category_id = c3.category_id  --и затем по category_id находим название категории
	where left(c2.city,1) = 'a'  -- нам интересны только города, которые начинаются на a
	order by sum_rental desc -- сортируем по убыванию показателя и выбираем только первую категорию
	limit 1 )

	union all --объединяем с таким же запросом, но с условием для городов, содержащих символ '-'. 
	--Все то же самое, только отличается условие на c2.city
	(select distinct 'For cities contain -' as title, c3."name" , 
	sum(round(abs(extract(epoch from r.return_date - r.rental_date)/3600)::numeric,2)) over(partition by c3."name") as sum_rental
	from public.film as f
	left outer join
	public.inventory i 
	on f.film_id = i.film_id 
	left outer join 
	public.rental r 
	on i.inventory_id = r.inventory_id 
	left outer join 
	public.customer c 
	on r.customer_id = c.customer_id 
	left outer join 
	public.address a
	on c.address_id = a.address_id
	left outer join 
	public.city c2 
	on a.city_id = c2.city_id 
	left outer join 
	public.film_category fc 
	on f.film_id = fc.film_id 
	left outer join 
	public.category c3 
	on fc.category_id = c3.category_id 
	where c2.city like '%-%'
	order by sum_rental desc 
	limit 1) 
	
) result;

