	

-----------------------����� 1-------------------------
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
-----------------------����� 1-------------------------

----------------����� 2-----------------

--��� ������ "��� ������ ������ ����� ����������" ������� - ���������� ������������ � ������ ����� ������� (�� ������������ ������, � ����������)
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
----------------����� 2-----------------


----------------����� 3-----------------
select "name"
from
(select distinct  c3."name", sum(p.amount)  
	from public.film as f
	left outer join
	public.inventory i 
	on f.film_id = i.film_id --������� ��� ������� ������ ����� inventory_id (��� ��������� � ������ �����)
	left outer join 
	public.rental r 
	on i.inventory_id = r.inventory_id --������� � �������� rental, ����� ����� �� rental_id ����� ������
	left outer join 
	public.payment p 
	on r.rental_id = p.rental_id -- �� rental_id ������� � payments, ����� ������� ������ ����� payment
	left outer join 
	public.film_category fc --����������, ��� ��� ����� ��������� �������, ������� � �������� film_category, ����� ����� category_id
	on f.film_id = fc.film_id 
	left outer join 
	public.category c3 
	on fc.category_id = c3.category_id  --� ����� �� category_id ������� �������� ���������
 group by c3."name"
 order by sum(p.amount) desc
 limit 1) result;
 ----------------����� 3-----------------

----------------����� 4-----------------
select f.film_id, f2.title 
from public.film as f
left outer join
public.inventory as inv
On f.film_id = inv.film_id 
left outer join 
public.film f2 
on f.film_id = f2.film_id 
Where inv.film_id is null;
----------------����� 4-----------------

-----------------����� 5------------------
--
select result2.actor_id, first_name, last_name
from
(
  --����� ������� ����� ����� �� ����� � ������ � ����������� �������
	select distinct actor_id, cnt,
	dense_rank() over (order by cnt desc) as pos
	from 
	(
    --����� ������� � �������� ������� �� ��������� 'Children' ������� ������
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
--������� �������, � ������� ���� <=3, �� �������� �����

---------------����� 5---------------------

-----------------����� 6--------------------
--�a - ���������� �������� ��������
--cna - ����������� ���������� ��������

--�� ��� �a ����� ���� is NULL, ���� ��� ������ �� ������� city �� ������� ������������ ������ � ������� address, ���� ���� �� ���������� ������ ��� ������� � ������� customer

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

-----------------����� 6--------------------


-----------------����� 7----------------------

select title, "name"

from 
(
	(select distinct 'For cities started with a' as title, c3."name" , -- ���� title �������� ������, ����� ��������� ������ �������
	sum(round(abs(extract(epoch from r.return_date - r.rental_date)/3600)::numeric,2)) over(partition by c3."name") as sum_rental 
	-- ����� ��������� ���-�� ����� ������ (return_date - rental_date) �� ������ ���������
	from public.film as f
	left outer join
	public.inventory i 
	on f.film_id = i.film_id --������� ��� ������� ������ ����� inventory_id (��� ��������� � ������ �����)
	left outer join 
	public.rental r 
	on i.inventory_id = r.inventory_id --����� ����� ������� rental ������� ����������, ������� ���������� ���� � ������ ����� inventory_id
	left outer join 
	public.customer c 
	on r.customer_id = c.customer_id -- ������� ���������� customer_id
	left outer join 
	public.address a
	on c.address_id = a.address_id -- ����� ������� ����� ����������
	left outer join 
	public.city c2 
	on a.city_id = c2.city_id -- ����� ����� ������� �����
	left outer join 
	public.film_category fc --����������, ��� ��� ����� ��������� �������, ������� � �������� film_category, ����� ����� category_id
	on f.film_id = fc.film_id 
	left outer join 
	public.category c3 
	on fc.category_id = c3.category_id  --� ����� �� category_id ������� �������� ���������
	where left(c2.city,1) = 'a' -- ��� ��������� ������ ������, ������� ���������� �� a
	order by sum_rental desc -- ��������� �� �������� ���������� � �������� ������ ������ ���������
	limit 1 )

	union all --���������� � ����� �� ��������, �� � �������� ��� �������, ���������� ������ '-'. 
	--��� �� �� �����, ������ ���������� ������� �� c2.city
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

