create temp table tmp_pivot_data on commit drop as
  select rental_id, extract(dow from rental_date)::integer as day_of_week, film.film_id, title  from rental
    join inventory on rental.inventory_id = inventory.inventory_id
    join film on inventory.film_id = film.film_id;

select make_pivot_table('{"title"}', 'day_of_week', 'film_id', 'count', 'tmp_pivot_data', 'tmp_pivot_result', false);

select * from tmp_pivot_result order by row_num;
