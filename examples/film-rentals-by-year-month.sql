create temp table tmp_pivot_data on commit drop as
  select rental_id, 
    extract(year from rental_date) || '-' || lpad(extract(month from rental_date)::text, 2, '0') as yr_mnth,
    film.film_id, title, rental_rate  from rental
      join inventory on rental.inventory_id = inventory.inventory_id
      join film on inventory.film_id = film.film_id;

select make_pivot_table('{"title"}', 'yr_mnth', 'film_id', 'count', 'tmp_pivot_data', 'tmp_pivot_result', false);

select * from tmp_pivot_result order by row_num;
