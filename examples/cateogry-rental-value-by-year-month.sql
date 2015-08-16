create temp table tmp_pivot_data on commit drop as
  select rental_id, 
    extract(year from rental_date) || '-' || lpad(extract(month from rental_date)::text, 2, '0') as yr_mnth,
    film.film_id, title, rental_rate, category.name as cat_name  from rental
      join inventory on rental.inventory_id = inventory.inventory_id
      join film on inventory.film_id = film.film_id
      join film_category on film.film_id = film_category.film_id
      join category on film_category.category_id = category.category_id;

select make_pivot_table('{"cat_name"}', 'yr_mnth', 'rental_rate', 'sum', 'tmp_pivot_data', 'tmp_pivot_result', false);

select * from tmp_pivot_result order by row_num;
