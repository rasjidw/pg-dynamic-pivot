create temp table tmp_pivot_data on commit drop as
  select actor.actor_id, first_name, last_name, category.name as cat_name
    from actor
      join film_actor on actor.actor_id = film_actor.actor_id
      join film on film_actor.film_id = film.film_id
      join film_category on film.film_id = film_category.film_id
      join category on film_category.category_id = category.category_id;

select make_pivot_table('{"actor_id", "first_name", "last_name"}', 'cat_name', 'actor_id', 'count', 'tmp_pivot_data', 'tmp_pivot_result', false);

select * from tmp_pivot_result order by row_num;
