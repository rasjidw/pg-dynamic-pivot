create temp table tmp_pivot_data on commit drop as
  select actor.actor_id, first_name, last_name, rating
    from actor
      join film_actor on actor.actor_id = film_actor.actor_id
      join film on film_actor.film_id = film.film_id;

select make_pivot_table('{"actor_id", "first_name", "last_name"}', 'rating', 'actor_id', 'count', 'tmp_pivot_data', 'tmp_pivot_result', false);

select * from tmp_pivot_result order by row_num;
