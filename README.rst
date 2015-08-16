=======================================
PostgreSQL Dynamic Pivot Table Function 
=======================================

* Free software: Simplified BSD license

Documentation
-------------

The Dynamic Pivot Table function here is a plpythonu function to dynamically create 'Pivot Table' data directly in PostgreSQL using standard tools like pgAdmin III.

A simple example:

::

    CREATE TEMP TABLE
      test_data (section   text,
                 status    text,
                 val       integer)
      ON COMMIT DROP;

    INSERT INTO test_data VALUES 
      ('A', 'Active', 1), ('A', 'Inactive', 2),
      ('B', 'Active', 4), ('B', 'Inactive', 5),
                          ('C', 'Inactive', 7);

    SELECT make_pivot_table('{"section"}', 'status', 'val', 'sum',
                            'test_data', 'tmp_data_out', False);

    SELECT * FROM tmp_data_out ORDER BY row_num;


This will give the output:

::

     row_num | section | Active | Inactive | total
    ---------+---------+--------+----------+-------
           1 | A       |      1 |        2 |     3
           2 | B       |      4 |        5 |     9
           3 | C       |        |        7 |     7

Note: In the above example if using psql you will need to pass --single-transaction (or -1) due to the use of ON COMMIT DROP.

Currently supported actions are: count, sum, min and max.

If keep_result is False the output table will be dropped on commit.


Sample Data
-----------

The Pagila sample data is from http://pgfoundry.org/projects/dbsamples/

Released under the 3 clause BSD license according to http://pgfoundry.org/frs/shownotes.php?group_id=1000150&release_id=998
