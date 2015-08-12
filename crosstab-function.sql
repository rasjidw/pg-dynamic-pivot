-- make_pivot_table
-- python version 0.9
-- last edited 2015-08-11 

create or replace function
 make_pivot_table(row_headers text[], category_field text, value_field text,
  value_action text, input_table text, output_table text, keep_result boolean)
returns void as
$$
## version 0.9 - last modified 2015-08-11 ##
# imports
from collections import defaultdict
import operator
import string

# constants
BATCH_SIZE = 100
VALID_ACTIONS = ('count', 'sum', 'min', 'max')
NULL_CATEGORY_NAME = 'NULL_CATEGORY'
TOTAL_COL = 'total'

# functions
def table_exists(tablename):
    plan = plpy.prepare("""select table_schema, table_name from
        information_schema.Tables where table_schema not in ('information_schema',
        'pg_catalog') and table_name = $1""", ["text"])
    rows = plpy.execute(plan, [input_table], 2)
    return bool(rows)

def make_rowkey(row):
    return tuple([row[header] for header in row_headers])

def quote_if_needed(value):
    return plpy.quote_literal(value) if isinstance(value, basestring) else str(value)

# assumes None is never a value in the dct
def update_if(dct, key, new_value, op, result=True):
    current_value = dct.get(key)
    if current_value is None or op(value, current_value) == result:
        dct[key] = new_value

def update_output_table(output_table, row_headers, colname, value):
    pg_value = plpy.quote_literal(value) if isinstance(value, basestring) else value
    sql = 'update %s set %s = %s where ' % (output_table, plpy.quote_ident(colname), 
                                            pg_value)
    conditions = []
    for index, row_header in enumerate(row_headers):
        conditions.append('%s = %s' % (plpy.quote_ident(row_header),
                                       quote_if_needed(rowkey[index])))
    sql += ' and '.join(conditions)
    plpy.execute(sql)


# -----------------

if not table_exists(input_table):
    plpy.error('input_table %s dones not exist' % input_table)

if value_action not in VALID_ACTIONS:
    plpy.error('%s is not a recognised action' % value_action)

# load the data into a dict
count_dict = defaultdict(int)
sum_dict = defaultdict(float)
total_dict = defaultdict(float)
min_dict = dict()
max_dict = dict()
categories_seen = set()
rowkeys_seen = set()
do_total = value_action in ('count', 'sum')
    
cursor = plpy.cursor('select * from %s' % plpy.quote_ident(input_table))
while True:
    rows = cursor.fetch(BATCH_SIZE)
    if not rows:
        break
    for row in rows:
        rowkey = make_rowkey(row)
        rowkeys_seen.add(rowkey)
        category = row[category_field]           
        value = row[value_field]
        dctkey = (rowkey, category)

        # skip if value field is null
        if value is None:
            continue

        categories_seen.add(category)
            
        if value_action == 'count':
	    count_dict[dctkey] += 1
	    total_dict[rowkey] += 1
	if value_action == 'sum':
            sum_dict[dctkey] += value
            total_dict[rowkey] += value
        if value_action == 'min':
            update_if(min_dict, dctkey, value, operator.lt)
        if value_action == 'max':
            update_if(max_dict, dctkey, value, operator.gt)
            
plpy.notice('seen %s summary rows and %s categories' % (len(rowkeys_seen),
                                                        len(categories_seen)))

# get the columns types
coltype_dict = dict()
input_type_sql = 'select * from %s where false' % plpy.quote_ident(input_table)
input_type_result = plpy.execute(input_type_sql)
for index, colname in enumerate(input_type_result.colnames()):
    coltype_num = input_type_result.coltypes()[index]
    coltype_sql = 'select typname from pg_type where oid = %s' % coltype_num
    coltype = list(plpy.cursor(coltype_sql))[0]
    plpy.notice('%s: %s' % (colname, coltype['typname']))
    coltype_dict[colname] = coltype['typname']
    
plpy.execute('drop table if exists %s' % plpy.quote_ident(output_table))
sql_parts = []
if keep_result:
    sql_parts.append('create table %s (' % plpy.quote_ident(output_table))
else:
    sql_parts.append('create temp table %s (' % plpy.quote_ident(output_table))

cols = []
for row_header in row_headers:
    cols.append('%s %s' % (plpy.quote_ident(row_header), coltype_dict[row_header]))

cat_type = 'bigint' if value_action == 'count' else coltype_dict[value_field]

for col in sorted(categories_seen):
    if col is None:
        cols.append('%s %s' % (plpy.quote_ident(NULL_CATEGORY_NAME), cat_type))
    else:
        cols.append('%s %s' % (plpy.quote_ident(col), cat_type))

if do_total:
    cols.append('%s %s' % (TOTAL_COL, cat_type))

sql_parts.append(',\n'.join(cols))
if keep_result:
    sql_parts.append(')')
else:
    sql_parts.append(') on commit drop')
plpy.execute('\n'.join(sql_parts))

dict_map = {'count': count_dict, 'sum': sum_dict, 'min': min_dict, 'max': max_dict }
value_dict = dict_map[value_action]
for rowkey in rowkeys_seen:
    sql = 'insert into %s values (' % plpy.quote_ident(output_table)
    sql += ', '.join([quote_if_needed(part) for part in rowkey])
    sql += ')'
    plpy.execute(sql)

if do_total:
    for rowkey, value in total_dict.iteritems():
        update_output_table(output_table, row_headers, TOTAL_COL, value)
        
for (rowkey, category), value in value_dict.iteritems():
    # put in cateogry value
    colname = NULL_CATEGORY_NAME if category is None else category
    update_output_table(output_table, row_headers, colname, value)

$$ language plpythonu
