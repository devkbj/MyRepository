import pymssql
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

CONNECTION_STRING = 'server=13.124.238.8,user=sa,password=ok1234%%66,database=adventureworks,charset=utf8'
connect_params = dict(entry.split('=') for entry in CONNECTION_STRING.split(','))
connect_params['autocommit'] = True
conn = pymssql.connect(**connect_params)


def get_db_type(sql):
    types = list()

    with conn.cursor(as_dict=True) as cursor:
        cursor.callproc('sp_describe_first_result_set', (sql, None, 0))
        selected_columns = ['name', 'system_type_name', 'max_length', 'precision', 'scale']

        for row in cursor:
            # "데이터형식(길이)" 에서 괄호가 포함되어 있으면 제거한다.
            type_name = row['system_type_name']
            row['system_type_name'] = type_name if type_name.find('(') < 0 else type_name[:type_name.find('(')]

            new_row = {c: row[c] for c in selected_columns}
            types.append(new_row)

    return types


def get_pa_type(sql_type, precision, scale):
    sql_type = sql_type.lower()

    if (sql_type == 'bigint'):
        return pa.int64()
    elif (sql_type == 'numeric'):
        # return pa.decimal128(precision, scale)
        return pa.float64()
    elif (sql_type == 'float'):
        return pa.float64()
    elif (sql_type == 'bit'):
        return pa.bool_()
    elif (sql_type == 'smallint'):
        return pa.int16()
    elif (sql_type == 'decimal'):
        return pa.decimal128(precision, scale)
    elif (sql_type == 'smallmoney'):
        return pa.float64()
    elif (sql_type == 'int'):
        return pa.int32()
    elif (sql_type == 'tinyint'):
        return pa.int8()
    elif (sql_type == 'money'):
        return pa.float64()
    elif (sql_type == 'datetime'):
        return pa.timestamp('us')
    elif (sql_type.find('char') >= 0):
        return pa.string()
    else:
        raise Exception('Unknown SQL data type - {}'.format(sql_type))


def make_pa_schema(sql):
    db_type = get_db_type(sql)
    pa_schema = list()

    for t in db_type:
        col_name = t['name']
        data_type = t['system_type_name']
        precision = t['precision']
        scale = t['scale']

        pa_schema.append(pa.field(col_name, get_pa_type(data_type, precision, scale)))

    return pa.schema(pa_schema)


def main():
    sql = 'SELECT * FROM DimPromotion'
    df = pd.read_sql(sql=sql, con=conn)
    schema = make_pa_schema(sql)

    table = pa.Table.from_pandas(df, schema=schema)
    pq.write_table(table, 'dimpromotion.parquet')

if __name__ == '__main__':
    main()
