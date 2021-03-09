from mysql import connector as mc

connection = mc.connect(user='root',
                        password='admin123',
                        host='localhost',
                        port='3307')

cursor = connection.cursor()
query = "select * from classicmodels.products"
# query = "select * from classicmodels.products_target"
# query = "truncate table classicmodels.products_target"
# query = "create table if not exists classicmodels.products_target select * from classicmodels.products"


cursor.execute(query)
data = cursor.fetchall()
columns = cursor.column_names
# connection.rollback()

cols = [col for col in columns]
cols_string = ', '.join(cols)
print("cols: ", cols_string)

col_values = tuple(map(lambda column: column.replace(column, '%s'), columns))
col_values_string = ', '.join(col_values)
print(col_values)

# for row in data:
#     print(row)
#

qry_insert = "INSERT INTO {} ({}) VALUES ({})".format("classicmodels.products_target", cols_string, col_values_string)
print(qry_insert)
batch_size = 20
count = 1
recs = []

for row in data:
    recs.append(row)
    if count % batch_size == 0:
        cursor.executemany(qry_insert, recs)
        connection.commit()
        recs = []

    count += 1

cursor.executemany(qry_insert, recs)
connection.commit()
connection.close()