from mysql import connector as mc
import json
import csv
import copy
import avro
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

connection = mc.connect(user='root',
                        password='admin123',
                        host='localhost',
                        port='3307')

def readDB_writeJson(table, filter, limit=0):
    cursor = connection.cursor()
    print(table)
    filter_string = ','.join(str(v) for v in filter)
    if limit > 0:
        query = "select * from {} where ordernumber in ({})".format(table, filter_string)
    else:
        query = "select * from {}".format(table)
    cursor.execute(query)
    columns = cursor.description
    # print(columns[0][0])
    result = [{columns[index][0]:str(column) for index, column in enumerate(value)} for value in cursor.fetchall()]

    # print(result)
    # write = []
    # for row in result:
    # print(json.dumps(row))

    path_file = "/home/victor/IdeaProjects/classicmodelsDWH/classicModelsDWH_RealTime/resources/{}.json".format(table.split(".")[1])
    with open(path_file, "w") as write_file:
        json.dump(result, write_file)

def db_to_csv(table):
    cursor = connection.cursor()

    if table != "classicmodels.products":
        query = "select * from {} ".format(table)
    else:
        query = """
        SELECT productCode, productName, productLine, productScale, productVendor, 
        replace(replace(productDescription, '\n', ''), '\r', '') as productDescription,
        quantityInStock, buyPrice, MSRP
        FROM classicmodels.products
        /* where productCode = 'S10_4698' */
        """.format(table)

    cursor.execute(query)
    headers = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    print(headers)

    path_file = "/home/victor/IdeaProjects/classicmodelsDWH/classicModelsDWH_RealTime/resources/{}.csv".format(table.split(".")[1])
    with open(path_file, 'w') as file:
        f = csv.writer(file, delimiter=',', quotechar='"', escapechar=' ', quoting=csv.QUOTE_NONNUMERIC, lineterminator='\n')
        f.writerow(headers)

        for row in rows:
            f.writerow(str(r) for r in row)

    # print(rows[0][5])
    # for row in rows:
    #     print(row)


    # f.close()

    # print("rows: {} written succesfully to file:{}".format(len(rows), f.name))

def db_to_avro(table):
    cursor = connection.cursor()
    query = "select * from {}".format(table)
    cursor.execute(query)

    result = cursor.fetchall()

    return result

def avro_test():
    schema = {
        'name': 'avro.example.User',
        'type': 'record',
        'fields': [
            {'name': 'name', 'type': 'string'},
            {'name': 'age', 'type': 'int'}
        ]
    }

    schema_parsed = avro.schema.Parse(json.dumps(schema))
    with open('/home/victor/IdeaProjects/classicmodelsDWH/classicModelsDWH_RealTime/users.avro', 'wb') as f:
        writer = DataFileWriter(f, DatumWriter(), schema_parsed)
        writer.append({'name': 'Victor Garcia', 'age': 99})
        writer.append({'name': 'Victor Huguito', 'age': 7})
        writer.close()

    with open('/home/victor/IdeaProjects/classicmodelsDWH/classicModelsDWH_RealTime/users.avro', 'rb') as f:
        reader = DataFileReader(f, DatumReader())
        metadata = copy.deepcopy(reader.meta)
        schema_from_file = metadata['avro.schema']
        users = [user for user in reader]
        print("Users from file")
        for user in reader:
            print(user)
        reader.close()

    print('Schema that we specified:\n {}'.format(schema))
    print('Schema that we parsed:\n {}'.format(schema_parsed))
    print('Schema from users.avro file:\n {}'.format(schema_from_file))
    # print('Users:\n {}'.format(users))
    # print(schema_parsed)

# avro_test()

# db_to_csv("classicmodels.products")
# db_to_csv("classicmodels.customers")
# db_to_csv("classicmodelsDWH.dim_time")
# readDB_writeJson("classicmodels.orders", (10100, 10101, 10102))
# readDB_writeJson("classicmodels.orderdetails", (10100, 10101, 10102))

products = db_to_avro("classicmodels.products")
for product in products:
    print(product)



connection.close()

# classicmodels.orderdetails


# values = [enumerate(value) for value in cursor.fetchall()]
#
# for row in values:
#     print(row)
#     for index, column in row:
#         # print(index, column)
#         print({columns[index][0]:str(column)})



# values = [value for value in cursor.fetchall()]
#
# for row in values:
#     print(enumerate(row))
#     for r in enumerate(row):
#         print(r)

# result_list = [enumerate(value) for value in cursor.fetchall()]
# print(type(result_list))
#
# for id, value in result_list:
#     print(id, value)

# values = ["a", "b", "c"]
# for id, value in enumerate(values):
#     print(id, value)


# result = cursor.fetchall()
#
# cursor.with_rows
# print(type(result))
#


# from     orders as o
# orderdetails as od
