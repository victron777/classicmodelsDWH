from mysql import connector as mc
import json

connection = mc.connect(user='root',
                        password='admin123',
                        host='localhost',
                        port='3307')

def readDB_writeJson(table, filter):
    cursor = connection.cursor()
    print(table)
    filter_string = ','.join(str(v) for v in filter)
    query = "select * from {} where ordernumber in ({})".format(table, filter_string)
    cursor.execute(query)
    columns = cursor.description
    # print(columns[0][0])
    result = [{columns[index][0]:str(column) for index, column in enumerate(value)} for value in cursor.fetchall()]

    print(result)
    # write = []
    # for row in result:
        # print(json.dumps(row))

    path_file = "/home/victor/IdeaProjects/classicmodelsDWH/classicModelsDWH_RealTime/resources/{}.json".format(table.split(".")[1])
    with open(path_file, "w") as write_file:
        json.dump(result, write_file)

def db_to_csv(table):
    cursor = connection.cursor()
    query = "select * from {}".format(table)
    cursor.execute(query)
    headers = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    print(headers)

    path_file = "/home/victor/IdeaProjects/classicmodelsDWH/classicModelsDWH_RealTime/resources/{}.csv".format(table.split(".")[1])
    f = open(path_file, 'w')

    f.write(','.join(headers) + '\n')

    # for row in rows:
    #     print(','.join(str(r) for r in row))
    for row in rows:
        f.write(','.join(str(r) for r in row) + '\n')

    f.close()

    print("rows: {} written succesfully to file:{}".format(len(rows), f.name))


db_to_csv("classicmodels.products")
db_to_csv("classicmodels.customers")
db_to_csv("classicmodelsDWH.dim_time")
# readDB_writeJson("classicmodels.orders", (10100, 10101, 10102))
# readDB_writeJson("classicmodels.orderdetails", (10100, 10101, 10102))


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


# from 	orders as o
# orderdetails as od