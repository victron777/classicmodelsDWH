import json
import re
import csv
#
def orderdetails_agg():
    with open("/home/victor/IdeaProjects/classicmodelsDWH/classicModelsDWH_RealTime/resources/orderdetails.json", "r") as read_file:
        orderdetails = json.load(read_file)

    orderdetails_raw={}
    for row in orderdetails:
        if row["orderNumber"] not in orderdetails_raw:
            orderdetails_raw[row["orderNumber"]] = []
            orderdetails_raw[row["orderNumber"]].append(int(row["quantityOrdered"])*float(row["priceEach"]))
        else:
            orderdetails_raw[row["orderNumber"]].append(int(row["quantityOrdered"])*float(row["priceEach"]))

    orderdetails_agg = {}
    for row in orderdetails_raw:
        orderdetails_agg[row] = {"orderNumber": row, "total_amount": round(sum(orderdetails_raw[row]),2), "avg_amount": round(sum(orderdetails_raw[row])/len(orderdetails_raw[row]),2)}

    return orderdetails_agg



def orderdetails():
    with open("/home/victor/IdeaProjects/classicmodelsDWH/classicModelsDWH_RealTime/resources/orderdetails.json", "r") as read_file:
        orderdetails = json.load(read_file)

    return orderdetails


def orders():
    with open("/home/victor/IdeaProjects/classicmodelsDWH/classicModelsDWH_RealTime/resources/orders.json", "r") as read_file:
        orders_raw = json.load(read_file)

    orders_d = {}
    for order in orders_raw:
        orders_d[order["orderNumber"]] = order

    return orders_d

    print("orders join orderdetails: ")
    for order in orders_raw:
        # print(order)
        # join_result = dict(order, orderdetails_agg[order["orderNumber"]])
        print(order, ' -> ', orderdetails_agg[order["orderNumber"]])

def order_merge():
    order_details = orderdetails()
    orders_raw = orders()
    orders_merge = []
    for orderdetail in order_details:
        orders_merge.append([orderdetail["orderNumber"],[orderdetail, orders_raw[orderdetail["orderNumber"]]]])

    # for order_merge in orders_merge:
    #     print(order_merge)
    #     print(order_merge[1][0])
        # print(order_merge[0], orders_merge[1][0])
    # print("orders")
    # orders_raw = orders()
    # print(type(orders_raw))
    # for order in orders_raw.items():
    #     print(order)
    return orders_merge


def read_products():
    with open("/home/victor/IdeaProjects/classicmodelsDWH/classicModelsDWH_RealTime/resources/products.csv", "r") as products_raw:
        columns = list(products_raw.readline().split(","))
        # print(columns)
        product_d = {}

        for row in products_raw:
            row_s = re.split(r',(?=")', row)
            product_d[row_s[0]] = {columns[0]: row_s[0], columns[1]: row_s[1], columns[2]: row_s[2], columns[3]: row_s[3], columns[4]: row_s[4], columns[5]: row_s[5], columns[6]: row_s[6], columns[7]: row_s[7]}

    return product_d


def read_cutomers():
    with open("/home/victor/IdeaProjects/classicmodelsDWH/classicModelsDWH_RealTime/resources/customers.csv", "r") as customers_raw:
        columns = list(customers_raw.readline().split(","))
        # print(columns)

        customers_d = {}
        for row in customers_raw:
            row_s = row.split(",")
            customers_d[row_s[0]] = {columns[0]: row_s[0],
                                     columns[1]: row_s[1],
                                     columns[2]: row_s[2],
                                     columns[3]: row_s[3],
                                     columns[4]: row_s[4],
                                     columns[5]: row_s[5],
                                     columns[6]: row_s[6],
                                     columns[7]: row_s[7],
                                     columns[8]: row_s[8],
                                     columns[9]: row_s[9],
                                     columns[10]: row_s[0],
                                     columns[11]: row_s[1]}

    return customers_d


print("fact orders")
order_odetails = order_merge()
products = read_products()
customers = read_cutomers()

orders_products = {}
for orders in order_odetails:
    order_customer = str('"' + order_odetails[1][1][1]["customerNumber"] + '"')
    order_product = str('"' + order_odetails[0][1][0]["productCode"] + '"')

    orders[1].append({**customers[order_customer]})
    orders[1].append({**products[order_product]})

    # print(orders[0], orders[1][0], orders[1][1], orders[1][2], orders[1][3])
    orders_products[orders[0]] = [orders[1][0], orders[1][1], orders[1][2], orders[1][3]]

# print(orders_products['10102'])
for row in orders_products.items():
    print(row)



# print(order_odetails['10123'][1][1]["customerNumber"])
# for product in products:
#     print(product[])

# print(order_odetails)
# print(order_odetails)
# for order_od in order_odetails:
    # print(order_od[1][1]["customerNumber"])
    # print(order_od[1][0]["productCode"])

# print("customers")
# customers = read_cutomers()
# # for customer in customers.items():
# #     print(customer)
# print(customers['"424"'])


# print("orderdetails_agg")
# orderdetails_agg = orderdetails_agg()
# for orderdetail in orderdetails_agg.items():
#     print(orderdetail)

# print("orderdetails")
# orderdetails = orderdetails()
# for orderdetail in orderdetails:
#     print(orderdetail)
#
# print("orders")
# orders_raw = orders()
# print(type(orders_raw))
# for orders in orders_raw.items():
#     print(orders)

# order_odetails = order_merge()
# for order_od in order_odetails:
#     print(order_od)

# print("products")
# products = read_products()
# for product in products.items():
#     print(product)
#
# print("Selected: ", products['"S10_4698"']['"productDescription"'])










######### PRODUCTS TESTING ##########
# columns = list(products_raw.readline().split(","))
# products_d = [ dict(( (columns[i].replace("\n",""), str(value).replace("\n",""))  ) for i, value in enumerate(re.split(r',(?=")', row))) for row in products_raw ]

# print(products_raw)
# f = csv.reader(products_raw, delimiter=',')
#
# print(f)
# for row in f:
#     print(row)

# print(list(products_raw)[1])
# products = re.split(r',(?=")', products_raw)
# print(products)

# products_d = [dict((columns[i].replace("\n",""), str(value).replace("\n","")) for i, value in enumerate(re.split(r',(?=")', row))) for row in products_raw]
# print(products_d)

# products_d = {}
# for product in products_raw:
#     # print(columns[0], product)
#     p = re.split(r',(?=")', product)
#     # print(p[0])
#     products_d[p[0]] = {columns[0] : p[0], columns[1] : str(p[1])}

# di = [[(columns[i], value) for i, value in enumerate(re.split(r',(?=")', row))] for row in products_raw]
# print(di)

# di = [[(i, value) for i, value in enumerate(columns)] for row in products_raw]
# print(di)

# di = [ dict((columns[i] = p[i])) for i in range(len(columns))]
# print(di)

# i = [dict( (columns[c], p[c])) for c in range(len(columns))]
# print(i)
# for product in products_raw:
#     print(re.split(r',(?=")', product))

# print(products_d)
######### PRODUCTS TESTING ##########

