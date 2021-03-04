import json


with open("/home/victor/IdeaProjects/classicmodelsDWH/classicModelsDWH_RealTime/resources/orderdetails.json", "r") as read_file:
    orderdetails = json.load(read_file)

orderdetails_raw={}
for row in orderdetails:
    # print(row)
    # print(row['orderNumber'])
    if row["orderNumber"] not in orderdetails_raw:
        orderdetails_raw[row["orderNumber"]] = []
        orderdetails_raw[row["orderNumber"]].append(int(row["quantityOrdered"])*float(row["priceEach"]))
    else:
        orderdetails_raw[row["orderNumber"]].append(int(row["quantityOrdered"])*float(row["priceEach"]))

print("orderdetails_raw: ",orderdetails_raw)
# print(d['10100'])
# print(round(sum(d['10100']),2))
#
orderdetails_agg = {}
for row in orderdetails_raw:
    orderdetails_agg[row] = {"orderNumber": row, "total_amount": round(sum(orderdetails_raw[row]),2), "avg_amount": round(sum(orderdetails_raw[row])/len(orderdetails_raw[row]),2)}

print("orderdetails_agg: ",orderdetails_agg, '\n')
# # print(result['10100']["orderNumber"], result['10100']["total_amount"])
#
with open("/home/victor/IdeaProjects/classicmodelsDWH/classicModelsDWH_RealTime/resources/orders.json", "r") as read_file:
    orders_raw = json.load(read_file)

print("orders join orderdetails: ")
for order in orders_raw:
    # print(order)
    # join_result = dict(order, orderdetails_agg[order["orderNumber"]])
    print(order, ' -> ', orderdetails_agg[order["orderNumber"]])


