import json


with open("/home/victor/IdeaProjects/classicmodelsDWH/classicModelsDWH_RealTime/resources/orderdetails.json", "r") as read_file:
    orderdetails = json.load(read_file)

# print(data[0]["orderNumber"])
#

# for row in data:
#     print(row)

d={}
for row in orderdetails:
    # print(row)
    # print(row['orderNumber'])
    if row["orderNumber"] not in d:
        d[row["orderNumber"]] = []
        d[row["orderNumber"]].append(int(row["quantityOrdered"])*float(row["priceEach"]))
    else:
        d[row["orderNumber"]].append(int(row["quantityOrdered"])*float(row["priceEach"]))

print(d)
# print(d['10100'])
# print(round(sum(d['10100']),2))

result = {}
for row in d:
    result[row] = {"orderNumber": row, "total_amount": round(sum(d[row]),2), "avg_amount": round(sum(d[row])/len(d[row]),2)}

print(result, '\n')
# print(result['10100']["orderNumber"], result['10100']["total_amount"])

with open("/home/victor/IdeaProjects/classicmodelsDWH/classicModelsDWH_RealTime/resources/orders.json", "r") as read_file:
    orders = json.load(read_file)

for order in orders:
    # print(order)
    print(order, ' -> ', result[order["orderNumber"]])


