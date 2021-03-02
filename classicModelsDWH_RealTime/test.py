import json

# a = {"one": 1, "two": 2, "three": 3, "four": 4}
# b = {"five": 5, "six": 6, "seven": 7, "eight": 8}
# r = {**a, **b}
# print(r)
# print(b["six"])

f1 = '[{ "id":"1", "name":"Tom" }, { "id":"2", "name":"Jim" }, { "id":"3", "name":"Bob" }, { "id":"4", "name":"Jeny" },  { "id":"5", "name":"Lara" }, { "id":"6", "name":"Lin" }, { "id":"7", "name":"Kim" }, { "id":"8", "name":"Jack" }, { "id":"9", "name":"Tony" }]'
f2 = '[{ "id":"1", "Details":[ { "label":"jcc", "hooby":"Swimming" }, { "label":"hkt", "hooby":"Basketball" } ] },{ "id":"2", "Details":[ { "label":"NTC", "hooby":"Games" }]}]'
a = json.loads(f1)
b = json.loads(f2)

# print(a[0])
# print(b[1])

# First method O(M * N) where M=len(a) and N=len(b)
list_a = []
# for i in range(len(a)):
#     for n in range(len(b)):
#         if b[n]["id"] == a[i]["id"]:
#             list_a.append(dict(b[n], **a[i]))
#
# for row in list_a:
#     print(row)
#     # print(row["name"])

# Second method
list_a = []
aid = {d['id']: d for d in a}
print("aid -> ", aid)

list_a = [{k: v for d in (b_dict, aid[b_dict['id']]) for k, v in d.items()}
          for b_dict in b if b_dict['id'] in aid]

b_dict_r = [b_dict for b_dict in b if b_dict["id"] in aid]
print("b_dict_r -> ", b_dict_r)

# print(aid[b_dict_r[0]['id']])
for d in b_dict_r:
    print(d, aid[d["id"]])
# d_r = [d for d in (b_dict_r, aid[b_dict_r['id']])]
# print("d_r -> ", d_r)

#
# for row in list_a:
#     print(row)
    # print(row["name"])