import sys
from mysql import connector as mc
from load_dims_facts import load_dims_facts

def main():
    print("Classic Moldes DWH")

    load_type = sys.argv[1]

    if load_type == 'by_slice':
        slice = sys.argv[2]
        print('Load Type: by_slice Current slice: {}'.format(slice))
        load_dims_facts(slice)

    if load_type == 'historical':
        # Connecting to DB
        connection = mc.connect(user='root',
                                password='admin123',
                                host='localhost',
                                port='3307')
        cursor = connection.cursor()
        query = "select distinct orderdate from classicmodels.orders order by 1;"
        cursor.execute(query)
        slices = cursor.fetchall()

        for slice in slices:
            # print(slice[0])
            load_dims_facts(slice[0])
            print("\n")




if __name__ == '__main__':
    main()