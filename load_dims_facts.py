from mysql import connector as mc


def load_dims_facts(slice: str):
    print('Load Type: by_slice Current slice: {}'.format(slice))
    # Connecting to DB
    connection = mc.connect(user='root',
                            password='admin123',
                            host='localhost',
                            port='3307')
    cursor = connection.cursor()

    print("Loading dim_products on slice {}".format(slice))

    query = """
    INSERT INTO classicmodelsDWH.dim_products(productcode,productname,productline,productvendor,textDescription,ds)(
    select 	p.productcode,
            p.productname,
            p.productline,
            p.productvendor,
            pl.textDescription,
            now() as `ds`
    from 	classicmodels.products p
            left join classicmodels.productlines pl 
                on(p.productline = pl.productline)
            inner join classicmodels.orderdetails od
                   on( p.productcode = od.productcode)
            left join classicmodels.orders o 
                   on( od.ordernumber = o.ordernumber)
            left join classicmodelsDWH.dim_products dp
                   on( p.productcode = dp.productcode )
    where 	o.orderdate = %s and
            dp.productcode is null
    )    
    """
    cursor.execute(query, (slice,))

    # Load dim_customers
    print("Loading dim_customers on slice {}".format(slice))

    # Truncate table dim_customers_stg
    query = """
    TRUNCATE TABLE classicmodelsDWH.dim_customers_stg
    """
    cursor.execute(query)

    # Load dim_customers_stg
    query = """
    INSERT INTO classicmodelsDWH.dim_customers_stg (customernumber, customername, contactLastName, contactFirstName, phone, employeeNumber, emp_lastname, emp_firstname, ds)(
    select 	c.customernumber,
            c.customername,
            c.contactLastName,
            c.contactFirstName,
            c.phone,
            e.employeeNumber,
            e.lastName as emp_lastname,
            e.firstName as emp_firstname,
            now() as ds
    from 	classicmodels.customers c 
            left join classicmodels.employees e
                on(c.salesrepemployeenumber = e.employeenumber)
            inner join classicmodels.orders as o
                on( c.customernumber = o.customernumber)
            left join classicmodelsDWH.dim_customers as dc
                on( c.customernumber = dc.customernumber )
    where 	o.orderdate = %s 
    )  
    """
    # and dc.customernumber is null
    cursor.execute(query, (slice,))

    # SCD Type 2 - Update existing records
    query = """
    UPDATE classicmodelsDWH.dim_customers
    INNER JOIN classicmodelsDWH.dim_customers_stg ON(classicmodelsDWH.dim_customers.customernumber = classicmodelsDWH.dim_customers_stg.customernumber)
    SET 
    classicmodelsDWH.dim_customers.end_date = CASE
        WHEN (classicmodelsDWH.dim_customers.phone <> classicmodelsDWH.dim_customers_stg.phone OR
             classicmodelsDWH.dim_customers.employeenumber <> classicmodelsDWH.dim_customers_stg.employeenumber OR
             classicmodelsDWH.dim_customers.emp_lastname <> classicmodelsDWH.dim_customers_stg.emp_lastname OR
             classicmodelsDWH.dim_customers.emp_firstname <> classicmodelsDWH.dim_customers_stg.emp_firstname) THEN now()
        ELSE classicmodelsDWH.dim_customers.end_date
        END,
    classicmodelsDWH.dim_customers.active = CASE
        WHEN (classicmodelsDWH.dim_customers.phone <> classicmodelsDWH.dim_customers_stg.phone OR
             classicmodelsDWH.dim_customers.employeenumber <> classicmodelsDWH.dim_customers_stg.employeenumber OR
             classicmodelsDWH.dim_customers.emp_lastname <> classicmodelsDWH.dim_customers_stg.emp_lastname OR
             classicmodelsDWH.dim_customers.emp_firstname <> classicmodelsDWH.dim_customers_stg.emp_firstname) THEN 0
        ELSE 1
        END
    """
    cursor.execute(query)

    # SCD Type 2 - Insert new records
    query = """
    INSERT INTO classicmodelsDWH.dim_customers (customernumber, customername, contactLastName, contactFirstName, phone, employeenumber, emp_lastname, emp_firstname, ds, start_date, end_date, active)
    SELECT 	cstg.customernumber, cstg.customername, cstg.contactLastName, cstg.contactFirstName, cstg.phone, cstg.employeenumber, cstg.emp_lastname, cstg.emp_firstname, cstg.ds,
            now() ,STR_TO_DATE('9999-12-12','%Y-%m-%dT%H:%i:%s'), 1 
    FROM 	classicmodelsDWH.dim_customers_stg cstg 
            left join classicmodelsDWH.dim_customers c
                    on(cstg.customernumber = c.customernumber)
    where 	c.customernumber is null OR
            (c.customernumber is not null AND
                (
                c.phone <> cstg.phone OR
                c.employeenumber <> cstg.employeenumber OR
                c.emp_lastname <> cstg.emp_lastname OR
                c.emp_firstname <> cstg.emp_firstname
                )
            )
    """
    cursor.execute(query)

    # Load fact_orders
    print("Loading fact_orders on slice {}".format(slice))
    query = """
    DELETE  classicmodelsDWH.fact_orders.*
    FROM    classicmodelsDWH.fact_orders
            LEFT JOIN classicmodelsDWH.dim_time
                    ON(classicmodelsDWH.fact_orders.order_date_id = classicmodelsDWH.dim_time.date_id)
    WHERE classicmodelsDWH.dim_time.fulldate = %s;
    """
    cursor.execute(query, (slice,))

    query = """
    INSERT INTO classicmodelsDWH.fact_orders (product_id, customer_id, ordernumber, order_date_id, required_date_id, shipped_date_id, status, orderlinenumber, quantityordered, priceeach, total_cost, ds)(
    SELECT 	   dp.product_id,
               dc.customer_id,
               o.ordernumber,
               dt_order.date_id as order_date_id,
               dt_required.date_id as required_date_id,
               dt_shipped.date_id as shipped_date_id,
               o.status,
               od.orderlinenumber,
               od.quantityordered,
               od.priceeach,
               od.quantityordered * od.priceeach AS total_cost,
               now() as ds
    FROM   	classicmodels.orders AS o
               LEFT JOIN classicmodels.orderdetails as od
                      ON( o.orderNumber = od.orderNumber )
               LEFT JOIN classicmodelsDWH.dim_products AS dp
                      ON( od.productcode = dp.productcode )
               LEFT JOIN classicmodelsDWH.dim_customers AS dc
                      ON( o.customernumber = dc.customernumber )
               LEFT JOIN classicmodelsDWH.dim_time dt_order
                      ON( o.orderdate = dt_order.fulldate)
               LEFT JOIN classicmodelsDWH.dim_time dt_required
                      ON( o.requireddate = dt_required.fulldate )
               LEFT JOIN classicmodelsDWH.dim_time dt_shipped
                      ON( o.shippeddate = dt_shipped.fulldate )
    WHERE 	   o.orderdate = %s
    );
    """
    cursor.execute(query, (slice,))

    connection.commit()
    connection.close()

    print("Process complete!!!")
