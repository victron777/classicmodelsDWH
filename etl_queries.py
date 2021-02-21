from datetime import date

# import MySQL connector
import mysql.connector

# connect to server
connection = mysql.connector.connect(user='', password='', host='127.0.0.1')
print('Connected to database.')

# 1. update dimensions

# 1.1 update dim_time
# date - yesterday
yesterday = date.fromordinal(date.today().toordinal()-1)
yesterday_str = '"' + str(yesterday) + '"'
# test if date is already in the table
cursor = connection.cursor()
query = (
        "SELECT COUNT(*) "
        "FROM subscription_dwh.dim_time "
        "WHERE time_date = " + yesterday_str)
cursor.execute(query)
result = cursor.fetchall()
yesterday_subscription_count = int(result[0][0])
if yesterday_subscription_count == 0:
    yesterday_year = 'YEAR("' + str(yesterday) + '")'
    yesterday_month = 'MONTH("' + str(yesterday) + '")'
    yesterday_week = 'WEEK("' + str(yesterday) + '")'
    yesterday_weekday = 'WEEKDAY("' + str(yesterday) + '")'
    query = (
            "INSERT INTO subscription_dwh.`dim_time`(`time_date`, `time_year`, `time_month`, `time_week`, `time_weekday`, `ts`) "
            " VALUES (" + yesterday_str + ", " + yesterday_year + ", " + yesterday_month + ", " + yesterday_week + ", " + yesterday_weekday + ", Now())")
    cursor.execute(query)

# 1.2 update dim_city
query = (
    "INSERT INTO subscription_dwh.`dim_city`(`city_name`, `postal_code`, `country_name`, `ts`) "
    "SELECT city_live.city_name, city_live.postal_code, country_live.country_name, Now() "
    "FROM subscription_live.city city_live "
    "INNER JOIN subscription_live.country country_live ON city_live.country_id = country_live.id "
    "LEFT JOIN subscription_dwh.dim_city city_dwh ON city_live.city_name = city_dwh.city_name AND city_live.postal_code = city_dwh.postal_code AND country_live.country_name = city_dwh.country_name "
    "WHERE city_dwh.id IS NULL")
cursor.execute(query)

print('Dimension tables updated.')


# 2. update facts

# 2.1 update customers subscribed
# delete old data for the same date
query = (
        "DELETE subscription_dwh.`fact_customer_subscribed`.* "
        "FROM subscription_dwh.`fact_customer_subscribed` "
        "INNER JOIN subscription_dwh.`dim_time` ON subscription_dwh.`fact_customer_subscribed`.`dim_time_id` = subscription_dwh.`dim_time`.`id` "
        "WHERE subscription_dwh.`dim_time`.`time_date` = " + yesterday_str)
cursor.execute(query)
# insert new data
query = (
        "INSERT INTO subscription_dwh.`fact_customer_subscribed`(`dim_city_id`, `dim_time_id`, `total_active`, `total_inactive`, `daily_new`, `daily_canceled`, `ts`) "
        " SELECT city_dwh.id AS dim_ctiy_id, time_dwh.id AS dim_time_id, SUM(CASE WHEN customer_live.active = 1 THEN 1 ELSE 0 END) AS total_active, SUM(CASE WHEN customer_live.active = 0 THEN 1 ELSE 0 END) AS total_inactive, SUM(CASE WHEN customer_live.active = 1 AND DATE(customer_live.time_updated) = @time_date THEN 1 ELSE 0 END) AS daily_new, SUM(CASE WHEN customer_live.active = 0 AND DATE(customer_live.time_updated) = @time_date THEN 1 ELSE 0 END) AS daily_canceled, MIN(NOW()) AS ts "
        "FROM subscription_live.`customer` customer_live "
        "INNER JOIN subscription_live.`city` city_live ON customer_live.city_id = city_live.id "
        "INNER JOIN subscription_live.`country` country_live ON city_live.country_id = country_live.id "
        "INNER JOIN subscription_dwh.dim_city city_dwh ON city_live.city_name = city_dwh.city_name AND city_live.postal_code = city_dwh.postal_code AND country_live.country_name = city_dwh.country_name "
        "INNER JOIN subscription_dwh.dim_time time_dwh ON time_dwh.time_date = " + yesterday_str + " "
                                                                                                   "GROUP BY city_dwh.id, time_dwh.id")
cursor.execute(query)

# 2.2 update subscription statuses
# delete old data for the same date
query = (
        "DELETE subscription_dwh.`fact_subscription_status`.* "
        "FROM subscription_dwh.`fact_subscription_status` "
        "INNER JOIN subscription_dwh.`dim_time` ON subscription_dwh.`fact_subscription_status`.`dim_time_id` = subscription_dwh.`dim_time`.`id` "
        "WHERE subscription_dwh.`dim_time`.`time_date` = " + yesterday_str)
cursor.execute(query)
# insert new data
query = (
        "INSERT INTO subscription_dwh.`fact_subscription_status`(`dim_city_id`, `dim_time_id`, `total_active`, `total_inactive`, `daily_new`, `daily_canceled`, `ts`) "
        "SELECT city_dwh.id AS dim_ctiy_id, time_dwh.id AS dim_time_id, SUM(CASE WHEN subscription_live.active = 1 THEN 1 ELSE 0 END) AS total_active, SUM(CASE WHEN subscription_live.active = 0 THEN 1 ELSE 0 END) AS total_inactive, SUM(CASE WHEN subscription_live.active = 1 AND DATE(subscription_live.time_updated) = @time_date THEN 1 ELSE 0 END) AS daily_new, SUM(CASE WHEN subscription_live.active = 0 AND DATE(subscription_live.time_updated) = @time_date THEN 1 ELSE 0 END) AS daily_canceled, MIN(NOW()) AS ts "
        "FROM subscription_live.`customer` customer_live "
        "INNER JOIN subscription_live.`subscription` subscription_live ON subscription_live.customer_id = customer_live.id "
        "INNER JOIN subscription_live.`city` city_live ON customer_live.city_id = city_live.id "
        "INNER JOIN subscription_live.`country` country_live ON city_live.country_id = country_live.id "
        "INNER JOIN subscription_dwh.dim_city city_dwh ON city_live.city_name = city_dwh.city_name AND city_live.postal_code = city_dwh.postal_code AND country_live.country_name = city_dwh.country_name "
        "INNER JOIN subscription_dwh.dim_time time_dwh ON time_dwh.time_date = " + yesterday_str + " "
                                                                                                   "GROUP BY city_dwh.id, time_dwh.id")
cursor.execute(query)

print('Fact tables updated.')

# commit & close connection
cursor.close()
connection.commit()
connection.close()
print('Disconnected from database.')