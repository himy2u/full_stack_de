"""
Full-stack Data Engineer
Part 1.1 - inserts & updates for OceanRecord db 

For OceanRecords datbase to simulate real worlds operational system - new records needs to be inserted and old one updated.
That script will provide following actions:
    + place order           (insert into fact)
    + update order          (for updating fact)
    + new customer_address  (insert into dim)
    + update product price  (for SCD type 2 use case)
"""
# built-in
from datetime import datetime
import random

# custom
from oceanrecordshandler import OceanRecordsHandler


def order_insert(handler):
    """
    placing new order flow:
    - 1 entry in customer_orders table:
        + existing customer_address and existing payment method
        + order_place_date is now
        + order_status should be id for "Purchase Order" (which is 1)
        + order_paid_date is NULL
        + total_order_price has to be calculated based on products and quantities
    - X new entires in customers_orders_products table
        + X is number of different ordered products
        + valid quantities and prices from product table
        + it HAVE TO be proper id! (the one which was inserted to customer_orders previously)
    """
    ca_id = handler.get_random_rows(table='os.customers_addresses', cols=['customer_address_id'])[0][0]
    pm_id = handler.get_random_rows(table='os.payment_methods', cols=['payment_met_id'])[0][0]
    products = handler.get_random_rows(table='os.products', cols=['product_id', 'product_price'], 
                                       limit=random.randint(1,handler.max_prods_in_order_limit))
    sql_script = """
                 BEGIN;
                 INSERT INTO os.customer_orders (customer_address_id, payment_met_id, order_status_id, order_placed_date,
                                                 order_paid_date, total_order_price)
                 VALUES ({}, {}, 1, current_date, NULL, {}) RETURNING order_id;
                 """.format(ca_id, pm_id, round(sum(float(x[1]) for x in products),2))
    handler.cursor.execute(sql_script)
    order_id = handler.cursor.fetchall()[0][0]
    sql_script = ''
    for prod in products:
        sql_script += """
                      INSERT INTO os.customer_orders_products (order_id, product_id, quantity)
                      VALUES ({}, {}, {});\n
                      """.format(order_id, prod[0], random.randint(1,handler.max_prod_quantity_limit))
    sql_script += 'COMMIT;'
    handler.cursor.execute(sql_script)
    print('Placed order: ', order_id)


def order_update(handler):
    """
    updating order:
    - for not finished orders (order_status_id not in (4,5))
    - orders status can change like: 1->2, 1->5, 2->3, 2->4, 3->4
    - if change from status 1 (Purchase Order) to 2 (Paid), then order_paid_date have to be updated
    - in other cases only status_order_id and updated_ts are being updated
    - cannot modify content of order (in theory, order should be closed/refunded and new one placed)
    flow:
    - get random order with proper status
    - change status (random if more options to change) and other relevant columns
    """
    order_id, order_status = handler.get_random_rows(table='(SELECT * FROM os.customer_orders WHERE order_status_id NOT IN (4,5)) x', 
                                                     cols=['order_id', 'order_status_id'])[0]
    if order_status == 1:
        new_status = random.choice([2,5])
    elif order_status == 2:
        new_status = random.choice([3,4])
    elif order_status == 3:
        new_status = 4
    sql_script = """
                 UPDATE os.customer_orders
                    SET order_status_id={new_status},
                        updated_ts = now(),
                        order_paid_date = CASE WHEN {old_status} = 1 AND {new_status} = 2 THEN current_date
                                               ELSE order_paid_date
                                           END
                  WHERE order_id = {order_id};
                  COMMIT;
                 """.format(order_id=order_id, old_status=order_status, new_status=new_status)
    handler.cursor.execute(sql_script)
    print('Updated order: {}. Changed status from: {} to {}'.format(order_id, order_status, new_status))


def customer_address_insert(handler):
    """
    new customer flow:
    - new customer is being inserted into os.customers
    - address is chosen from os.addresses OR new one is inserted
    - new customers_addresses entry is inserted
    """
    sql_script = """
                 BEGIN; 
                 INSERT INTO os.customers (first_name, last_name, mobile_no, email, date_of_birth, registration_ts)
                 VALUES ({}) RETURNING customer_id;
                 """.format(','.join(["'{}'".format(col) for col in handler.new_customer_row()]))
    handler.cursor.execute(sql_script)
    customer_id = handler.cursor.fetchall()[0][0]
    is_new_address = random.choice([0,1])
    if is_new_address == 1:
        # create and insert new addres
        sql_script = """
                     INSERT INTO os.addresses (addresse_line1, addresse_line2, city, zip, country)
                     VALUES ({}) RETURNING address_id;
                     """.format(','.join(["'{}'".format(col) for col in handler.new_address_row()]))
        handler.cursor.execute(sql_script)
        address_id = handler.cursor.fetchall()[0][0]
    else:
        # choose existing one
        address_id = handler.get_random_rows(table='os.addresses', cols=['address_id'])[0][0]
    # insert customers_addresses row and commit
    sql_script = """
                 INSERT INTO os.customers_addresses (customer_id, address_id)
                 VALUES ({}, {}) RETURNING customer_address_id;
                 """.format(customer_id, address_id)
    handler.cursor.execute(sql_script)
    customer_address_id = handler.cursor.fetchall()[0][0]
    handler.cursor.execute('COMMIT;')
    msg = 'Inserted new customers_addresses row (id: {}) with customer:{} and address: {}'.format(customer_address_id, 
                                                                                                  customer_id, address_id)
    if is_new_address == 1:
        msg += '. Address was newly created'
    print(msg)


def update_product_price(handler):
    """
    updating product price flow:
    - only 1 table is being altered - os.products
    At point when change is introduced, any orders created will use new price. Old price in most cases won't be 
    able to restore from older orders.
    """
    prod_id, old_price = handler.get_random_rows(table='os.products', cols=['product_id', 'product_price'])[0]
    new_price = round(float(old_price) + (float(old_price)*(0.1*random.randint(1,9))*random.choice([1,-1])), 2)
    sql_script = """
                 UPDATE os.products
                    SET product_price = {}
                        , updated_ts = now()
                  WHERE product_id = {};
                 COMMIT;
                 """.format(new_price, prod_id)
    handler.cursor.execute(sql_script)
    print('Updated product price: {}. Changed from: {} to {}'.format(prod_id, old_price, new_price))


def main():
    # frequency is in seconds
    freq_lookup = {'order_insert': {'freq':1, 'previous_ts': datetime.now(), 'function':order_insert}, 
                   'order_update': {'freq':2, 'previous_ts': datetime.now(), 'function':order_update},
                   'customer_address': {'freq':3, 'previous_ts': datetime.now(), 'function':customer_address_insert},
                   'update_product_price': {'freq':15, 'previous_ts': datetime.now(), 'function':update_product_price}}
    handler = OceanRecordsHandler(autocommit=False)
    while True:
        for action in freq_lookup.values():
            if (datetime.now() - action['previous_ts']).seconds >= action['freq']:
                action['function'](handler)
                action['previous_ts'] = datetime.now()


if __name__ == '__main__':
    main()
