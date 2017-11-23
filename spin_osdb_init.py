"""
Full-stack Data Engineer
Part 1.0 - spin initial state for OceanRecords db. In this script, operational sytem (postgres) populated with fake 
(but not completly random) data will be created.
"""
# built-in
import csv
import datetime
import os
import random

# 3rd party
import luigi
import numpy as np

# custom
from oceanrecordshandler import OceanRecordsHandler

class CreateTablesDDL(luigi.Task):
    def requires(self):
        return []

    def output(self):
        return luigi.LocalTarget('tmp_init_files/tables_ddl_created.txt')

    def run(self):
        with self.output().open('w') as f:
            with open('os_create_statements.sql', 'r') as inp:
                sql_script = inp.read()
            handler.cursor.execute(sql_script)


class CreateCustomersRowsInit(luigi.Task):
    def requires(self):
        return CreateTablesDDL()

    def output(self):
        return luigi.LocalTarget('tmp_init_files/customer_rows_init.csv')

    def run(self):
        with self.output().open('w') as f:
            writer = csv.writer(f)
            for _ in range(handler.customers_rec_no):
                writer.writerow(handler.new_customer_row())


class CopyInitCustomerRows(luigi.Task):
    def requires(self):
        return CreateCustomersRowsInit()

    def output(self):
        return luigi.LocalTarget('tmp_init_files/customer_loaded.txt')

    def run(self):
        with self.output().open('w') as f:
            copy_cmd = """ COPY os.customers (first_name, last_name, mobile_no, email, date_of_birth, registration_ts)
                           FROM '{}' DELIMITER ',' CSV
                       """.format(os.path.join(os.getcwd(), self.input().path))
            handler.cursor.execute(copy_cmd)


class CreateAddressesRowsInit(luigi.Task):
    def requires(self):
        return CopyInitCustomerRows()

    def output(self):
        return luigi.LocalTarget('tmp_init_files/addresses_rows_init.csv')

    def run(self):
        with self.output().open('w') as f:
            writer = csv.writer(f)
            for _ in range(handler.addresses_rec_no):
                writer.writerow(handler.new_address_row())


class CopyInitAddressesRows(luigi.Task):
    def requires(self):
        return CreateAddressesRowsInit()

    def output(self):
        return luigi.LocalTarget('tmp_init_files/addresses_loaded.txt')

    def run(self):
        with self.output().open('w') as f:
            copy_cmd = """ COPY os.addresses (addresse_line1, addresse_line2, city, zip, country)
                           FROM '{}' DELIMITER ',' CSV
                       """.format(os.path.join(os.getcwd(), self.input().path))
            handler.cursor.execute(copy_cmd)


class CreateInitCustomersAddressesRows(luigi.Task):
    def requires(self):
        return CopyInitAddressesRows()

    def output(self):
        return luigi.LocalTarget('tmp_init_files/customers_addresses_rows_init.csv')

    def run(self):
        customers_ids = handler.get_res_single_col('SELECT DISTINCT customer_id FROM os.customers;')
        adds_ids = handler.get_res_single_col('SELECT DISTINCT address_id FROM os.addresses;')
        # compose rows
        used = set()
        with self.output().open('w') as f:
            writer = csv.writer(f)
            idx = 0
            while idx < handler.customers_addresses_rec_no:
                candidate = (random.choice(customers_ids), random.choice(adds_ids))
                if candidate not in used:
                    writer.writerow(candidate)
                    used.add(candidate)
                    idx += 1


class CopyInitCustomersAddressesRows(luigi.Task):
    def requires(self):
        return CreateInitCustomersAddressesRows()

    def output(self):
        return luigi.LocalTarget('tmp_init_files/customers_addresses_loaded.txt')

    def run(self):
        with self.output().open('w') as f:
            copy_cmd = "COPY os.customers_addresses (customer_id, address_id) FROM '{}' DELIMITER ',' CSV".format(os.path.join(os.getcwd(), 
                                                                                                                  self.input().path))
            handler.cursor.execute(copy_cmd)


class LoadOrderStatusPaymentMethodsTbls(luigi.Task):
    def requires(self):
        return CopyInitCustomersAddressesRows()

    def output(self):
        return luigi.LocalTarget('tmp_init_files/order_status_payment_methods_loaded.txt')

    def run(self):
        with self.output().open('w') as f:
            cmd = """
                  INSERT INTO os.payment_methods (payment_met_name) 
                  VALUES ('Debit Card'), ('Credit Card'), ('Bank Transfer'), ('PayPal'), ('Android Pay'), ('BitCoin'), ('Apple Pay'), ('USAC');

                  INSERT INTO os.order_status (order_status_name) VALUES ('Purchase Order'), ('Paid'), ('Shipped'), ('Refunded'), ('Closed');
                  """
            handler.cursor.execute(cmd)


class CreateInitCustomerOrdersRows(luigi.Task):
    def requires(self):
        return LoadOrderStatusPaymentMethodsTbls()

    def output(self):
        return luigi.LocalTarget('tmp_init_files/customer_orders_rows_init.csv')

    def run(self):
        """
        to make it a little bit more interesting, will split customer_address into 3 groups where:
        ~10% of custs will do ~20% of customer_orders
        ~40% of custs will do ~50% of customer_orders
        ~50% of custs will do ~30% of customer_orders
        """
        customer_address_ids = handler.get_res_single_col('SELECT DISTINCT customer_address_id FROM os.customers_addresses;')
        custs_ids_len = len(customer_address_ids)
        random.shuffle(customer_address_ids)
        g1,g2,g3 = round(custs_ids_len*0.1), round(custs_ids_len*0.4), round(custs_ids_len*0.5)
        probabilities = [(0.2)/g1]*g1
        probabilities.extend([(0.5)/g2]*g2)
        probabilities.extend([(0.3)/g3]*g3)
        pay_mets_ids = handler.get_res_single_col('SELECT DISTINCT payment_met_id FROM os.payment_methods;')
        ord_stat_ids = handler.get_res_single_col('SELECT DISTINCT order_status_id FROM os.order_status;')
        with self.output().open('w') as f:
            writer = csv.writer(f)
            for user_address in np.random.choice(customer_address_ids, size=(handler.customer_orders_rec_no,), p=probabilities):
                order_status = random.choice(ord_stat_ids)
                min_order_date =  datetime.date(datetime.date.today().year - 7, 1, 1)
                order_placed_date = handler.faker.date_between(start_date=min_order_date, end_date='today')
                order_paid_date =  order_placed_date + datetime.timedelta(days=random.randint(1,7)) if order_status != 1 else 'NULL'
                # initially total order price is 0.0. it will be updated later
                writer.writerow([user_address, random.choice(pay_mets_ids), order_status, order_placed_date, order_paid_date, '0.0'])


class CopyInitCustomerOrdersRows(luigi.Task):
    def requires(self):
        return CreateInitCustomerOrdersRows()

    def output(self):
        return luigi.LocalTarget('tmp_init_files/customer_orders_loaded.txt')

    def run(self):
        with self.output().open('w') as f:
            copy_cmd = """COPY os.customer_orders (customer_address_id, payment_met_id, order_status_id, order_placed_date, 
                                                   order_paid_date, total_order_price) 
                          FROM '{}' DELIMITER ',' CSV NULL AS 'NULL'""".format(os.path.join(os.getcwd(), self.input().path))
            handler.cursor.execute(copy_cmd)


class LoadProductTypesSubtypesTbls(luigi.Task):
    def requires(self):
        return CopyInitCustomerOrdersRows()

    def output(self):
        return luigi.LocalTarget('tmp_init_files/product_types_subtypes_loaded.txt')

    def run(self):
        with self.output().open('w') as f:
            cmd = """
                  INSERT INTO os.product_types (product_type_name) VALUES ('Furniture'), ('Office Supplies'), ('Technology'); 
                  
                  INSERT INTO os.product_subtypes (product_subtype_name) 
                  VALUES ('Appliances'), ('Binders and Binder Accessories'), ('Bookcases'), ('Chairs & Chairmats'), ('Computer Peripherals'),
                        ('Copiers and Fax'), ('Envelopes'), ('Labels'), ('Office Furnishings'), ('Office Machines'), ('Paper'),
                        ('Pens & Art Supplies'), ('Rubber Bands'), ('Scissors, Rulers and Trimmers'), ('Storage & Organization'),
                        ('Tables'), ('Telephones and Communication');
                  """
            handler.cursor.execute(cmd)


class LoadSuppliersTbl(luigi.Task):
    def requires(self):
        return LoadProductTypesSubtypesTbls()

    def output(self):
        return luigi.LocalTarget('tmp_init_files/suppliers_loaded.txt')

    def run(self):
        with self.output().open('w') as f:
            for _ in range(5):
                tpl = (handler.faker.company(), handler.faker.phone_number(), handler.faker.company_email())
                insert_cmd = "INSERT INTO os.suppliers (supplier_name, supplier_phone, supplier_mail) VALUES " + str(tpl)
                handler.cursor.execute(insert_cmd)


class CreateInitProductsRows(luigi.Task):
    def requires(self):
        return LoadSuppliersTbl()

    def output(self):
        return luigi.LocalTarget('tmp_init_files/products_rows_init.csv')

    def run(self):
        handler.cursor.execute('SELECT DISTINCT product_type_name, product_type_id FROM os.product_types;')
        types_mapping = {k:v for k,v in handler.cursor.fetchall()}
        handler.cursor.execute('SELECT DISTINCT product_subtype_name, product_subtype_id FROM os.product_subtypes;')
        subtypes_mapping = {k:v for k,v in handler.cursor.fetchall()}
        suppliers_id = handler.get_res_single_col('SELECT DISTINCT supplier_id FROM os.suppliers;')
        # assumes that external (it's not created in this script) prod_raw file exists
        with open('prod_raw', 'r') as inp:
            reader = csv.reader(inp, delimiter='\t')
            with self.output().open('w') as f:
                writer = csv.writer(f)
                for prod in reader:
                    writer.writerow([prod[0], types_mapping[prod[1]], subtypes_mapping[prod[2]], random.choice(suppliers_id), prod[3]])


class CopyInitProductsRows(luigi.Task):
    def requires(self):
        return CreateInitProductsRows()

    def output(self):
        return luigi.LocalTarget('tmp_init_files/products_loaded.txt') 

    def run(self):
        with self.output().open('w') as f:
            copy_cmd = """COPY os.products (product_name, product_type_id, product_subtype_id, supplier_id, product_price)
                          FROM '{}' DELIMITER ',' CSV""".format(os.path.join(os.getcwd(), self.input().path))
            handler.cursor.execute(copy_cmd)


class CreteInitCustomerOrdersProducts(luigi.Task):
    def requires(self):
        return CopyInitProductsRows()

    def output(self):
        return luigi.LocalTarget('tmp_init_files/customer_orders_products_rows_init.csv')

    def run(self):
        order_ids = handler.get_res_single_col('SELECT DISTINCT order_id FROM os.customer_orders;')
        product_ids = handler.get_res_single_col('SELECT DISTINCT product_id FROM os.products;')
        with self.output().open('w') as f:
            writer = csv.writer(f)
            for order in order_ids:
                prod_idx = 0
                prods_in_order = set()
                order_prod_limit = random.randint(1, handler.max_prods_in_order_limit)
                while prod_idx < order_prod_limit:
                    # create row for each product in order
                    prod_id = random.choice(product_ids)
                    while prod_id in prods_in_order:
                        # choose another one if given prod is already in given order
                        prod_id = random.choice(product_ids)
                    prods_in_order.add(prod_id)
                    writer.writerow([order, prod_id, random.randint(1, handler.max_prod_quantity_limit)])
                    prod_idx += 1


class CopyInitCustomerOrdersProductsRows(luigi.Task):
    def requires(self):
        return CreteInitCustomerOrdersProducts()

    def output(self):
        return luigi.LocalTarget('tmp_init_files/customer_orders_products_loaded.txt')

    def run(self):
        with self.output().open('w') as f:
            copy_cmd = """COPY os.customer_orders_products (order_id, product_id, quantity)
                          FROM '{}' DELIMITER ',' CSV""".format(os.path.join(os.getcwd(), self.input().path))
            handler.cursor.execute(copy_cmd)


class UpdateCustomerOrdersTotalPrice(luigi.Task):
    def requires(self):
        return CopyInitCustomerOrdersProductsRows()

    def output(self):
        return luigi.LocalTarget('tmp_init_files/customer_orders_updated.txt')

    def run(self):
        with self.output().open('w') as f:
            update = """ UPDATE os.customer_orders co
                            SET total_order_price = q.total_order_price
                                ,updated_ts = now()
                           FROM (SELECT cop.order_id, SUM(cop.quantity*p.product_price) AS total_order_price
                                   FROM os.customer_orders_products cop
                          INNER JOIN os.products p ON cop.product_id = p.product_id
                          GROUP BY cop.order_id) q
                          WHERE co.order_id = q.order_id;"""
            handler.cursor.execute(update)


class OceanRecordsInit(luigi.WrapperTask):
    # though globals are in general evil, it's better to define it here rather than passing it as luigi parameter allover the placeses
    global handler
    handler = OceanRecordsHandler()
    def requires(self):
        yield UpdateCustomerOrdersTotalPrice()


if __name__ == '__main__':
    """
    usage: python fde1_spin_db_init.py OceanRecordsInit --local-scheduler
    """
    luigi.run()
