# built-in
import datetime
import random

# 3rd party
from faker import Faker
import psycopg2

class OceanRecordsHandler():
    def __init__(self, autocommit=True):
        self.conn = psycopg2.connect(dbname='oceanrecords', host='localhost', port=5432, user='os_admin', password='getme')
        self.conn.autocommit = autocommit
        self.cursor = self.conn.cursor()
        self.faker = Faker()
        # row numbers for initialization and limits 
        self.customers_rec_no           = 1000
        self.addresses_rec_no           = 1500
        self.customers_addresses_rec_no = 2000
        self.customer_orders_rec_no     = 10000
        self.max_prods_in_order_limit   = 5
        self.max_prod_quantity_limit    = 3
        self.min_cust_age = datetime.date(datetime.date.today().year - 18, 1, 1)
        self.min_reg_date = datetime.date(datetime.date.today().year - 7, 1, 1)

    def get_res_single_col(self, sql):
        self.cursor.execute(sql)
        return [row[0] for row in self.cursor.fetchall()]

    def get_random_rows(self, table=None, cols=[], limit=1):
        cmd = """ SELECT {cols} 
                      FROM {tbl}
                    OFFSET FLOOR(RANDOM()*(SELECT COUNT(*) FROM {tbl}))
                    LIMIT {limit};
                """.format(cols=','.join(cols), tbl=table, limit=limit)
        self.cursor.execute(cmd)
        return self.cursor.fetchall()

    def new_customer_row(self):
        return [self.faker.first_name(), self.faker.last_name(), self.faker.phone_number(), self.faker.email(), 
                self.faker.date_between(start_date='-80y', end_date=self.min_cust_age),
                self.faker.date_between(start_date=self.min_reg_date, end_date='today')]

    def new_address_row(self):
        return [self.faker.street_address().replace("'", ''),
                self.faker.secondary_address().replace("'", '') if random.randint(0,1) == 1 else 'NULL',
                self.faker.city().replace("'", ''),
                self.faker.zipcode().replace("'", ''),
                self.faker.country().replace("'", '')]
