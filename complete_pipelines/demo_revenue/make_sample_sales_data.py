#!/usr/bin/env python3
'''

Create table as follows:

CREATE TABLE `sales` (
  id INT,
  sale_date DATETIME,
  product_id VARCHAR(255),
  price FLOAT,
  returned_date DATETIME,
  created_ts DATETIME,
  modified_ts DATETIME
);


LOAD DATA LOCAL INFILE 'sales.csv'
INTO TABLE sales
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';

'''

import os, random, datetime

def make_dataset():
    today = datetime.datetime.now().replace(hour=0,minute=0,second=0, microsecond=0)
    date = today - datetime.timedelta(days = 60)
    product_id = 1
    sale_id = 1
    day_offset = 0
    while date < today:
        make_row(sale_id, date, f"product_{product_id}", today, day_offset)
        if random.random() < .3:
            product_id += 1
        if random.random() < .01 or product_id >= 20:
            date += datetime.timedelta(days=1)
            product_id = 1
            day_offset += 1
        sale_id += 1

def make_row(sale_id, date, product, max_date, day_offset):
    created_ts = date + datetime.timedelta(seconds=random.randrange(86399))
    price = 10 + day_offset * .1 + random.random() * 20
    if random.randrange(10) == 0:
        modified_ts = created_ts + datetime.timedelta(seconds=int(86400 * 31 * random.random()))
        returned_date = modified_ts.replace(hour=0,minute=0,second=0, microsecond=0)
        if returned_date >= max_date:
            modified_ts = created_ts
            returned_date = ''
    else:
        modified_ts = created_ts
        returned_date = ''
    print(f"{sale_id},{date},{product},{price},{returned_date},{created_ts},{modified_ts}")


if __name__ == '__main__':
    make_dataset()

