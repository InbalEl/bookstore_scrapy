# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
import mysql.connector

class BookscraperPipeline:
    def process_item(self, item, spider):

        adapter = ItemAdapter(item)

        # Strip all whitespaces from str
        field_names = adapter.field_names()
        for field_name in field_names:
            if field_name != 'description':
                value = adapter.get(field_name)
                adapter[field_name] = value[0].strip()
        
        # Category & product type --> switch to lowercase
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()

        # Price --> convert to float
        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£', '')
            
            num = 0.0
            if (len(value) > 0):
                num = float(value)
            adapter[price_key] = float(num)
        
        # Availability --> take num and switch to int
        availability_str = adapter.get('availability')
        split_avail_str = availability_str.split('(')

        num = 0
        if (len(split_avail_str)) > 1:
            num_str = split_avail_str[1].split(' ')
            num = int(num_str[0])

        adapter['availability'] = int(num)

        # Reviews --> convert str to int
        num_reviews_str = adapter.get('num_reviews')
        adapter['num_reviews'] = int(num_reviews_str)

        # Star rating --> to int
        star_rating_str = adapter.get('stars')
        stars_str = star_rating_str.split(' ')[1].lower()

        stars = 0

        if stars_str == 'one':
            stars = 1
        elif stars_str == 'two':
            stars = 2
        elif stars_str == 'three':
            stars = 3
        elif stars_str == 'four':
            stars = 4
        elif stars_str == 'five':
            stars = 5
        
        adapter['stars'] = stars



        return item


class SaveToMySQLPipeline:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host = 'localhost',
            user = 'scrapyuser',
            password = 'scrapy1',
            database = 'books'
        )

        ## Create cursor, used to execute commands
        self.cur = self.conn.cursor()

        ## Create books table if it doesn't exist
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS books(
            id int NOT NULL auto_increment, 
            url VARCHAR(255),
            title text,
            upc VARCHAR(255),
            product_type VARCHAR(255),
            price_excl_tax DECIMAL,
            price_incl_tax DECIMAL,
            tax DECIMAL,
            price DECIMAL,
            availability INTEGER,
            num_reviews INTEGER,
            stars INTEGER,
            category VARCHAR(255),
            description text,
            PRIMARY KEY (id)
        )
        """)

    def process_item(self, item, spider):

        ## Define insert statement
        self.cur.execute(""" insert into books (
            url, 
            title, 
            upc, 
            product_type, 
            price_excl_tax,
            price_incl_tax,
            tax,
            price,
            availability,
            num_reviews,
            stars,
            category,
            description
            ) values (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
                )""", (
            item["url"],
            item["title"],
            item["upc"],
            item["product_type"],
            item["price_excl_tax"],
            item["price_incl_tax"],
            item["tax"],
            item["price"],
            item["availability"],
            item["num_reviews"],
            item["stars"],
            item["category"],
            str(item["description"][0])
        ))

        # ## Execute insert of data into database
        self.conn.commit()
        return item
            
    def close_spider(self, spider):

        ## Close cursor & database conn
        self.cur.close()
        self.conn.close()