# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter


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
