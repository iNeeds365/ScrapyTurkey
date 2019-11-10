import warnings
with warnings.catch_warnings():
    warnings.simplefilter("ignore")

    import sys
    import requests
    import bs4
    import json
    import pymssql
    import yaml
    import re

    # Classes
    class Product:
        def __init__(self):
            self.restaurant = ''
            self.info = ''
            self.desc = ''
            self.price = 0
        
    class Restaurant:
        def __init__(self):
            self.name=''
            self.ship_areas=''
            self.work_hours=''
            self.service_time=''
            self.promotions=''
            self.warnings=''
            self.payment_methods=''
            self.trade_info=''
            self.url = ''
        
    class Comment:
        def __init__(self):
            self.user_name=''
            self.speed=''
            self.service=''
            self.flavor=''
            self.comment=''
        
    def fix(obj):
        regex = r"[\n\t ]+"
        for prop in vars(obj).keys():
            if not prop.startswith('_'):
                val = str(getattr(obj, prop))
                val = val.replace("'", "''")
                val = re.sub(regex, " ", val).strip()
                setattr(obj, prop, val)

    try:
        # read url from the arguments
        URL='https://www.yemeksepeti.com/dominos-pizza-toros-mah-hayal-park-adana'
        if len(sys.argv) > 1:
            URL = sys.argv[1]
        print("# Scraping started: %s" % URL)

        # read server info from the setting file
        host = ''
        user = ''
        password = ''
        database = ''
        with open("config.yaml", 'r') as yamlfile:
            cfg = yaml.load(yamlfile)
        print("# Database setting loaded: %s, %s, %s, %s" % (host, user, password, database))

        # create db connection
        conn = pymssql.connect(server = cfg['mssql']['host'], user = cfg['mssql']['user'], password = cfg['mssql']['password'], database = cfg['mssql']['db'])
        cursor = conn.cursor()
        print("# Database connected")
        
        # get html
        # with open('html.txt', encoding='utf-8',  mode = 'r') as file:
        #     html = file.read()

        print("# Requesting %s" % URL)
        resp = requests.get(URL)
        resp.raise_for_status()
        html = resp.text
        bs = bs4.BeautifulSoup(html, 'html.parser')

        # restaurant info
        rest = Restaurant()
        rest.name = bs.select("h1.ys-h2")[0].text
        rest.ship_areas = bs.select("#regions")[0].text
        rest.work_hours = bs.select("#popup2")[0].text
        rest.service_time = bs.select("div[class*=deliveryTime]")[0].text
        rest.promotions = ";".join([x.text for x in bs.select("#promotions p")])
        rest.warnings = bs.select("#warnings")[0].text.strip()
        rest.payment_methods = bs.select("#payment-types")[0].text.strip()
        rest.trade_info = bs.select("#tradeinfo")[0].text.strip()
        rest.url = URL
        fix(rest)
        values = "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')" % (rest.name, rest.ship_areas, rest.work_hours, rest.service_time, rest.promotions, rest.warnings, rest.payment_methods, rest.trade_info, rest.url)
        query = "insert into tbl_restaurant (name, ship_areas, work_hours, service_time, promotions, warnings, payment_methods, trade_info, url) values " + values
        cursor.execute(query)
        print("  Restaurant %s" % rest.name)

        # menu
        info = bs.select("div[class*=product-info]")
        desc = bs.select("div[class*=product-desc]")
        price = bs.select("div[class*=product-price]")

        if len(info) != len(desc) or len(info) != len(price):
            print('Error')
        else:
            products = []
            values = ''
            for i in range(len(info)):
                prod = Product()
                prod.restaurant = rest.name
                prod.info = info[i].text.strip().split('\n')[0]
                prod.desc = desc[i].text
                prod.price = price[i].text
                fix(prod)
                products.append(prod)
                values += "('%s', '%s', '%s', '%s')," % (prod.restaurant, prod.info, prod.desc, prod.price)

            query = "insert into tbl_product (restaurant, product_info, product_desc, product_price) values " + values[:-1]
            cursor.execute(query)
            print("  %d menu items are scraped" % len(products))

        # comment
        comments = []
        values = ''
        pages = len(bs.select("ul.ys-commentlist-page li"))
        for pg in range(pages):
            if pg > 0:
                pg_url = URL + '?section=comments&page=%d'%(pg + 1)
                print("# Requesting %s" % pg_url)
                resp = requests.get(pg_url)
                resp.raise_for_status()
                html = resp.text
                bs = bs4.BeautifulSoup(html, 'html.parser')

            comment_body = bs.select("div.comments-body")
            
            for body in comment_body:
                spd_tag = body.select("div[class*=speed]")
                if len(spd_tag ) > 0:
                    com = Comment()
                    com.speed = body.select("div[class*=speed]")[0].text.split(':')[-1]
                    com.service = body.select("div[class*=serving]")[0].text.split(':')[-1]
                    com.flavor = body.select("div[class*=flavour]")[0].text.split(':')[-1]
                    com.user_name = body.select("div.userName div")[0].text
                    com.comment = body.select("div.comment.row p")[0].text
                    fix(com)
                    comments.append(com)
                    values += "('%s', %s, %s, %s, '%s')," % (com.user_name, com.speed, com.service, com.flavor, com.comment)
        
        query = "insert into tbl_comments (user_name, speed, service, flavor, comment) values " + values[:-1]
        cursor.execute(query)
        print("  %d comments are scraped" % len(comments))

    except Exception as e:
        print(str(e))

    conn.commit()
    cursor.close()
    conn.close()

