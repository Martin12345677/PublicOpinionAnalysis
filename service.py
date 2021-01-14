from pyhanlp import HanLP
import pymysql
import conf
import dgraph

db = pymysql.connect(conf.MYSQL_HOST, conf.MYSQL_USER, conf.MYSQL_PASSWORD, 'news')

cursor = db.cursor()

keywords = {}
keywords_len = 0


def insert_keywords_into_mysql(words, news_id):
    global keywords_len
    for word in words:
        keyword_id = ''
        if word in keywords:
            keyword_id = keywords[word]
        else:
            keyword_id = keywords_len
            keywords[word] = str(keyword_id)
            keywords_len += 1
            insert_keyword_sql = "insert into keyword(id,content) values ('%s','%s');" % (keyword_id, word)
            cursor.execute(insert_keyword_sql)
        insert_keyword_to_news_sql = "insert into keywordsOfNews(keywordId,newsId) value ('%s','%s');" % (keyword_id, news_id)
        cursor.execute(insert_keyword_to_news_sql)

    db.commit()


def get_keywords():
    try:
        sql = 'SELECT * FROM NEWSWB'
        cursor.execute(sql)
        news = cursor.fetchall()
        for n in news:
            insert_keywords_into_mysql(HanLP.extractKeyword(n[5], 5), n[0])
            print(n[0], ' finish')
    except IOError as err:
        print(err)


def show_words():
    sql = 'SELECT * FROM NEWSWB'
    cursor.execute(sql)
    news = cursor.fetchone()
    print(news[5], '>>>>>>>', HanLP.extractKeyword(news[5], 5))


def news_mysql_to_dgraph():
    sql = 'SELECT * FROM NEWSWB'
    cursor.execute(sql)
    news = cursor.fetchall()
    index = 0
    for n in news:
        dgraph.create_data({
            'id': n[0],
            'commentNum': n[1],
            'type': n[2],
            'thumbNum': n[3],
            'title': n[4],
            'content': n[5],
            'time': n[6],
            'author': n[7],
            'url': n[8]
        })
        index += 1
        print(index, len(news))


def search_news(keyword):
    word = '%' + keyword + '%'
    sql = "select * from newsWB where title like '%s' or content like '%s'" % (word, word)
    cursor.execute(sql)
    news = cursor.fetchall()
    return news


def get_news(id):
    sql = "select * from newsWB where id = '%s'" % id
    cursor.execute(sql)
    news = cursor.fetchone()
    return news
