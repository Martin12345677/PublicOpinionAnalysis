from pyhanlp import HanLP
import pymysql
import conf
import dgraph
from pyecharts import options as opts
from pyecharts.charts import Graph, Bar, Pie
import time
import os
from snownlp import SnowNLP
import csv
import jieba
from wordcloud import WordCloud
import threading

lock = threading.RLock()

db = pymysql.connect(conf.MYSQL_HOST, conf.MYSQL_USER, conf.MYSQL_PASSWORD, 'news')

cursor = db.cursor()

db_wb = pymysql.connect(conf.MYSQL_HOST, conf.MYSQL_USER, conf.MYSQL_PASSWORD, 'weibo')

cursor_wb = db_wb.cursor()

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
        insert_keyword_to_news_sql = "insert into keywordsOfNews(keywordId,newsId) value ('%s','%s');" % (
        keyword_id, news_id)
        cursor.execute(insert_keyword_to_news_sql)

    db.commit()


def insert_weibo_into_mysql():
    file = open('/Users/matianyu/毕设/project/WeiboSuperSpider/无 GUI 功能独立版/topic/新冠肺炎.csv')
    lines = csv.reader(file)
    i = 0
    for line in lines:
        if '微' in line[0]:
            continue
        try:
            sql = "insert into weibo(id, nickname, gender, content, time, thumb, share, comment) values ('%s','%s','%s','%s','%s','%d','%d','%d')" % (line[0], line[1], line[2], line[6], line[8], int(line[10]), int(line[11]), int(line[12]))
            cursor_wb.execute(sql)
            i += 1
            print(i)
        except:
            continue
    db_wb.commit()


def get_keywords():
    try:
        sql = 'SELECT * FROM NEWSWB'
        lock.acquire()
        cursor.execute(sql)
        lock.release()
        news = cursor.fetchall()
        for n in news:
            insert_keywords_into_mysql(HanLP.extractKeyword(n[5], 5), n[0])
            print(n[0], ' finish')
    except IOError as err:
        print(err)


def show_words():
    sql = 'SELECT * FROM NEWSWB'
    lock.acquire()
    cursor.execute(sql)
    lock.release()
    news = cursor.fetchone()
    print(news[5], '>>>>>>>', HanLP.extractKeyword(news[5], 5))


def news_mysql_to_dgraph():
    sql = 'SELECT * FROM NEWSWB'
    lock.acquire()
    cursor.execute(sql)
    lock.release()
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
    lock.acquire()
    cursor.execute(sql)
    lock.release()
    news = cursor.fetchall()
    return news


def get_news(id):
    sql = "select * from newsWB where id = '%s'" % id
    lock.acquire()
    cursor.execute(sql)
    lock.release()
    news = cursor.fetchone()
    return news


def get_comment(id):
    sql = "select * from comment where id = '%s'" % id
    lock.acquire()
    cursor.execute(sql)
    lock.release()
    comment = cursor.fetchone()
    return comment


def get_comments(id):
    sql_get_comment_ids = "select * from commentsOfNews where newsId = '%s'" % id
    lock.acquire()
    cursor.execute(sql_get_comment_ids)
    lock.release()
    comment_ids = cursor.fetchall()
    comments = []
    for comment_2_news in comment_ids:
        comment = get_comment(comment_2_news[0])
        if comment:
            comments.append(comment)
    return comments


def get_comments_word_cloud(id):
    news = get_news(id)
    if not news:
        return

    comments = get_comments(id)
    text = ''
    for comment in comments:
        text += comment[1]

    text += news[5]

    words = jieba.cut(text)

    l = []
    for w in words:
        if len(w) > 1:
            l.append(w)

    content = ' '.join(l)

    wc = WordCloud(width=2000, height=1200, font_path='/System/Library/Fonts/Hiragino Sans GB.ttc', background_color='#f4f6f9', max_words=30)

    wc.generate(content)

    name = './html/' + id + '_' + str(int(time.time())) + '.jpg'
    wc.to_file(name)

    img = open(name, mode='rb').read()

    os.remove(name)

    return img


def make_graph_html(id, nodes, links):
    name = './html/' + id + '_' + str(int(time.time())) + '.html'
    Graph().add(
        "",
        nodes,
        links,
        is_draggable=True,
        is_rotate_label=True,
        repulsion=500,
        itemstyle_opts=opts.ItemStyleOpts(color='#687384'),
        # edge_label=opts.LabelOpts(
        #     is_show=True, position="middle", formatter="{c}",
        # ),
    ).render(name)
    html = open(name).read()
    os.remove(name)
    return html


def get_news_comments_graph_html(id):
    news = get_news(id)
    name = news[4]
    nodes = [
        opts.GraphNode(name, value=news[5], symbol_size=50),
    ]
    comments = get_comments(id)
    links = []
    for comment in comments:
        content = comment[1]
        comment_id = comment[0]
        nodes.append(opts.GraphNode(name=comment_id, value=content, symbol_size=20))
        links.append(opts.GraphLink(source=name, value=content, target=comment_id))
    return make_graph_html(id, nodes, links)


def get_keyword_news_graph_html(keyword):
    news = search_news(keyword)
    nodes = []
    links = []
    exist_ids = []
    for n in news:
        name = n[4]
        id = n[0]
        if name not in exist_ids:
            nodes.append(opts.GraphNode(name, value=n[5], symbol_size=50))
            exist_ids.append(name)
        comments = get_comments(id)
        for comment in comments:
            content = comment[1]
            comment_id = comment[0]
            if comment_id not in exist_ids:
                nodes.append(opts.GraphNode(name=comment_id, value=content, symbol_size=20))
                exist_ids.append(comment_id)
            links.append(opts.GraphLink(source=name, value=content, target=comment_id))
    return make_graph_html(keyword, nodes, links)


def get_all_weibo_news():
    sql = "select * from weibo"
    cursor_wb.execute(sql)
    news = cursor_wb.fetchall()
    return news


def get_is_time_fun(time):
    def is_time(n):
        return time in n[4]
    return is_time


def get_weibo_news_by_month(month):
    news = get_all_weibo_news()

    is_time = get_is_time_fun(month)
    return list(filter(is_time, news))


def make_news_attitude():
    news = get_all_weibo_news()
    i = 0
    l = len(news)
    for n in news:
        print(n)
        if n[8]:
            print('jump')
            continue
        attitude = SnowNLP(n[3]).sentiments
        update_sql = "update weibo set attitude = %f where id = '%s';" % (attitude, n[0])
        cursor_wb.execute(update_sql)
        i = i + 1
        print(i, '/', l)
    db_wb.commit()


def get_weibo_time(n):
    return n[4]


def get_num_by_time():
    news = list(get_all_weibo_news())
    news.sort(key=get_weibo_time)
    times = []
    nums = []
    for n in news:
        t = n[4][0: 7]
        try:
            i = times.index(t)
            nums[i] = nums[i] + 1
        except ValueError:
            times.append(t)
            nums.append(1)
    return {
        '时间': times,
        '数量': nums
    }


def get_num_by_attitude(month=None):
    if month:
        news = get_weibo_news_by_month(month)
    else:
        news = get_all_weibo_news()
    active = 0
    negative = 0
    objective = 0
    for n in news:
        attitude = n[8]
        if attitude > 0.7:
            active += 1
        elif attitude < 0.3:
            negative += 1
        else:
            objective += 1
    return {
        '积极': active,
        '消极': negative,
        '客观': objective
    }


def make_bar_html(data):
    file_name = './html/bar_' + str(int(time.time())) + '.html'
    # file_name = './html/bar-covid-num.html'
    num = 0
    charts = Bar()
    for name in data:
        if num == 0:
            charts = charts.add_xaxis(data[name])
        else:
            charts = charts.add_yaxis(name, data[name], color='#687384')
        num += 1
    charts.set_global_opts(yaxis_opts=opts.AxisOpts(
        axisline_opts=opts.AxisLineOpts(linestyle_opts=opts.LineStyleOpts(color='#687384'))
    ), xaxis_opts=opts.AxisOpts(
        axisline_opts=opts.AxisLineOpts(linestyle_opts=opts.LineStyleOpts(color='#687384'))
    )).set_series_opts(label_opts=opts.LabelOpts(color='#687384'))
    charts.render(file_name)
    html = open(file_name).read()
    os.remove(file_name)
    return html


def get_weibo_time_num_bar_html():
    html = open('./html/bar-covid-num.html').read()
    return html


def make_pie_html(data):
    file_name = './html/pie_' + str(int(time.time())) + '.html'
    Pie() \
        .add('', [[name, data[name]] for name in data]) \
        .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}")) \
        .render(file_name)
    html = open(file_name).read()
    os.remove(file_name)
    return html


def get_weibo_attitude_num_pie_html(month=None):
    if month:
        data = get_num_by_attitude(month)
        return make_pie_html(data)
    else:
        html = open('./html/pie-covid-attitude.html').read()
        return html
