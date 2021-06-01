from flask import Flask
from flask import request
from flask import Response
from flask_cors import CORS
import service
import utils
from utils import common_rep

app = Flask('web_api')
CORS(app, supports_credentials=True)


@app.route('/news/search', methods=['GET'])
def news_search():
    keyword = request.args['keyword']
    if not keyword:
        return common_rep(True, [])
    news = service.search_news(keyword)
    return common_rep(True, utils.format_news(news))


@app.route('/news/show', methods=['GET'])
def get_news():
    id = request.args['news_id']
    news = service.get_news(id)
    if news:
        return common_rep(True, utils.format_news([news])[0])
    else:
        return common_rep(True, msg='没有找到相关新闻')


@app.route('/graph/comments', methods=['GET'])
def get_comment_graph():
    id = request.args['news_id']
    if not id:
        return common_rep(False, msg='缺少参数')
    return service.get_news_comments_graph_html(id)


@app.route('/graph/wordcloud', methods=['GET'])
def get_comment_word_cloud():
    id = request.args['news_id']
    if not id:
        return common_rep(False, msg='缺少参数')
    img = service.get_comments_word_cloud(id)

    return Response(img, mimetype='image/jpeg')


@app.route('/graph/keyword', methods=['GET'])
def get_keyword_graph():
    keyword = request.args['keyword']
    if not keyword:
        return common_rep(False, msg='缺少参数')
    return service.get_keyword_news_graph_html(keyword)


@app.route('/graph/attitude/covid', methods=['GET'])
def get_attitude_graph():
    month = request.args.get('month')
    return service.get_weibo_attitude_num_pie_html(month)


@app.route('/graph/num/covid', methods=['GET'])
def get_num_graph():
    return service.get_weibo_time_num_bar_html()


if __name__ == '__main__':
    app.run()
