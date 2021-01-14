from flask import Flask
from flask import request
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


if __name__ == '__main__':
    app.run()
