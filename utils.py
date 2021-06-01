def format_news(news):
    f_news = []
    for n in news:
        f_news.append({
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
    return f_news


def common_rep(suc, data, msg=''):
    if suc:
        return {
            'code': 1,
            'data': data
        }
    else:
        return {
            'code': -1,
            'msg': msg
        }
