import service

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # service.get_weibo_time_num_bar_html()
    # service.get_weibo_attitude_num_pie_html()
    print(list(service.get_weibo_news_by_month('2020-06')))

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
