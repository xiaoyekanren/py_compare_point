# coding=utf-8
from iotdb.Session import Session

query_step_size_row = 200000  # 每次查询的行，统计、对比时间序列，统计、对比点数

a_host, a_port, a_user, a_pass = '127.0.0.1,6667,root,root'.split(',')
b_host, b_port, b_user, b_pass = '127.0.0.1,6667,root,root'.split(',')

session1 = Session(a_host, a_port, a_user, a_pass)
session2 = Session(b_host, b_port, b_user, b_pass)


def compare_two_result(list1, list2, message=None):
    if message:
        print(message)

    list1 = [list1]
    list2 = [list2]

    if list1 == list2:
        print('info: 2个结果集一致')
        return True
    else:
        print('error: 结果集不一致，开始判断')

    # 判断2个session的数据总数是否一致
    if len(list1) == len(list2):
        print(f'info: 序列数量一致，{len(list1)}条，')
    else:
        print(f'error: session_one = {len(list1)}, session_two = {len(list2)}')

    # 判断点数是否一致
    a = []
    for i in list1:
        if i in list2:
            list2.remove(i)
        else:
            print(f'序列 "{i}" not in session2')
            a.append(i)
    if a:
        print(f'以下序列在session2中不存在: {a}')

    if list2:
        print(f'以下序列在session1中不存在: {list2}')
    return False


def exec_sql(session, sql):
    session.open(False)

    list_ = []  # 最终结果

    query = session.execute_query_statement(sql)  # 查询

    while query.has_next():  # 保存结果
        line_ = query.next()
        ts = str(line_.get_timestamp())  # 拿到时间戳
        values_list = []  # 存放每一行的值的list
        for i in range(len(line_.get_fields())):  # 取得values的长度，即有多少列，做循环
            values_list.append(str(line_.get_fields()[i]))
        list_.append((ts, values_list))
    # for i in list_:  # 打印
    #     print(i)
    session.close()
    return list_


def get_results_list(s1, s2, sql, message=None):
    result_list = []

    for i in s1, s2:
        result = exec_sql(i, sql)
        if result:
            result_list.append(result)
            # print(result)
    # print(result_list)
    if message:
        print(message)
    if not compare_two_result(*result_list):
        print('info: 程序退出')
        exit()
    return result_list


def return_query_count(s1, s2, base_sql):
    session1_ts_count, session2_ts_count = get_results_list(s1, s2, base_sql, f'sql: {base_sql}')
    # print(session1_ts_count, session2_ts_count, sep='\n')
    s1_count_ts, s2_count_ts = int(session1_ts_count[0][1][0]), int(session2_ts_count[0][1][0])  # count timeseries的结果
    return s1_count_ts, s2_count_ts


def return_query_select(s1, s2, base_sql, count_value):
    session1_list, session2_list = [], []
    if count_value <= query_step_size_row:
        session1_list, session2_list = get_results_list(s1, s2, sql=base_sql, message='info: 逐点对比2个iotdb的序列...')  # 获得两个iotdb序列的list
    else:
        offset = 0
        while count_value > offset:  # 3333,1000
            if offset + query_step_size_row >= count_value:
                break
            sql = f'{base_sql} offset {offset} limit {query_step_size_row}'
            session1_one_query_list, session2_one_query_list = get_results_list(s1, s2, sql, f'sql: {sql}')
            session1_list = session1_list + session1_one_query_list
            session2_list = session2_list + session2_one_query_list
            offset = offset + query_step_size_row
        sql = f'{base_sql} offset {offset} limit {count_value - offset}'
        session1_one_query_list, session2_one_query_list = get_results_list(s1, s2, sql, f'sql: {sql}')
        session1_list = session1_list + session1_one_query_list
        session2_list = session2_list + session2_one_query_list
    return session1_list, session2_list


def compare_ts_or_point(s1, s2, count_base_sql, select_base_sql):
    s1_count_ts, s2_count_ts = return_query_count(s1, s2, count_base_sql)  # 拿到count timeseries的值，int
    print(f'info: count result: {s1_count_ts}')

    session1_list, session2_list = return_query_select(s1, s2, select_base_sql, s1_count_ts)
    return session1_list, session2_list


def get_ts_from_session_ts_list(session1_list):
    ts_list = []
    for i in range(len(session1_list)):
        ts = session1_list[i][1][0]
        ts_list.append(ts)
    return ts_list


def compare_point_avg_ts(s1, s2, ts_list):
    for ts in ts_list:  # 使用哪个ts都一样，走到这里了，list完全一致
        series_path, series_name = '.'.join(ts.split('.')[:-1]), ts.split('.')[-1]
        count_base_sql = f'select count({series_name}) from {series_path}'
        select_base_sql = f'select {series_name} from {series_path}'
        compare_ts_or_point(s1, s2, count_base_sql, select_base_sql)


def main():
    session_one_ts_list, session_two_ts_list = compare_ts_or_point(session1, session2, 'count timeseries', 'show timeseries')  # 拿到序列，列表

    ts_list = get_ts_from_session_ts_list(session_one_ts_list)  # 加工成可以使用的时间序列，列表

    compare_point_avg_ts(session1, session2, ts_list)  # 对比时间序列


if __name__ == '__main__':
    main()





