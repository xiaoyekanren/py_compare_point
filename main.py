# coding=utf-8
from iotdb.Session import Session
import threading
# from threading import Thread
import random
import queue

query_step_size_row = 100000  # 每次查询的行数，不包含count
is_random_or_all_ts_compare = 'random'
num_of_random_ts = 300

a_host, a_port, a_user, a_pass = '172.20.31.16,6667,root,root'.split(',')
b_host, b_port, b_user, b_pass = '172.20.31.24,6667,root,root'.split(',')

session1 = Session(a_host, a_port, a_user, a_pass)
session2 = Session(b_host, b_port, b_user, b_pass)

ts_queue = queue.Queue()  # 用于保存需要对比的时间序列
result_lock = threading.Lock()  # 用于控制对比结果的保存

thread_num = 5  # 线程数量
result_list = [[] for i in range(thread_num)]  # 保存每个线程的对比结果


def compare_two_result(list1, list2):
    list1 = [list1]
    list2 = [list2]

    if list1 == list2:
        print('result: 2个结果集一致')
        return True
    else:
        print('error: 结果集不一致，开始判断...')

    # 判断2个session的数据总数是否一致
    if len(list1) == len(list2):
        print(f'check_list: 序列数量一致，{len(list1)}条')
    else:
        print(f'check_list: session_one = {len(list1)}, session_two = {len(list2)}')

    # 判断点数是否一致
    a = []
    for i in list1:
        if i in list2:
            list2.remove(i)
        else:
            print(f'check_list: 序列 "{i}" not in session2')
            a.append(i)
    if a:
        print(f'check_list: 以下序列在session2中不存在: {a}')

    if list2:
        print(f'check_list: 以下序列在session1中不存在: {list2}')
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
    result_list_ = []

    for i in s1, s2:
        result = exec_sql(i, sql)
        if result:
            result_list_.append(result)
            # print(result)
    # print(result_list_)
    if message:
        print(message)
    if 'show timeseries' not in sql:
        if not compare_two_result(*result_list_):
            print('error: 程序退出')
            exit()
    else:
        print(f'info: 跳过 {sql}，容易因对比顺序不一致而导致失败')
    return result_list_


def return_query_count(s1, s2, base_sql):
    session1_ts_count, session2_ts_count = get_results_list(s1, s2, base_sql, f'sql: {base_sql}')
    # print(session1_ts_count, session2_ts_count, sep='\n')
    s1_count_ts, s2_count_ts = int(session1_ts_count[0][1][0]), int(session2_ts_count[0][1][0])  # count timeseries的结果
    return s1_count_ts, s2_count_ts


def return_query_select(s1, s2, base_sql, count_value):
    session1_list, session2_list = [], []
    if count_value <= query_step_size_row:
        session1_list, session2_list = get_results_list(s1, s2, base_sql, f'sql: {base_sql}')  # 获得两个iotdb序列的list
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


def compare_ts_or_point(s1, s2, count_base_sql, select_base_sql, result_list):
    print('info: 查询&对比 count point...')
    s1_count_ts, s2_count_ts = return_query_count(s1, s2, count_base_sql)  # 拿到count timeseries的值，int
    print(f'count_result: {s1_count_ts}')

    print(f'info: 查询&对比 query return list...')
    session1_list, session2_list = return_query_select(s1, s2, select_base_sql, s1_count_ts)
    with result_lock:
        result_list.append([session1_list, session2_list])


def return_random_ts_list(ts_list):
    new_ts_list = []
    count_ts_list = len(ts_list)

    ts_index_list = []
    for i in range(num_of_random_ts):
        while True:
            random_num = int(random.uniform(0,count_ts_list))
            if random_num not in ts_index_list:
                ts_index_list.append(random_num)
                break

    for index in ts_index_list:
        new_ts_list.append(ts_list[index])

    print(f'info: 已启用随机序列，将选择{num_of_random_ts}个，分别为：\n{new_ts_list}')
    return new_ts_list


def get_ts_from_session_ts_list(session1_list):
    ts_list = []
    for i in range(len(session1_list)):
        ts = session1_list[i][1][0]
        ts_list.append(ts)
    if is_random_or_all_ts_compare == 'random':
        ts_list = return_random_ts_list(ts_list)
    return ts_list


def compare_point_avg_ts(s1, s2, ts_list):
    for ts in ts_list:  # 使用哪个ts都一样，走到这里了，list完全一致
        ts_queue.put(ts)


def compare_thread(id, s1, s2, result_list):
    while True:
        try:
            ts = ts_queue.get(timeout=1)
            if ts is None:
                break

            series_path, series_name = '.'.join(ts.split('.')[:-1]), ts.split('.')[-1]
            count_base_sql = f'select count({series_name}) from {series_path}'
            select_base_sql = f'select {series_name} from {series_path}'
            compare_ts_or_point(s1, s2, count_base_sql, select_base_sql, result_list[id])
        except queue.Empty:
            break


def main():
    session_one_ts_list, session_two_ts_list = compare_ts_or_point(session1, session2, 'count timeseries root.test.**', 'show timeseries root.test.**')  # 拿到序列，列表

    ts_list = get_ts_from_session_ts_list(session_one_ts_list)  # 加工成可以使用的时间序列，列表

    compare_point_avg_ts(session1, session2, ts_list)  # 对比时间序列

    thread_pool = []
    for i in range(thread_num):
        t = threading.Thread(target=compare_thread, args=(i, session1, session2, result_list))
        thread_pool.append(t)

    for t in thread_pool:
        t.start()

    for t in thread_pool:
        t.join()

    print('比较结果：')
    for i in result_list:
        list1, list2 = i[0], i[1]
        compare_two_result(list1, list2)


if __name__ == '__main__':
    main()

# 要实现并发，可以使用多线程或多进程的方式。这里提供一种多线程的实现方式：
#
# 1.首先，在导入模块的时候，加入 from threading import Thread 语句，导入 Thread 类。
#
# 2.修改 compare_ts_or_point() 函数，新增一个参数 result_list，用来保存查询结果。
#
# 3.在 compare_point_avg_ts() 函数中，将比较时的每个时间序列放入一个队列中。
#
# 4.定义一个线程函数 compare_thread()，在其中取出队列中的时间序列，调用 compare_ts_or_point() 函数进行比较，将结果保存到 result_list 中。
#
# 5.定义一个线程池集合，其中的每一个线程都是执行 compare_thread() 函数。
#
# 6.启动线程池，等待所有线程执行完毕。
