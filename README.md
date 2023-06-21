# iotdb 的点-点对比
> 2023-06-21

## 说明
就是对2个iotdb进行点到点对比，因为是单序列级的串行，序列多会超级慢，所以建议取随机数去对比。  
（后续考虑增加：多序列的串行、序列级别的并行、设备级的并行）

## 执行方式
```shell
# 前台
python3 main.py
# 后台
nohup python3 -u main.py > output.log 2>&1 &
```

## 依赖
```shell
python3.7+
  apache-iotdb
```

## 连接参数
```shell
# 下面2行就是2个session，改对应信息即可
a_host, a_port, a_user, a_pass = '172.20.31.16,6667,root,root'.split(',')
b_host, b_port, b_user, b_pass = '172.20.31.24,6667,root,root'.split(',')
```

## 可选参数
```shell
query_step_size_row = 100000  # 每次查询的行数，不包含count
is_random_or_all_ts_compare = 'random'  # 可选项: random or other, 对比全部时间序列还是部分时间序列
num_of_random_ts = 100  # 如果对比部分，随机抽取的数量
```

