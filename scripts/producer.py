import csv
import time
from kafka import KafkaProducer

# 实例化一个KafkaProducer示例，用于向Kafka投递消息
producer = KafkaProducer(bootstrap_servers='localhost:9092')
# 打开数据文件
csvfile = open("../data/user_log.csv", "r")
# 生成一个可用于读取csv文件的reader
reader = csv.reader(csvfile)
def gender():
    for line in reader:
        age_range = line[8]  # 性别在每行日志代码的第9个元素
        if age_range == 'age_range':
            continue  # 去除第一行表头
        time.sleep(0.1)  # 每隔0.1秒发送一行数据
        # 发送数据，topic为'sex'
        producer.send('age', line[9].encode('utf8'))


if __name__ == '__main__':
    gender()
