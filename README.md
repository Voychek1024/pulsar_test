# pulsar_test

2023 X Project

Go 语言实现的 Pulsar 多命名空间 压测工具

支持：

-   客户端多线程、配置文件、Prometheus接入、日志时间戳打标
-   生产者全局限速、Send/SendAsync方法实现
-   消费者Chan方法接收、多命名空间的消息TPS、P50、P99统计
-   基于Python、MySQL的逐条消息TPS、P50、P99统计



## 使用手册

### 首次部署

初始化环境

```bash
chmod u+x ./sbin/*.sh
./sbin/init_env.sh
```

Python环境，推荐使用`Python 3.6+`

需求：详见requirements.txt

```bash
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

修改Python程序目录，脚本赋权

```bash
sed -i "s/#!\/path\/to\/bin\/python/<实际bin/python目录>/g" ./utils/log_analyzer.py ./utils/log_visualizer.py
chmod u+x ./utils/log_analyzer.py ./utils/log_visualizer.py
```

搭建MySQL环境：

```sql
CREATE TABLE `table_pulsar_specs` (
    `ID` bigint unsigned NOT NULL AUTO_INCREMENT,
    `PULSAR_PUB_TIMESTAMP` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    `PULSAR_SUB_TIMESTAMP` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    `PAYLOAD_SPEC` int NOT NULL DEFAULT '0',
    `COMMENTS` varchar(512) CHARACTER SET gb18030 COLLATE gb18030_bin NOT NULL DEFAULT '',
    `PUB_HOST` varchar(256) COLLATE gb18030_bin NOT NULL DEFAULT '',
    `SUB_HOST` varchar(256) COLLATE gb18030_bin NOT NULL DEFAULT '',
    `RECORD_CREATE_TIMESTAMP` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    `RECORD_UPDATE_TIMESTAMP` datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (`ID`),
    KEY `INDEX_PULSAR_SPECS_1` (`PULSAR_PUB_TIMESTAMP`),
    KEY `INDEX_PULSAR_SPECS_2` (`PULSAR_SUB_TIMESTAMP`)
) ENGINE=InnoDB DEFAULT CHARSET=gb18030 COLLATE=gb18030_bin COMMENT='Pulsar PUB-SUB statistics'
```

若想要使用OS_ENV方式调用日志分析工具，需配置环境变量：

```bash
export WSL_HOST="<host>"
export PULSAR_DB_VALUES="<port> <user> <password> <database>"
```



### 日志分析工具

将读取consumer.dev.log.$(date '+%Y%m%d')_\$HOSTNAME文件并逐条落库表 `table_pulsar_specs`

程序调用

```bash
./utils/log_analyzer.py OS_ENV # 使用系统环境变量填写MySQL连接要素
./utils/log_analyzer.py <host> <port> <user> <password> <database>
# 手工于命令行输入MySQL连接要素
```

后续可使用SQL语句进行时序、TPS等分析

其中，统计表字段意义如下：

| 字段名                     | 字段意义               |
|-------------------------|--------------------|
| ID                      | 主键，自增列，可体现消费者的重复消费 |
| PULSAR_PUB_TIMESTAMP    | 生产者端发送时间戳打标        |
| PULSAR_SUB_TIMESTAMP    | 消费者端接收时间戳打标        |
| PAYLOAD_SPEC            | 报文特征（报文长度）         |
| PUB_HOST                | 发送方\$HOSTNAME      |
| SUB_HOST                | 接收方\$HOSTNAME      |
| COMMENTS                | 组合字段               |
| RECORD_CREATE_TIMESTAMP | 记录创建时间             |
| RECORD_UPDATE_TIMESTAMP | 记录更新时间             |

使用log_visualizer.py查询每秒钟TPS、P99时延，生成csv文件以备制图，程序调用方式同log_analyzer.py

*注意：log_analyzer.py所处理的日志文件不能过大，建议使用`split -l 5000000 $file_src $file_dest`方法将大文件分为5000000行/文件再行处理。*

**日志清理**

```bash
chmod u+x ./sbin/cleanup.sh
./sbin/cleanup.sh
```



### Go压测工具

编译

```bash
sh build.sh
```

配置sample.yml文件，样例如下，建议创建在conf文件夹中：

```yaml
PULSAR_SETTINGS:
  tenant: "public" # 租户名
  namespace: "default" # 命名空间, 多个tag以英文逗号无空格分隔, 如: default00,default01,...
  topic: "my-topic" # 主题名
  broker_address: "pulsar://localhost:6650" # broker服务地址
#  production_name: "my-pub" # 生产者名称，去重时使用
#  token: "token_standalone" # JWT-token验证文件，位于./tokens/中

CONSUMER_SETTINGS:
  subscription_name: "my-sub" # 订阅名称
  thread: 1 # 消费者协程数
  log_detail: true # 日志记录每条消息, 用于log_analyzer.py
  log_counter: true # 日志记录消息计数器(每秒触发)

PRODUCER_SETTINGS:
  func_name: "SendAsync" # 选择发送方法: Send 或 SendAsync
  thread: 1 # 生产者协程数
  log_detail: true # 日志记录每条消息, 用于和消费者比对数据, 使用Prometheus时可置为false
  payload: "HELLO PULSAR" # 消息内容
```

调用方式（Pub端支持全局限速）：

```bash
bin/consumer conf/<configFilename>
bin/producer conf/<configFilename> publishRatio(int, msg/s)
# 如 bin/consumer conf/sample.yml
```

验证方式，收发报文总量一致，可通过Prometheus观察：

```bash
grep "Perform stat" logs/consumer* | awk -F "- " '{print $2}' | awk -F ")" '{print $1}' | paste -sd+ | bc
```

Prometheus端口：2112

配置prometheus.yml监听地址：http://localhost:2112/metrics

