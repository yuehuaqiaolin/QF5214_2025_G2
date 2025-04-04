Automated Data Pipeline for Financial Risk Monitoring

（一）项目简介
本项目旨在构建一个自动化的数据管道，用于金融风险监控。该系统通过高频股指价格数据实时评估市场风险，并通过计算风险指标（如VaR和ES）进行风险预警。系统利用Kafka进行数据流处理，自动执行计算任务并生成可视化报告。

（二）系统架构
该系统主要包括以下组件：
数据采集：从中国、美国、新加坡等主要市场获取高频股指数据。
数据处理管道：通过Kafka进行实时数据流处理，计算市场风险指标。
自动化定时任务：通过Cron任务定时拉取数据并进行风险计算。
风险计算：使用历史模拟法计算VaR（Value at Risk）和ES（Expected Shortfall）指标。
可视化与报告：生成实时和历史的股指价格曲线图和风险报告。

（三）环境设置
（1）安装依赖
首先确保安装了Python 3.10及以上版本，然后通过以下命令安装依赖库：
pip install -r requirements.txt
（2）配置Kafka与Zookeeper
该项目使用Docker来部署Kafka和Zookeeper。在项目根目录下运行以下命令来启动相关服务：
docker-compose up -d
（3）设置交易日历文件
将以下CSV文件放入trade_calendar/目录中，以确保系统正确识别各市场的交易日期和时间：
trade_calendar_STI_2025.csv
trade_calendar_SP500_2025.csv
trade_calendar_SH300_2025.csv
（4）配置Crontab
该系统通过Crontab自动执行任务，确保实时数据采集与风险计算。根据不同市场的时间安排，配置以下任务：
crontab -e
然后在Crontab配置文件中添加定时任务。
（5）手动测试
在启用自动调度之前，可以手动执行以下Python脚本进行测试：
python kafka/producer.py
python kafka/consumer.py
python kafka/consumer_history.py
（6）日志监控
系统会将所有输出记录到指定的日志文件中，可以使用以下命令实时查看日志：
tail -f /path/to/logs/realtime_kafka.log

（四）使用示例
实时数据生产者输出
显示实时抓取的股指数据并推送到Kafka队列。
实时数据消费者输出
显示从Kafka队列接收的数据并进行处理。
历史数据消费者输出
显示历史数据处理过程并计算VaR。

（五）系统运行结果
实时VaR和ES计算：展示市场的即时风险。
风险预警：当VaR超过设定阈值时，自动触发邮件预警。
