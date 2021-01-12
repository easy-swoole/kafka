# kafka
本项目代码参考自 https://github.com/weiboad/kafka-php

# 安装
```php
composer require easyswoole/kafka
```

### 注册kafka服务
```php
namespace EasySwoole\EasySwoole;

use App\Producer\Process as ProducerProcess;
use App\Consumer\Process as ConsumerProcess;
use EasySwoole\EasySwoole\Swoole\EventRegister;
use EasySwoole\EasySwoole\AbstractInterface\Event;
use EasySwoole\Http\Request;
use EasySwoole\Http\Response;

class EasySwooleEvent implements Event
{

    public static function initialize()
    {
        // TODO: Implement initialize() method.
        date_default_timezone_set('Asia/Shanghai');
    }

    public static function mainServerCreate(EventRegister $register)
    {
        // TODO: Implement mainServerCreate() method.
        // 生产者
        ServerManager::getInstance()->getSwooleServer()->addProcess((new ProducerProcess())->getProcess());
        // 消费者
        ServerManager::getInstance()->getSwooleServer()->addProcess((new ConsumerProcess())->getProcess());
    }
    
    ......
    
}

```
### 生产者
```php
namespace App\Producer;

use EasySwoole\Component\Process\AbstractProcess;
use EasySwoole\Kafka\Config\ProducerConfig;
use EasySwoole\Kafka\Kafka;

class Process extends AbstractProcess
{
    protected function run($arg)
    {
        go(function () {
            $config = new ProducerConfig();
            $config->setMetadataBrokerList('127.0.0.1:9092,127.0.0.1:9093');
            $config->setBrokerVersion('0.9.0');
            $config->setRequiredAck(1);

            $kafka = new kafka($config);
            $result = $kafka->producer()->send([
                [
                    'topic' => 'test',
                    'value' => 'message--',
                    'key'   => 'key--',
                ],
            ]);

            var_dump($result);
            var_dump('ok');
        });
    }
}
```


### 消费者
```php
namespace App\Consumer;

use EasySwoole\Component\Process\AbstractProcess;
use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Kafka;

class Process extends AbstractProcess
{
    protected function run($arg)
    {
        go(function () {
            $config = new ConsumerConfig();
            $config->setRefreshIntervalMs(1000);
            $config->setMetadataBrokerList('127.0.0.1:9092,127.0.0.1:9093');
            $config->setBrokerVersion('0.9.0');
            $config->setGroupId('test');

            $config->setTopics(['test']);
            $config->setOffsetReset('earliest');

            $kafka = new Kafka($config);
            // 设置消费回调
            $func = function ($topic, $partition, $message) {
                var_dump($topic);
                var_dump($partition);
                var_dump($message);
            };
            $kafka->consumer()->subscribe($func);
        });
    }
}

```

### docker-compose.yml
```
启动
    docker-compose -f docker-compose.yml up -d
生成更多节点
    docker-compose scale kafka=3
创建topic
    docker exec kafka_kafka_1 kafka-topics.sh  --create --topic test --partitions 3 --zookeeper zookeeper:2181 --replication-factor 3
查看topic
    docker exec kafka_kafka_1 kafka-topics.sh --list --zookeeper zookeeper:2181
生产
    docker exec -it kafka_kafka_1 kafka-console-producer.sh --topic test --broker-list kafka_kafka_1:9092,kafka_kafka_2:9092,kafka_kafka_2:9092
消费
    docker exec -it kafka_kafka_1 kafka-console-consumer.sh --topic test --bootstrap-server kafka_kafka_1:9092,kafka_kafka_2:9092,kafka_kafka_2:9092
```
### Any Question
kafka使用问题及bug，欢迎到Easyswoole的kaka群中提问或反馈
QQ群号：827432930
