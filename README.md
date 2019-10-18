# kafka
本项目代码参考自 https://github.com/weiboad/kafka-php

# 安装
```php

```

### 生产者
```php
use EasySwoole\Kafka\Config\ProducerConfig;
use EasySwoole\Kafka\Producer;

$config = ProducerConfig::getInstance();
    $config->setMetadataBrokerList('127.0.0.1:9092,127.0.0.1:9093');
    $config->setBrokerVersion('0.9.0');
    $config->setRequiredAck(1);

    $producer = new Producer();

    for ($i = 0; $i < 50; $i++) {
        $producer->send([
            [
                'topic' => 'test',
                'value' => 'message' . $i,
                'key'   => 'key' . $i,
            ]
        ]);
    }
    
    // 两张方式发送结果相同，都是单条生产
    $producer->send([
        [
            'topic' => 'test',
            'value' => 'message1',
            'key'   => 'key1',
        ],
        [
            'topic' => 'test',
            'value' => 'message2',
            'key'   => 'key2',
        ]
    ]);
    
```


### 消费者
```php
use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Consumer;

go(function () {
    $config = ConsumerConfig::getInstance();
    $config->setRefreshIntervalMs(1000);// 消费间隔
    $config->setMetadataBrokerList('127.0.0.1:9092');//节点host
    $config->setBrokerVersion('0.9.0');//kafka版本
    $config->setGroupId('test');// groupId

    $config->setTopics(['test']);// 订阅的topic
    $config->setOffsetReset('earliest');

    $consumer = new Consumer(function ($topic, $partition, $message) {
        var_dump($topic);
        var_dump($partition);
        var_dump($message);
    });

    $consumer->subscribe();
});
```