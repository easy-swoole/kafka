<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/18
 * Time: 上午10:28
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Consumer;

go(function () {
    $config = ConsumerConfig::getInstance();
    $config->setRefreshIntervalMs(1000);
    $config->setMetadataBrokerList('127.0.0.1:9092,127.0.0.1:9093');
    $config->setBrokerVersion('0.9.0');
    $config->setGroupId('test');

    $config->setTopics(['test']);
    $config->setOffsetReset('earliest');

    $consumer = new Consumer(function ($topic, $partition, $message) {
        var_dump($topic);
        var_dump($partition);
        var_dump($message);
    });

    $consumer->subscribe();
});
