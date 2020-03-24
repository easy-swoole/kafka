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
use EasySwoole\Kafka\Kafka;

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
    $kafka->consumer()->subscribe($func, 1, 256);


    // 停止消费
    $kafka->consumer()->stop();
});
