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
    $config->setMetadataRefreshIntervalMs(10000);
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setGroupId('test');
    $config->setBrokerVersion('1.0.0');
    $config->setTopics(['test']);
    $config->setOffsetReset('earliest');

    $consumer = new Consumer();

    $consumer->start(function ($topic, $part, $message): void {
        var_dump($message);
    });
});
