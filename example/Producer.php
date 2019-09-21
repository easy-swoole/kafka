<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/26
 * Time: 下午7:44
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\Config\ProducerConfig;
use EasySwoole\Kafka\Producer;

go(function () {

    $config = ProducerConfig::getInstance();
    $config->setMetadataRefreshIntervalMs(10000);
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.9.0');
    $config->setRequiredAck(1);
    $config->setIsAsyn(false);
    $config->setProduceInterval(500);

    $producer = new Producer();

    $result = $producer->send([
        [
            'topic' => 'test',
            'value' => 'test2....message.',
            'key'   => '',
        ]
    ]);
    var_dump($result);
});
