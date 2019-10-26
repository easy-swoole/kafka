<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/24
 * Time: 上午11:16
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\Offset;
use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Consumer\Assignment;

go(function () {
    $config = new ConsumerConfig();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.8.2');

    $config->setGroupId('test');
    $config->setTopics(['test']);

    $broker = new \EasySwoole\Kafka\Broker();
    $broker->setGroupBrokerId('127.0.0.1:9092');

    $assign = new Assignment();
    $assign->setGenerationId(0);
    $assign->setMemberId('');

    $offset = new Offset($config, $assign, $broker);

    $result = $offset->commit([
        'test' => [
            1 => 0,
            2 => 0
        ]
    ]);
    var_dump($result);
});
