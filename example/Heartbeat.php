<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/24
 * Time: 下午5:33
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\Heartbeat;
use EasySwoole\Kafka\Config\HeartBeatConfig;
use EasySwoole\Kafka\Consumer\Assignment;

go(function () {
    $config = new \EasySwoole\Kafka\Config\ConsumerConfig();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.9.0.1');

    $config->setGroupId('test');

    $assign = Assignment::getInstance();
    $assign->setGenerationId(2);
    $assign->setMemberId('Easyswoole-kafka-d2a3bca8-6709-457c-8d6b-95fe7f95a107');

    \EasySwoole\Kafka\Broker::getInstance()->setGroupBrokerId('127.0.0.1:9092');

    $heartbeat = new Heartbeat();

    $result = $heartbeat->beat();
    var_dump($result);
});
