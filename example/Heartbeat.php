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
use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Consumer\Assignment;

go(function () {
    $config = new ConsumerConfig();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.9.0.1');

    $config->setGroupId('test');

    EasySwoole\Kafka\Broker::getInstance()->setGroupBrokerId('127.0.0.1:9092');
    $ret = EasySwoole\Kafka\Group\Process::getInstance()->joinGroup();
    $assign = Assignment::getInstance();
    $assign->setMemberId($ret['memberId']);
    $assign->setGenerationId($ret['generationId']);
//    $assign->setGenerationId(2);
//    $assign->setMemberId('Easyswoole-kafka-d2a3bca8-6709-457c-8d6b-95fe7f95a107');

    $heartbeat = new Heartbeat();

    $result = $heartbeat->beat();
    var_dump($result);
});
