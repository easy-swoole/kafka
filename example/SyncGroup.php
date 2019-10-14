<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/24
 * Time: 下午3:48
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\Group;
use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Consumer\Assignment;

go(function () {
    $config = new ConsumerConfig();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.8.2');
    $config->setGroupId('test');

    EasySwoole\Kafka\Broker::getInstance()->setGroupBrokerId('127.0.0.1:9092');

    $group = new Group();
    $ret = $group->joinGroup();
    Assignment::getInstance()->setGenerationId($ret['generationId']);
    Assignment::getInstance()->setMemberId($ret['memberId']);
    Assignment::getInstance()->assign($ret['members']);
//    Assignment::getInstance()->setGenerationId(1);
//    Assignment::getInstance()->setMemberId('Easyswoole-kafka-d2a3bca8-6709-457c-8d6b-95fe7f95a107');
    $result = $group->syncGroup();
    var_dump($result);
});
