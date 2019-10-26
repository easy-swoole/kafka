<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/21
 * Time: 下午11:18
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\Group;
use EasySwoole\Kafka\Config\ConsumerConfig;

go(function () {
    $config = new ConsumerConfig();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.8.2');
    $config->setGroupId('test');

    $broker = new \EasySwoole\Kafka\Broker();
    $broker->setGroupBrokerId('127.0.0.1:9092');

    $group = new Group($config, new \EasySwoole\Kafka\Consumer\Assignment(), $broker);
    $result = $group->joinGroup();
    var_dump($result);
});
