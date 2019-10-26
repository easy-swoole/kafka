<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/24
 * Time: 下午4:23
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\Group;
use EasySwoole\Kafka\Config\ConsumerConfig;

go(function () {
    $config = new ConsumerConfig();
    $config->setMetadataBrokerList('127.0.0.1:9093');
    $config->setBrokerVersion('0.10.2');
    $config->setGroupId('test');


    $broker = new \EasySwoole\Kafka\Broker();
    $broker->setGroupBrokerId('127.0.0.1:9092');

    $group = new Group($config, new \EasySwoole\Kafka\Consumer\Assignment(), $broker);
    $result = $group->describeGroups();
    var_dump($result);
});
