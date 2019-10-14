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
use EasySwoole\Kafka\Config\GroupConfig;

go(function () {
    $config = new GroupConfig();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.8.2');

    $config->setGroupId('test');

    $offset = new Group();

    \EasySwoole\Kafka\Broker::getInstance()->setGroupBrokerId('127.0.0.1:9092');

    $result = $offset->describeGroups();
    var_dump($result);
});
