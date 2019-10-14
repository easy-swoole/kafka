<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/21
 * Time: 下午11:10
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\Group;
use EasySwoole\Kafka\Config\Config;

go(function () {
    $config = new Config();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.8.2');

    $group = new Group();

    \EasySwoole\Kafka\Broker::getInstance()->setGroupBrokerId('127.0.0.1:9092');

    $result = $group->listGroup();
    var_dump($result);
});
