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
use EasySwoole\Kafka\Config\GroupConfig;
use EasySwoole\Kafka\Consumer\Assignment;

go(function () {
    $config = new GroupConfig();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.8.2');

    $config->setGroupId('test');

    $assign = Assignment::getInstance();
    $assign->setGenerationId(1);
    $assign->setMemberId('Easyswoole-kafka-d2a3bca8-6709-457c-8d6b-95fe7f95a107');

    $offset = new Group();

    $result = $offset->syncGroup();
    var_dump($result);
});
