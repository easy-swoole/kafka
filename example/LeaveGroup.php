<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/22
 * Time: 下午2:15
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\Group;
use EasySwoole\Kafka\Config\GroupConfig;

go(function () {
    $config = new GroupConfig();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.8.2');

    $config->setGroupId('connect-fast-data');

    $group = new Group();

    $result = $group->leaveGroup();
    var_dump($result);
});
