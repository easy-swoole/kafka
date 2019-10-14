<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/21
 * Time: 下午1:51
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Fetch;

go(function () {

    $config = ConsumerConfig::getInstance();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.9.0');

    EasySwoole\Kafka\SyncMeta\Process::getInstance()->syncMeta();
    EasySwoole\Kafka\Broker::getInstance()->setGroupBrokerId('127.0.0.1:9092');

    $fetch = new Fetch();
    $result = $fetch->fetch();
    var_dump($result);
});
