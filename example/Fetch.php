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

    $config = new ConsumerConfig();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.9.0');

    $syncMeta = new EasySwoole\Kafka\SyncMeta\Process($config);
    $broker = $syncMeta->syncMeta();
    $broker->setGroupBrokerId('127.0.0.1:9092');

    $fetch = new Fetch($config, new \EasySwoole\Kafka\Consumer\Assignment(), $broker);
    $result = $fetch->fetch();
    var_dump($result);
});
