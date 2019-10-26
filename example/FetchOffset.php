<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/22
 * Time: 下午2:42
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\Offset;
use EasySwoole\Kafka\Config\ConsumerConfig;

go(function () {
    $config = new ConsumerConfig();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.8.2');
    $config->setGroupId('test');
    $config->setTopics(['test']);

    $syncMeta = new EasySwoole\Kafka\SyncMeta\Process($config);
    $broker = $syncMeta->syncMeta();
    $broker->setGroupBrokerId('127.0.0.1:9092');

    $offset = new Offset($config, new \EasySwoole\Kafka\Consumer\Assignment(), $broker);
    $result = $offset->fetchOffset();
    var_dump($result);
});
