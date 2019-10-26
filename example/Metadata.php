<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/24
 * Time: 下午5:28
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\Config\Config;
use EasySwoole\Kafka\SyncMeta\Process;

go(function () {

    $config = new Config();
    $config->setMetadataBrokerList('127.0.0.1:9092,127.0.0.1:9093');
    $config->setBrokerVersion('0.9.0');

    $metaData = new Process($config);
    var_dump($metaData->syncMeta()->getTopics());
});
