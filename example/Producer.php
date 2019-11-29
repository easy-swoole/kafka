<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/26
 * Time: 下午7:44
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\Config\ProducerConfig;
use EasySwoole\Kafka\Kafka;

go(function () {

    $config = new ProducerConfig();
    $config->setMetadataBrokerList('127.0.0.1:9092,127.0.0.1:9093');
    $config->setBrokerVersion('0.10.0');
    $config->setRequiredAck(1);
    $kafka = new Kafka($config);
    $result = $kafka->producer()->send([
        [
        'topic' => 'test',
        'value' => 'message--',
        'key'   => 'key--',
        ],
    ]);
    var_dump($result);
});
