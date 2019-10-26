<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/24
 * Time: 下午5:43
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\SaslHandShake;
use EasySwoole\Kafka\Config\Config;
use EasySwoole\Kafka\Consumer\Assignment;

go(function () {
    $config = new Config();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.8.2');
    $config->setSaslMechanism('PLAIN');

    $broker = new \EasySwoole\Kafka\Broker();
    $broker->setGroupBrokerId('127.0.0.1:9092');

    $saslHandShake = new SaslHandShake($config, $broker);
    $result = $saslHandShake->handShake();
    var_dump($result);
});
