<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/20
 * Time: 上午10:45
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\ApiVersions;
use EasySwoole\Kafka\Config\Config;

go(function () {
    $config = new Config();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.10.0.0');

    $broker = new \EasySwoole\Kafka\Broker();
    $broker->setGroupBrokerId('127.0.0.1:9092');

    $apiVersions = new ApiVersions($config, $broker);

    $result = $apiVersions->getVersions();
    var_dump($result);
});
