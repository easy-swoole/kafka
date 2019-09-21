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
//    $config->setMetadataRefreshIntervalMs(10000);
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.8.2');

    $apiVersions = new ApiVersions();

    $result = $apiVersions->getVersions();
    var_dump($result);
});
