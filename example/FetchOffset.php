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
use EasySwoole\Kafka\Config\OffsetConfig;

go(function () {
    $config = new OffsetConfig();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.8.2');

    $config->setGroupId('connect-fast-data');
    $config->setTopics(['test']);

    $offset = new Offset();

    $result = $offset->fetchOffset();
    var_dump($result);
});
