<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/21
 * Time: 下午10:35
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\Group;
use EasySwoole\Kafka\Config\GroupConfig;

go(function () {
    $config = new GroupConfig();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.8.2');

    $config->setGroupId('test');

    $group = new Group();

    $result = $group->findCoordinator();
    var_dump($result);
    /**
     * array(1) {
            [0]=>
            array(4) {
                ["errorCode"]=>
                int(0)
                ["coordinatorId"]=>
                int(0)
                ["coordinatorHost"]=>
                string(9) "127.0.0.1"
                ["coordinatorPort"]=>
                int(9092)
            }
        }
     */
});
