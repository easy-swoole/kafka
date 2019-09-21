<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/21
 * Time: 下午11:10
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\Group;
use EasySwoole\Kafka\Config\Config;

go(function () {
    $config = new Config();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.8.2');

    $group = new Group();

    $result = $group->listGroup();
    var_dump($result);
    /**
     * array(1) {
        [0]=>
        array(2) {
            ["errorCode"]=>
            int(0)
            ["groups"]=>
                array(2) {
                [0]=>
                    array(2) {
                    ["groupId"]=>
                    string(15) "schema-registry"
                    ["protocolType"]=>
                    string(2) "sr"
                }
                [1]=>
                    array(2) {
                    ["groupId"]=>
                    string(17) "connect-fast-data"
                    ["protocolType"]=>
                    string(7) "connect"
                }
            }
        }
    }
     */
});
