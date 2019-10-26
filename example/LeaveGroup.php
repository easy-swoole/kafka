<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/22
 * Time: 下午2:15
 */
require '../vendor/autoload.php';
date_default_timezone_set('PRC');

use EasySwoole\Kafka\Group;
use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Consumer\Assignment;

go(function () {
    $config = new ConsumerConfig();
    $config->setMetadataBrokerList('127.0.0.1:9092');
    $config->setBrokerVersion('0.8.2');
    $config->setGroupId('test');

    $broker = new \EasySwoole\Kafka\Broker();
    $broker->setGroupBrokerId('127.0.0.1:9092');

    $assignment = new Assignment();
    $group = new Group($config, $assignment, $broker);
    $ret = $group->joinGroup();
    $assignment->setMemberId($ret['memberId']);// 测试leaveGroup需要先生成MemberId，如果知道该参数，可直接赋值测试

    $result = $group->leaveGroup();
    var_dump($result);
});
