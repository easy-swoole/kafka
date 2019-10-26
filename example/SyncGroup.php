<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/24
 * Time: 下午3:48
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

    $syncMeta = new EasySwoole\Kafka\SyncMeta\Process($config);
    $broker = $syncMeta->syncMeta();
    $broker->setGroupBrokerId('127.0.0.1:9092');
    $assignment = new Assignment();

    $group = new Group($config, $assignment, $broker);
    $ret = $group->joinGroup();
    $assignment->setGenerationId($ret['generationId']);
    $assignment->setMemberId($ret['memberId']);
    $assignment->assign($ret['members'], $broker);

    $result = $group->syncGroup();
    var_dump($result);
});
