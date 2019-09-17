<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/18
 * Time: 下午9:10
 */
namespace EasySwoole\Kafka\Test;

use EasySwoole\Kafka\Producer;
use EasySwoole\Kafka\ProducerConfig;
use PHPUnit\Framework\TestCase;

class ProducerSyncTest extends TestCase
{
    /**
     * @throws \EasySwoole\Kafka\Exception
     * @throws \EasySwoole\Kafka\Exception\Config
     */
    public function testProducer ()
    {
        $config = ProducerConfig::getInstance();
        $config->setMetadataRefreshIntervalMs(10000);
        $config->setMetadataBrokerList('127.0.0.1:9092');
        $config->setBrokerVersion('0.10.2.1');
        $config->setRequiredAck(1);
        $config->setIsAsyn(false);
        $config->setProduceInterval(500);
        $producer = new Producer();
        for ($i = 0; $i < 1; $i++) {
            $result = $producer->send([
                [
                    'topic' => 'test',
                    'value' => 'test....msg',
                    'key'   => '',
                ]
            ]);
        }
    }
}