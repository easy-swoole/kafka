<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/17
 * Time: 下午11:08
 */
namespace EasySwoole\Kafka\Test;

use EasySwoole\Kafka\Config;
use EasySwoole\Kafka\ProducerConfig;
use EasySwoole\Log\Logger;
use PHPUnit\Framework\TestCase;
use EasySwoole\Kafka\Producer;

class ProducerAsyncTest extends TestCase
{
    /**
     * @throws \EasySwoole\Kafka\Exception
     * @throws \EasySwoole\Kafka\Exception\Config
     */
    function testProducer()
    {
        $config = ProducerConfig::getInstance();
        $config->setMetadataRefreshIntervalMs(10000);
        $config->setMetadataBrokerList('127.0.0.1:9093');
        $config->setBrokerVersion('1.0.0');
        $config->setRequiredAck(1);
        $config->setIsAsyn(false);
        $config->setProduceInterval(500);
        $config->setSecurityProtocol(Config::SECURITY_PROTOCOL_SASL_SSL);
        $config->setSaslMechanism(Config::SASL_MECHANISMS_SCRAM_SHA_256);
        $config->setSaslUsername('manlin');
        $config->setSaslPassword('123456');
        $config->setSaslKeytab('');
        $config->setSaslPrincipal('');

        $producer = new Producer(function (){
            return [
                [
                    'topic' => 'test',
                    'value' => 'test....message',
                    'key'   => '',
                ]
            ];
        });

        $producer->send(true);
    }
}