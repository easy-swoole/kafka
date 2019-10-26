<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/24
 * Time: 下午5:47
 */
namespace EasySwoole\Kafka\SaslHandShake;

use EasySwoole\Kafka\BaseProcess;
use EasySwoole\Kafka\Broker;
use EasySwoole\Kafka\Config\Config;
use EasySwoole\Kafka\Exception\ConnectionException;
use EasySwoole\Kafka\Protocol;

class Process extends BaseProcess
{
    public function __construct(Config $config, Broker $broker)
    {
        parent::__construct($config);
        $this->setBroker($broker);
    }

    /**
     * @return array
     * @throws ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function handShake()
    {
        $connect = $this->getBroker()->getMetaConnect($this->getBroker()->getGroupBrokerId());

        if ($connect === null) {
            throw new ConnectionException();
        }

        $params = [
            $this->config->getSaslMechanism()
        ];

        $requestData = Protocol::encode(Protocol::SASL_HAND_SHAKE_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::SASL_HAND_SHAKE_REQUEST, substr($data, 8));

        return $ret;
    }
}
