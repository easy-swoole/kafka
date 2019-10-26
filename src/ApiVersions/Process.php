<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/20
 * Time: 上午10:17
 */
namespace EasySwoole\Kafka\ApiVersions;

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
    public function apiVersions()
    {
        $connect = $this->getBroker()->getMetaConnect($this->getBroker()->getGroupBrokerId());

        if ($connect === null) {
            throw new ConnectionException();
        }

        $params     = [];

        $requestData = Protocol::encode(Protocol::API_VERSIONS_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::API_VERSIONS_REQUEST, substr($data, 8));

        return $ret;
    }
}
