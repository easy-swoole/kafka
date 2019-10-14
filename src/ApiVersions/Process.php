<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/20
 * Time: 上午10:17
 */
namespace EasySwoole\Kafka\ApiVersions;

use EasySwoole\Kafka\BaseProcess;
use EasySwoole\Kafka\Exception\ConnectionException;
use EasySwoole\Kafka\Protocol;
use EasySwoole\Log\Logger;

class Process extends BaseProcess
{
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

        $this->logger->log('Get ApiVersions start, params:' . json_encode($params), Logger::LOG_LEVEL_INFO);
        $data = $connect->send($requestData);
        $correlationId = Protocol\Protocol::unpack(Protocol\Protocol::BIT_B32, substr($data, 4, 4));
        $ret = Protocol::decode(Protocol::API_VERSIONS_REQUEST, substr($data, 8));

        return $ret;
    }
}
