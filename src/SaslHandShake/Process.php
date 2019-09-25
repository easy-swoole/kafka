<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/24
 * Time: 下午5:47
 */
namespace EasySwoole\Kafka\SaslHandShake;

use EasySwoole\Kafka\BaseProcess;
use EasySwoole\Kafka\Protocol;

class Process extends BaseProcess
{
    /**
     * Process constructor.
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function __construct()
    {
        parent::__construct();

        $this->config = $this->getConfig();
        Protocol::init($this->config->getBrokerVersion());
        $this->getBroker()->setConfig($this->config);
    }

    /**
     * @return array
     * @throws \EasySwoole\Kafka\Exception\ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function handShake()
    {
        $result     = [];
        foreach ($this->brokerHost as $host) {
            $connect = $this->getBroker()->getMetaConnect($host);

            if ($connect === null) {
                continue;
            }

            $params = [
                $this->config->getSaslMechanism()
            ];

            $requestData = Protocol::encode(Protocol::SASL_HAND_SHAKE_REQUEST, $params);
            $data = $connect->send($requestData);
            $ret = Protocol::decode(Protocol::SASL_HAND_SHAKE_REQUEST, substr($data, 8));

            $result[] = $ret;
        }

        return $result;
    }
}
