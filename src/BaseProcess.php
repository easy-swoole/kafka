<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 上午8:49
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Config\Config;
use EasySwoole\Kafka\Exception\ConnectionException;
use EasySwoole\Kafka\Exception\Exception;
use EasySwoole\Log\Logger;
use EasySwoole\Kafka\Protocol;

class BaseProcess
{
    /**
     * @var Logger
     */
    protected $logger;

    /**
     * @var Config
     */
    protected $config;

    /**
     * @var array
     */
    protected $brokerHost = [];

    /**
     * BaseProcess constructor.
     * @throws Exception
     */
    public function __construct()
    {
        $this->logger       = new Logger();
        $this->config       = $this->getConfig();
        $this->brokerHost   = $this->getBrokerLists();
        $this->getBroker()->setConfig($this->config);

        Protocol::init($this->config->getBrokerVersion());
    }

    /**
     * @return array
     * @throws Exception
     */
    protected function getBrokerLists(): ?array
    {
        $brokerList = $this->config->getMetadataBrokerList();
        $brokerHost = [];
        foreach (explode(',', $brokerList) as $key => $val) {
            if (trim($val)) {
                $brokerHost[] = $val;
            }
        }
        if (count($brokerHost) === 0) {
            throw new Exception('No valid broker configured');
        }
        shuffle($brokerHost);

        return $brokerHost;
    }

    /**
     * @throws ConnectionException
     * @throws Exception
     */
    protected function syncMeta(): void
    {
        $brokerList = $this->config->getMetadataBrokerList();
        $brokerHost = [];
        foreach (explode(',', $brokerList) as $key => $val) {
            if (trim($val)) {
                $brokerHost[] = $val;
            }
        }
        if (count($brokerHost) === 0) {
            throw new Exception('No valid broker configured');
        }

        shuffle($brokerHost);
        $broker = $this->getBroker();

        foreach ($brokerHost as $host) {
            $client = $broker->getMetaConnect($host, true);
            if (! $client->isConnected()) {
                continue;
            }

            $params = [];
            $this->logger->log('Start sync metadata request params:' . json_encode($params));

            $requestData = Protocol::encode(Protocol::METADATA_REQUEST, $params);
            $data = $client->send($requestData);
            $dataLen = Protocol\Protocol::unpack(Protocol\Protocol::BIT_B32, substr($data, 0, 4));
            $correlationId = Protocol\Protocol::unpack(Protocol\Protocol::BIT_B32, substr($data, 4, 4));
            // 0-4字节是包头长度
            // 4-8字节是correlationId
            $result = Protocol::decode(Protocol::METADATA_REQUEST, substr($data, 8));
            if (! isset($result['brokers'], $result['topics'])) {
                throw new Exception("Get metadata is fail, brokers or topics is null.");
            }
            // 更新 topics和brokers
            $broker->setData($result['topics'], $result['brokers']);

            return;
        }

        throw new ConnectionException($brokerList);
    }

    protected function getBroker(): Broker
    {
        return Broker::getInstance();
    }

    protected function getConfig()
    {
        return new Config();
    }
}
