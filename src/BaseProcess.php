<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 上午8:49
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Config\Config;
use EasySwoole\Kafka\Exception\Exception;
use EasySwoole\Log\Logger;

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

    protected function getBroker(): Broker
    {
        return Broker::getInstance();
    }

    protected function getConfig()
    {
        return new Config();
    }
}
