<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 上午8:49
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Config\Config;
use EasySwoole\Kafka\Consumer\Assignment;
use EasySwoole\Kafka\Exception\Exception;

class BaseProcess
{
    /**
     * @var Config
     */
    protected $config;

    /**
     * @var Assignment
     */
    protected $assignment;

    /**
     * @var Broker
     */
    private $broker;

    /**
     * @var array
     */
    protected $brokerHost = [];

    /**
     * BaseProcess constructor.
     * @param Config $config
     * @throws Exception
     */
    public function __construct(Config $config)
    {
        $this->setConfig($config);
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
     * @return Broker
     */
    protected function getBroker(): Broker
    {
        if ($this->broker === null) {
            $this->broker = new Broker();
        }
        return $this->broker;
    }

    /**
     * @param Broker $broker
     */
    public function setBroker(Broker $broker): void
    {
        $this->broker = $broker;
    }

    /**
     * @return Config|mixed
     */
    protected function getConfig()
    {
        return $this->config;
    }

    /**
     * @param Config $config
     */
    public function setConfig(Config $config): void
    {
        $this->config = $config;
    }

    /**
     * @return Assignment
     */
    public function getAssignment(): Assignment
    {
        return $this->assignment;
    }

    /**
     * @param Assignment $assignment
     */
    public function setAssignment(Assignment $assignment): void
    {
        $this->assignment = $assignment;
    }
}
