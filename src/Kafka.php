<?php
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Config\Config;
use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Config\ProducerConfig;

class Kafka
{

    private $consumerConfig;

    private $producerConfig;

    private $consumer;

    private $producer;

    public function __construct(Config $config)
    {
        if ($config instanceof ConsumerConfig) {
            $this->consumerConfig = $config;
        } elseif ($config instanceof ProducerConfig) {
            $this->producerConfig = $config;
        }
    }

    /**
     * @return Consumer
     * @throws Exception\Exception
     */
    public function consumer()
    {
        if ($this->consumer === null) {
            $this->consumer = new Consumer($this->consumerConfig);
        }
        return $this->consumer;
    }

    /**
     * @return Producer
     * @throws Exception\Exception
     */
    public function producer()
    {
        if ($this->producer === null) {
            $this->producer = new Producer($this->producerConfig);
        }
        return $this->producer;
    }
}
