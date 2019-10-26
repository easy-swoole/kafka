<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 下午1:43
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Consumer\Assignment;
use EasySwoole\Kafka\Heartbeat\Process;

class Heartbeat
{
    private $process;

    /**
     * Heartbeat constructor.
     * @param ConsumerConfig $config
     * @param Assignment     $assignment
     * @param Broker         $broker
     * @throws Exception\Exception
     */
    public function __construct(ConsumerConfig $config, Assignment $assignment, Broker $broker)
    {
        $this->process = new Process($config, $assignment, $broker);
    }

    /**
     * @return array
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function beat()
    {
        return $this->process->heartbeat();
    }
}
