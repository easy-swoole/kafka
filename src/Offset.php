<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 上午10:26
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Consumer\Assignment;
use EasySwoole\Kafka\Offset\Process;

class Offset
{
    private $process;

    /**
     * Offset constructor.
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
    public function listOffset(): array
    {
        return $this->process->listOffset();
    }

    /**
     * @return array
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function fetchOffset(): array
    {
        return $this->process->fetchOffset();
    }

    /**
     * @param array $commitOffsets
     * @return array
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function commit(array $commitOffsets = []): array
    {
        return $this->process->commit($commitOffsets);
    }
}
