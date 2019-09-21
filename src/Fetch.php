<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 上午8:56
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Fetch\Process;

class Fetch
{
    private $process;

    /**
     * Fetch constructor.
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function __construct()
    {
        $this->process = new Process();
    }

    /**
     * @param $topicName
     * @return array|null
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function fetch($topicName): ?array
    {
        return $this->process->fetch($topicName);
    }
}
