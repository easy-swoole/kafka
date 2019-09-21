<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 下午1:43
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Heartbeat\Process;

class Heartbeat
{
    private $process;

    public function __construct()
    {
        $this->process = new Process();
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
