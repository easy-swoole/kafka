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
     * @throws Exception\Exception
     */
    public function __construct()
    {
        $this->process = new Process();
    }

    /**
     * @return array|null
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function fetch(): ?array
    {
        return $this->process->fetch();
    }
}
