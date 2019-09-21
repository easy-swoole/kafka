<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 上午10:26
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Offset\Process;

class Offset
{
    private $process;

    public function __construct()
    {
        $this->process = new Process();
    }

    /**
     * @return array|null
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function listOffset(): ?array
    {
        return $this->process->offset();
    }

    /**
     * @return array|null
     * @throws Exception\Config
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function fetchOffset(): ?array
    {
        return $this->process->fetchOffset();
    }

    /**
     * @return array|null
     * @throws Exception\Config
     * @throws Exception\Exception
     */
    public function commit(): ?array
    {
        return $this->process->commit();
    }
}
