<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/20
 * Time: 上午10:17
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\ApiVersions\Process;

class ApiVersions
{
    private $process;

    /**
     * ApiVersions constructor.
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
    public function getVersions(): ?array
    {
        return $this->process->apiVersions();
    }
}
