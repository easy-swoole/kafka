<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/24
 * Time: 下午5:46
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\SaslHandShake\Process;

class SaslHandShake
{
    private $process;

    /**
     * SaslHandShake constructor.
     * @throws Exception\Exception
     */
    public function __construct()
    {
        $this->process = new Process();
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function handShake()
    {
        return $this->process->handShake();
    }
}
