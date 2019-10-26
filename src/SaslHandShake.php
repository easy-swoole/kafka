<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/24
 * Time: 下午5:46
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Config\Config;
use EasySwoole\Kafka\SaslHandShake\Process;

class SaslHandShake
{
    private $process;

    /**
     * SaslHandShake constructor.
     * @param Config $config
     * @param Broker $broker
     * @throws Exception\Exception
     */
    public function __construct(Config $config, Broker $broker)
    {
        $this->process = new Process($config, $broker);
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
