<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/17
 * Time: 下午11:00
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Consumer\Process;

class Consumer
{
    /**
     * @var Process|null
     */
    private $process;

    /**
     * Consumer constructor.
     * @param callable|null $func
     * @throws Exception\Exception
     */
    public function __construct(?callable $func = null)
    {
        $this->process = new Process($func);
    }

    /**
     * @throws \Throwable
     */
    public function subscribe()
    {
        $this->process->subscribe();
    }


    /**
     * @throws Exception\Config
     */
    public function stop()
    {
        // todo
        ConsumerConfig::getInstance()->setConsumeStatus(false);
    }
}
