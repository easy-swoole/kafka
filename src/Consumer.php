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
     * @param ConsumerConfig $config
     * @throws Exception\Exception
     */
    public function __construct(ConsumerConfig $config)
    {
        $this->process = new Process($config);
    }

    /**
     * @param callable|null $func
     * @param float         $breakTime
     * @param int           $maxCurrency
     * @throws \Throwable
     */
    public function subscribe(?callable $func = null, $breakTime = 0.01, $maxCurrency = 128)
    {
        $this->process->subscribe($func, $breakTime, $maxCurrency);
    }

    public function stop()
    {
        $this->process->stop();
    }
}
