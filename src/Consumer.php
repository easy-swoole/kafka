<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/17
 * Time: 下午11:00
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Consumer\Process;
use EasySwoole\Kafka\Consumer\StopStrategy;

class Consumer
{

    /**
     * @var StopStrategy|null
     */
    private $stopStrategy;

    /**
     * @var Process|null
     */
    private $process;

    /**
     * Consumer constructor.
     * @param callable $func
     * @throws Exception\Exception
     */
    public function __construct(callable $func)
    {
        $this->process = new Process($func);
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function subscribe()
    {
        $this->process->subscribe();
    }
}
