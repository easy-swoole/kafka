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
     * @param StopStrategy|null $stopStrategy
     * @throws Exception\Exception
     */
    public function __construct(?StopStrategy $stopStrategy = null)
    {
        $this->stopStrategy = $stopStrategy;
        $this->process = new Process();
    }

    public function subscribe(callable $func)
    {
        $this->process->subscribe($func);
    }
}
