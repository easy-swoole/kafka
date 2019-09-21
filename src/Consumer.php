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
use EasySwoole\Log\Logger;

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

    private $logger;

    public function __construct(?StopStrategy $stopStrategy = null)
    {
        $this->stopStrategy = $stopStrategy;
        $this->logger = new Logger();
    }

    /**
     * @param callable|null $consumer
     */
    public function start(?callable $consumer = null): void
    {
        if ($this->process !== null) {
            $this->logger->log('Consumer is already being executed', Logger::LOG_LEVEL_ERROR);
            return;
        }

        $this->setupStopStrategy();

        $this->process = new Process($consumer);
        $this->process->start();

        Loop::run();
    }

    private function setupStopStrategy(): void
    {
        if ($this->stopStrategy === null) {
            return;
        }

        $this->stopStrategy->setup($this);
    }

    public function stop(): void
    {
        if ($this->process === null) {
            $this->logger->log('Consumer is not running', Logger::LOG_LEVEL_ERROR);
            return;
        }

        $this->process->stop();
        $this->process = null;

        Loop::stop();
    }
}
