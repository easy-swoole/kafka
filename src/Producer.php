<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/17
 * Time: 下午11:00
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Producer\Process;
use EasySwoole\Kafka\Producer\SyncProcess;
use EasySwoole\Log\Logger;

class Producer
{

    protected $logger;

    private $process;

    public function __construct(?callable $producer = null)
    {
        $this->process = $producer === null ? new SyncProcess() : new Process($producer);
        $this->logger = new Logger();
    }

    /**
     * @param bool|array $data
     * @return array|null
     * @throws Exception
     */
    public function send($data = true): ?array
    {
        $this->logger->log('Start send producer data');

        // $data 数组，走同步
        if (is_array($data)) {
            return $this->sendSynchronously($data);
        }

        // $data 非数组，走异步
        $this->sendAsynchronously($data);

        return null;
    }

    /**
     * Send message synchronously
     * @param array $data
     * @return array
     * @throws Exception
     */
    public function sendSynchronously(array $data): array
    {
        if (! $this->process instanceof SyncProcess) {
            throw new Exception("An asynchronous process is not able to send message synchronously.");
        }

        return $this->process->send();
    }

    /**
     * Send messages asynchronously
     * @param bool $startLoop
     * @throws Exception
     */
    public function sendAsynchronously(bool $startLoop): void
    {
        if ($this->process instanceof SyncProcess) {
            throw new Exception("A synchronous process is not able to send messages asynchronously.");
        }

        $this->process->start();

        if ($startLoop) {
            Loop::run();
        }
    }
}