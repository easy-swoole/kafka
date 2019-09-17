<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/17
 * Time: 下午11:00
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Producer\Process;
use EasySwoole\Log\Logger;

class Producer
{

    protected $logger;

    private $process;

    /**
     * Producer constructor.
     * @param callable|null $producer
     * @throws Exception\Exception
     */
    public function __construct(?callable $producer = null)
    {
        $this->process = new Process();
        $this->logger = new Logger();
    }

    /**
     * @param array $data
     * @return array|null
     * @throws Exception\Exception
     * @throws Exception\InvalidRecordInSet
     */
    public function send(array $data): ?array
    {
        $this->logger->log('Start send producer data');

        return $this->process->send($data);

//        // 同步
//        if (is_array($data)) {
//            return $this->sendSynchronously($data);
//        }
//        // 异步
//        $this->sendAsynchronously($data);
//
//        return null;
    }

    /**
     * @param array $data
     * @return array
     * @throws Exception\InvalidRecordInSet
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