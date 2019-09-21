<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/17
 * Time: 下午11:00
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Producer\Process;

class Producer
{
    private $process;

    /**
     * Producer constructor.
     * @param callable|null $producer
     * @throws Exception\Exception
     */
    public function __construct(?callable $producer = null)
    {
        $this->process = new Process();
    }

    /**
     * @param array $data
     * @return array|null
     * @throws Exception\Exception
     * @throws Exception\InvalidRecordInSet
     */
    public function send(array $data): ?array
    {
        return $this->process->send($data);
    }
}
