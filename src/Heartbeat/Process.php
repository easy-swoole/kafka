<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 下午1:49
 */
namespace EasySwoole\Kafka\Heartbeat;

use EasySwoole\Kafka\BaseProcess;
use EasySwoole\Kafka\Broker;
use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Consumer\Assignment;
use EasySwoole\Kafka\Exception\ConnectionException;
use EasySwoole\Kafka\Protocol;

class Process extends BaseProcess
{
    public function __construct(ConsumerConfig $config, Assignment $assignment, Broker $broker)
    {
        parent::__construct($config);

        $this->setAssignment($assignment);
        $this->setBroker($broker);
    }

    /**
     * @return array
     * @throws ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Config
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function heartbeat(): array
    {
        $connect = $this->getBroker()->getMetaConnect($this->getBroker()->getGroupBrokerId());
        if ($connect === null) {
            throw new ConnectionException();
        }

        $params = [
            'group_id'      => (new ConsumerConfig())->getGroupId(),
            'generation_id' => $this->getAssignment()->getGenerationId(),
            'member_id'     => $this->getAssignment()->getMemberId(),
        ];

        $requestData = Protocol::encode(Protocol::HEART_BEAT_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::HEART_BEAT_REQUEST, substr($data, 8));
        return $ret;
    }
}
