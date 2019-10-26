<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 下午3:54
 */
namespace EasySwoole\Kafka\Group;

use EasySwoole\Kafka\BaseProcess;
use EasySwoole\Kafka\Broker;
use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Consumer\Assignment;
use EasySwoole\Kafka\Exception\ConnectionException;
use EasySwoole\Kafka\Protocol;

class Process extends BaseProcess
{
    /**
     * Process constructor.
     * @param ConsumerConfig $config
     * @param Assignment     $assignment
     * @param Broker         $broker
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function __construct(ConsumerConfig $config, Assignment $assignment, Broker $broker)
    {
        parent::__construct($config);

        $this->setAssignment($assignment);
        $this->setBroker($broker);
    }

    /**
     * @return array
     * @throws ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function getGroupBrokerId(): array
    {
        $broker  = $this->getBroker();
        $connect = $broker->getRandConnect();

        if ($connect === null) {
            throw new ConnectionException();
        }

        $params = ['group_id' => $this->getConfig()->getGroupId()];

        $requestData = Protocol::encode(Protocol::GROUP_COORDINATOR_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::GROUP_COORDINATOR_REQUEST, substr($data, 8));
        return $ret;
    }

    /**
     * @return array
     * @throws ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function joinGroup(): array
    {
        $connect = $this->getBroker()->getMetaConnect($this->getBroker()->getGroupBrokerId());

        if ($connect === null) {
            throw new ConnectionException();
        }

        $params = [
            'group_id'          => $this->getConfig()->getGroupId(),
            'session_timeout'   => $this->getConfig()->getSessionTimeout(),
            'rebalance_timeout' => $this->getConfig()->getRebalanceTimeout(),
            'member_id'         => $this->getAssignment()->getMemberId(),
            'data'              => [
                [
                    'protocol_name' => 'group',
                    'version'       => 0,
                    'subscription'  => ['The group id is ' . $this->getConfig()->getGroupId()],
                    'user_data'     => '',
                ],
            ],
        ];

        $requestData = Protocol::encode(Protocol::JOIN_GROUP_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::JOIN_GROUP_REQUEST, substr($data, 8));
        return $ret;
    }

    /**
     * @return array
     * @throws ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function leaveGroup(): array
    {
        $connect = $this->getBroker()->getMetaConnect($this->getBroker()->getGroupBrokerId());

        if ($connect === null) {
            throw new ConnectionException();
        }

        $params = [
            'group_id'          => $this->getConfig()->getGroupId(),
            'member_id'         => $this->getAssignment()->getMemberId(),
        ];

        $requestData = Protocol::encode(Protocol::LEAVE_GROUP_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::LEAVE_GROUP_REQUEST, substr($data, 8));

        return $ret;
    }

    /**
     * @return array
     * @throws ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function syncGroupOnJoinLeader(): array
    {
        $connect = $this->getBroker()->getMetaConnect($this->getBroker()->getGroupBrokerId());

        if ($connect === null) {
            throw new ConnectionException();
        }

        $assign       = $this->getAssignment();
        $memberId     = $assign->getMemberId();
        $generationId = $assign->getGenerationId();

        $params = [
            'group_id'      => $this->getConfig()->getGroupId(),
            'generation_id' => $generationId,
            'member_id'     => $memberId,
            'data'          => $assign->getAssignments(),// leader可以同步data数据
        ];

        $requestData = Protocol::encode(Protocol::SYNC_GROUP_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::SYNC_GROUP_REQUEST, substr($data, 8));

        return $ret;
    }

    /**
     * @return array
     * @throws ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function syncGroupOnJoinFollower(): array
    {
        $connect = $this->getBroker()->getMetaConnect($this->getBroker()->getGroupBrokerId());

        if ($connect === null) {
            throw new ConnectionException();
        }

        $assign       = $this->getAssignment();
        $memberId     = $assign->getMemberId();
        $generationId = $assign->getGenerationId();
        $params = [
            'group_id'      => $this->getConfig()->getGroupId(),
            'generation_id' => $generationId,
            'member_id'     => $memberId,
            'data'          => []
        ];
        $requestData = Protocol::encode(Protocol::SYNC_GROUP_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::SYNC_GROUP_REQUEST, substr($data, 8));

        return $ret;
    }

    /**
     * @return array
     * @throws ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function describeGroups(): array
    {
        $connect = $this->getBroker()->getMetaConnect($this->getBroker()->getGroupBrokerId());

        if ($connect === null) {
            throw new ConnectionException();
        }

        $params = [
            'groups' => $this->getConfig()->getGroupId(),
        ];

        $requestData = Protocol::encode(Protocol::DESCRIBE_GROUPS_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::DESCRIBE_GROUPS_REQUEST, substr($data, 8));

        return $ret;
    }

    /**
     * @return array
     * @throws ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function listGroup(): array
    {
        $connect = $this->getBroker()->getMetaConnect($this->getBroker()->getGroupBrokerId());

        if ($connect === null) {
            throw new ConnectionException();
        }

        $params = [];

        $requestData = Protocol::encode(Protocol::LIST_GROUPS_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::LIST_GROUPS_REQUEST, substr($data, 8));

        return $ret;
    }
}
