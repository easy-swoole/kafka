<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 下午1:49
 */
namespace EasySwoole\Kafka\Heartbeat;

use EasySwoole\Kafka\BaseProcess;
use EasySwoole\Kafka\Protocol;

class Process extends BaseProcess
{

    /**
     * Process constructor.
     */
    public function __construct()
    {
        parent::__construct();

        $config = $this->getConfig();
        Protocol::init($config->getBrokerVersion());

        $broker = $this->getBroker();
        $broker->setConfig($config);

        $this->syncMeta();
    }

    /**
     * @return array
     * @throws \EasySwoole\Kafka\Exception\ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function heartbeat(): array
    {
        $broker        = $this->getBroker();
        $groupBrokerId = $broker->getGroupBrokerId();
        $connect       = $broker->getMetaConnect((string) $groupBrokerId);

        if ($connect === null) {
            return [];
        }

        $assign   = $this->getAssignment();
        $memberId = $assign->getMemberId();

        if (trim($memberId) === '') {
            return [];
        }

        $generationId = $assign->getGenerationId();

        $params = [
            'group_id'      => $this->getConfig()->getGroupId(),
            'generation_id' => $generationId,
            'member_id'     => $memberId,
        ];

        $requestData = Protocol::encode(Protocol::HEART_BEAT_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::PRODUCE_REQUEST, substr($data, 4));

        return $ret;
    }
}
