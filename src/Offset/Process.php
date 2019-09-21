<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 上午10:28
 */
namespace EasySwoole\Kafka\Offset;

use EasySwoole\Kafka\BaseProcess;
use EasySwoole\Kafka\Broker;
use EasySwoole\Kafka\Config\OffsetConfig;
use EasySwoole\Kafka\Protocol;
use EasySwoole\Log\Logger;

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
    public function offset(): array
    {
        $context = [];
        $broker  = $this->getBroker();
        $topics  = $this->getAssignment()->getTopics();

        foreach ($topics as $brokerId => $topicList) {
            $connect = $broker->getMetaConnect((string) $brokerId);

            if ($connect === null) {
                return [];
            }

            $data = [];
            foreach ($topicList as $topic) {
                $item = [
                    'topic' => $topic['topic_name'],
                    'partitions' => [],
                ];

                foreach ($topic['partitions'] as $partId) {
                    $item['partitions'][] = [
                        'partition_id' => $partId,
                        'offset' => 1,
                        'time' =>  -1,
                    ];
                    $data[]               = $item;
                }
            }

            $params = [
                'replica_id' => -1,
                'data'       => $data,
            ];

            $requestData = Protocol::encode(Protocol::OFFSET_REQUEST, $params);

            $data = $connect->send($requestData);
            $ret = Protocol::decode(Protocol::OFFSET_REQUEST, substr($data, 4));
            $context[] = $ret;
        }

        return $context;
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\Config
     * @throws \EasySwoole\Kafka\Exception\ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function fetchOffset(): array
    {
        $broker        = $this->getBroker();
        $groupBrokerId = $broker->getGroupBrokerId();
        $connect       = $broker->getMetaConnect((string) $groupBrokerId);

        if ($connect === null) {
            return [];
        }

        $topics = $this->getAssignment()->getTopics();
        $data   = [];

        foreach ($topics as $brokerId => $topicList) {
            foreach ($topicList as $topic) {
                $partitions = [];

                if (isset($data[$topic['topic_name']]['partitions'])) {
                    $partitions = $data[$topic['topic_name']]['partitions'];
                }

                foreach ($topic['partitions'] as $partId) {
                    $partitions[] = $partId;
                }

                $data[$topic['topic_name']]['partitions'] = $partitions;
                $data[$topic['topic_name']]['topic_name'] = $topic['topic_name'];
            }
        }

        $params = [
            'group_id' => $this->getConfig()->getGroupId(),
            'data'     => $data,
        ];

        $requestData = Protocol::encode(Protocol::OFFSET_FETCH_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::OFFSET_FETCH_REQUEST, substr($data, 4));

        return $ret;
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\Config
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function commit(): array
    {
        $config = $this->getConfig();

        if ($config->getConsumeMode() === ConsumerConfig::CONSUME_BEFORE_COMMIT_OFFSET) {
            $this->consumeMessage();
        }

        $broker        = $this->getBroker();
        $groupBrokerId = $broker->getGroupBrokerId();
        $connect       = $broker->getMetaConnect((string) $groupBrokerId);

        if ($connect === null) {
            return [];
        }

        $commitOffsets = $this->getAssignment()->getCommitOffsets();
        $topics        = $this->getAssignment()->getTopics();
        $this->getAssignment()->setPreCommitOffsets($commitOffsets);
        $data = [];

        foreach ($topics as $brokerId => $topicList) {
            foreach ($topicList as $topic) {
                $partitions = [];

                if (isset($data[$topic['topic_name']]['partitions'])) {
                    $partitions = $data[$topic['topic_name']]['partitions'];
                }

                foreach ($topic['partitions'] as $partId) {
                    if ($commitOffsets[$topic['topic_name']][$partId] === -1) {
                        continue;
                    }

                    $partitions[$partId]['partition'] = $partId;
                    $partitions[$partId]['offset']    = $commitOffsets[$topic['topic_name']][$partId];
                }

                $data[$topic['topic_name']]['partitions'] = $partitions;
                $data[$topic['topic_name']]['topic_name'] = $topic['topic_name'];
            }
        }

        $params = [
            'group_id' => $this->getConfig()->getGroupId(),
            'generation_id' => $this->getAssignment()->getGenerationId(),
            'member_id' => $this->getAssignment()->getMemberId(),
            'data' => $data,
        ];

        $this->logger->log('Commit current fetch offset start, params:' . json_encode($params), Logger::LOG_LEVEL_INFO);
        $requestData = Protocol::encode(Protocol::OFFSET_COMMIT_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::OFFSET_FETCH_REQUEST, substr($data, 4));

        return $ret;
    }

    private function getBroker(): Broker
    {
        return Broker::getInstance();
    }

    private function getConfig(): OffsetConfig
    {
        return OffsetConfig::getInstance();
    }
}
