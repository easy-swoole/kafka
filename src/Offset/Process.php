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
    public function listOffset(): array
    {
        $assignedTopics     = $this->getAssignment()->getTopics();
        $topicList  = $this->getConfig()->getTopics();
        foreach ($assignedTopics as $nodeId => $topics) {
            $data = [];
            foreach ($topics as $topic => $partitions) {
                foreach ($topicList as $topicName) {
                    if ($topic !== $topicName) {
                        continue;
                    }

                    $item = [
                        'topic_name' => $topic,
                        'partitions' => [],
                    ];

                    foreach ($partitions['partitions'] as $k => $partId) {
                        $item['partitions'][] = [
                            'partition_id' => $partId,
                            'offset' => 100,
                            'time' =>  -1,
                        ];
                    }

                    if (isset($data[$topic])) {
                        $data[$topic]['partitions'] = array_merge($data[$topic]['partitions'], $item['partitions']);
                    } else {
                        $data[$topic] = $item;
                    }
                }
            }
            $data = array_merge($data, []);
            $params = [
                'replica_id' => -1,
                'data'       => $data,
            ];
            $connect = $this->getBroker()->getMetaConnect($nodeId);
            if ($connect === null) {
                throw new ConnectionException();
            }
            $requestData = Protocol::encode(Protocol::OFFSET_REQUEST, $params);
            $data = $connect->send($requestData);
            $ret[] = Protocol::decode(Protocol::OFFSET_REQUEST, substr($data, 8));
        }

        if (!empty($ret)) {
            $result = [];
            for ($i = 0; $i < count($ret); $i++) {
                if ($i == 0) {
                    $result = $ret[$i];
                    continue;
                }
                foreach ($result as $k => $topic) {
                    foreach ($ret[$i] as $queryTopic) {
                        if ($topic['topicName'] == $queryTopic['topicName']) {
                            $result[$k]['partitions'] = array_merge($result[$k]['partitions'], $queryTopic['partitions']);
                            break;
                        }
                    }
                }
            }

        }
        return $result ?? [];
    }

    /**
     * @return array
     * @throws ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function fetchOffset(): array
    {
        $broker     = $this->getBroker();
        $topics     = $broker->getTopics();
        $topicList  = $this->getConfig()->getTopics();

        $connect = $broker->getMetaConnect($broker->getGroupBrokerId());

        if ($connect === null) {
            throw new ConnectionException();
        }

        $data   = [];

        foreach ($topics as $topic => $partitions) {
            foreach ($topicList as $topicName) {
                if ($topic !== $topicName) {
                    continue;
                }
                $partition          = [];

                if (isset($data[$topic]['partitions'])) {
                    $partition      = $data[$topic]['partitions'];
                }

                foreach ($partitions as $partId => $leader) {
                    $partition[]    = $partId;
                }
                $data[$topic]['partitions'] = $partition;
                $data[$topic]['topic_name'] = $topicName;
            }
        }

        $params = [
            'group_id' => $this->getConfig()->getGroupId(),
            'data'     => $data,
        ];

        $requestData    = Protocol::encode(Protocol::OFFSET_FETCH_REQUEST, $params);
        $data           = $connect->send($requestData);
        $ret            = Protocol::decode(Protocol::OFFSET_FETCH_REQUEST, substr($data, 8));

        return $ret;
    }

    /**
     * @param array $commitOffsets
     * @return array
     * @throws ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function commit(array $commitOffsets = []): array
    {
        $data = [];
        $connect = $this->getBroker()->getMetaConnect($this->getBroker()->getGroupBrokerId());

        if ($connect === null) {
            throw new ConnectionException();
        }

        foreach ($commitOffsets as $topicName => $topics) {
            $partitions = [];
            foreach ($topics as $partId => $offset) {
                $partitions[$partId]['partition']    = $partId;
                $partitions[$partId]['offset']       = $offset;
            }

            $data[$topicName]['partitions'] = $partitions;
            $data[$topicName]['topic_name'] = $topicName;
        }

        $params = [
            'group_id'  => $this->getConfig()->getGroupId(),
            'generation_id' => $this->getAssignment()->getGenerationId(),
            'member_id' => $this->getAssignment()->getMemberId(),
            'data'      => $data,
        ];

        $requestData = Protocol::encode(Protocol::OFFSET_COMMIT_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::OFFSET_COMMIT_REQUEST, substr($data, 8));

        return $ret;
    }
}
