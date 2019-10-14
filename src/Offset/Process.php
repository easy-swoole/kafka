<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 上午10:28
 */
namespace EasySwoole\Kafka\Offset;

use EasySwoole\Component\Singleton;
use EasySwoole\Kafka\BaseProcess;
use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Consumer\Assignment;
use EasySwoole\Kafka\Exception\ConnectionException;
use EasySwoole\Kafka\Protocol;

class Process extends BaseProcess
{
    use Singleton;

    /**
     * @return array
     * @throws ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function listOffset(): array
    {
        $topics     = $this->getBroker()->getTopics();
        $topicList  = ConsumerConfig::getInstance()->getTopics();

        $connect = $this->getBroker()->getMetaConnect($this->getBroker()->getGroupBrokerId());

        if ($connect === null) {
            throw new ConnectionException();
        }

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

                foreach ($partitions as $partId => $leader) {
                    $item['partitions'][] = [
                        'partition_id' => $partId,
                        'offset' => 100,
                        'time' =>  -1,
                    ];
                }
                $data[] = $item;
            }
        }

        $params = [
            'replica_id' => -1,
            'data'       => $data,
        ];

        $requestData = Protocol::encode(Protocol::OFFSET_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::OFFSET_REQUEST, substr($data, 8));

        return $ret;
    }

    /**
     * @return array
     * @throws ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Config
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function fetchOffset(): array
    {
        $broker     = $this->getBroker();
        $topics     = $broker->getTopics();
        $topicList  = $this->config->getTopics();

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
            'group_id' => ConsumerConfig::getInstance()->getGroupId(),
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
     * @throws \EasySwoole\Kafka\Exception\Config
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
            'group_id'  => ConsumerConfig::getInstance()->getGroupId(),
            'generation_id' => $this->getAssignment()->getGenerationId(),
            'member_id' => $this->getAssignment()->getMemberId(),
            'data'      => $data,
        ];

        $requestData = Protocol::encode(Protocol::OFFSET_COMMIT_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::OFFSET_COMMIT_REQUEST, substr($data, 8));

        return $ret;
    }

    protected function getAssignment(): Assignment
    {
        return Assignment::getInstance();
    }
}
