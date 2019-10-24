<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 上午8:48
 */
namespace EasySwoole\Kafka\Fetch;

use EasySwoole\Component\Singleton;
use EasySwoole\Kafka\BaseProcess;
use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Consumer\Assignment;
use EasySwoole\Kafka\Exception;
use EasySwoole\Kafka\Exception\ConnectionException;
use EasySwoole\Kafka\Protocol;

class Process extends BaseProcess
{
    use Singleton;

    /**
     * @param array $offsets
     * @return array
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function fetch(array $offsets = []): array
    {
        if (empty($offsets)) {
            return [];
        }

        foreach ($this->getAssignment()->getTopics() as $nodeId => $topics) {
            $data = [];
            foreach ($topics as $topicName => $partitions) {
                $item = [
                    'topic_name' => $topicName,
                    'partitions' => [],
                ];
                foreach ($offsets[$topicName] as $partId => $offset) {

                    if (in_array($partId, $partitions['partitions'])) {

                        $item['partitions'][] = [
                            'partition_id' => $partId,
                            'offset' => $offset > 0 ? $offset : 0,
                            'max_bytes' => ConsumerConfig::getInstance()->getMaxBytes(),
                        ];
                    }
                }
                $data[] = $item;
            }
            $params = [
                'max_wait_time'     => ConsumerConfig::getInstance()->getMaxWaitTime(),
                'min_bytes'         => ConsumerConfig::getInstance()->getMinBytes(),
                'replica_id'        => -1,
                'data'              => $data,
            ];

            $connect = $this->getBroker()->getMetaConnect($nodeId);
            if ($connect === null) {
                throw new ConnectionException();
            }
            $requestData = Protocol::encode(Protocol::FETCH_REQUEST, $params);
            $data = $connect->send($requestData);
            $ret[] = Protocol::decode(Protocol::FETCH_REQUEST, substr($data, 8));
        }
        if(!empty($ret)) {
            $allTopicName = [];
            $throttleTime = [];
            foreach ($ret as $keyRet => $valueRet){
                foreach ($valueRet['topics'] as $keyTopics => $valueTopics){
                    $allTopicName['topics'][$valueTopics['topicName']]['topicName'] = $valueTopics['topicName'];
                    foreach ($valueTopics['partitions'] as $keyPartitions => $valuePartitions){
                        if(!isset($throttleTime[$valueTopics['topicName']])){
                            $throttleTime[$valueTopics['topicName']] = $valueRet['throttleTime'];
                        }
                        $allTopicName['topics'][$valueTopics['topicName']]['partitions'][] = $valuePartitions;
                    }
                }
            }
            $res = [];
            foreach ($allTopicName['topics'] as $keyRes => $valueRes){
                if(!isset($res['throttleTime'])){
                    $res['throttleTime'] = $throttleTime[$keyRes];
                }
                $res['topics'][] = $valueRes;
            }
        }
        return $res ?? [];
    }

    protected function getAssignment(): Assignment
    {
        return Assignment::getInstance();
    }
}
