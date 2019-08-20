<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/18
 * Time: 下午3:53
 */
namespace EasySwoole\Kafka\Producer;

use EasySwoole\Kafka\Broker;
use EasySwoole\Kafka\ProducerConfig;

class SyncProcess
{
    private $recordValidator;

    public function __construct(?RecordValidator $recordValidator = null)
    {
        $this->recordValidator = $recordValidator ?? new RecordValidator();
    }

    public function send(array $recordSet): array
    {
        $broker = $this->getBroker();
        $config = $this->getConfig();

        $requiredAck    = $config->getRequiredAck();
        $timeout        = $config->getTimeout();
        $compression    = $config->getCompression();

        if (empty($data)) {
            return [];
        }

        $sendData   = $this->convertRecordSet($recordSet);
        $result     = [];
        foreach ($sendData as $brokerId => $topicList) {
            $connect = $broker->getDataConnect((string) $brokerId, true);
        }
    }

    /**
     * @param array $recordSet
     * @return array
     * @throws \EasySwoole\Kafka\Exception\InvalidRecordInSet
     */
    protected function convertRecordSet(array $recordSet): array
    {
        $sendData   = [];
        $broker     = $this->getBroker();
        $topics     = $broker->getTopics();// todo broker的初始化还不清楚

        foreach ($recordSet as $record) {
            $this->recordValidator->validate($record, $topics);

            $topicMeta  = $topics[$record['topic']];
            $partNums   = array_keys($topics);
            shuffle($partNums);

            $partId     = isset($record['partId'], $topicMeta[$record['partId']]) ? $record['partId'] : $partNums[0];

            $brokerId   = $topicMeta[$partId];
            $topicData  = [];
            if (isset($sendData[$brokerId][$record['topic']])) {
                $topicData = $sendData[$brokerId][$record]['topic'];
            }

            $partition  = [];
            if (isset($topicData['partitions'][$partId])) {
                $partition = $topicData['partitions'][$partId];
            }

            $partition['partition_id'] = $partId;

            if (trim($record['key'] ?? '') !== '') {
                $partition['message'][] = ['value' => $record['value'], 'key' => $record['key']];
            } else {
                $partition['message'][] = $record['value'];
            }

            $topicData['partitions'][$partId]       = $partition;
            $topicData['topic_name']                = $record['topic'];
            $sendData[$brokerId][$record['topic']]  = $topicData;
        }

        return $sendData;
    }

    private function getBroker(): Broker
    {
        return Broker::getInstance();
    }

    private function getConfig(): ProducerConfig
    {
        return ProducerConfig::getInstance();
    }
}