<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/18
 * Time: 下午3:53
 */
namespace EasySwoole\Kafka\Producer;

use EasySwoole\Kafka\BaseProcess;
use EasySwoole\Kafka\Exception\Exception;
use EasySwoole\Kafka\Config\ProducerConfig;
use EasySwoole\Kafka\Protocol;

class Process extends BaseProcess
{
    private $recordValidator;

    /**
     * SyncProcess constructor.
     * SyncProcess constructor.
     * @param RecordValidator|null $recordValidator
     * @throws Exception
     */
    public function __construct(?RecordValidator $recordValidator = null)
    {
        parent::__construct();

        $this->recordValidator = $recordValidator ?? new RecordValidator();

        $this->config = $this->getConfig();
        $this->getBroker()->setConfig($this->config);

        \EasySwoole\Kafka\SyncMeta\Process::getInstance()->syncMeta();
    }

    /**
     * 发送数据
     * @param array $recordSet
     * @return array
     * @throws Exception
     * @throws \EasySwoole\Kafka\Exception\InvalidRecordInSet
     */
    public function send(array $recordSet): array
    {
        $broker = $this->getBroker();
        $config = $this->getConfig();

        $requiredAck    = $config->getRequiredAck();
        $timeout        = $config->getTimeout();
        $compression    = $config->getCompression();

        if (empty($recordSet)) {
            return [];
        }

        // 处理数据
        $sendData   = $this->convertRecordSet($recordSet);
        $result     = [];
        foreach ($sendData as $brokerId => $topicList) {
            $client = $broker->getDataConnect((string) $brokerId);
            if ($client === null || ! $client->isConnected()) {
                return [];
            }

            $params = [
                'transactional_id' => null,
                'required_ack' => $requiredAck,
                'timeout'      => $timeout,
                'data'         => $topicList,
                'compression'  => $compression,
            ];

            $requestData = Protocol::encode(Protocol::PRODUCE_REQUEST, $params);
            $data = $client->send($requestData);
            if ($requiredAck !== 0) { // If it is 0 the server will not send any response
                $correlationId = Protocol\Protocol::unpack(Protocol\Protocol::BIT_B32, substr($data, 0, 4));
                $ret = Protocol::decode(Protocol::PRODUCE_REQUEST, substr($data, 8));
                $result[] = $ret;
            }
        }
        return $result;
    }

    /**
     * @param array $recordSet
     * @return array
     * @throws \EasySwoole\Kafka\Exception\InvalidRecordInSet
     */
    protected function convertRecordSet(array $recordSet): array
    {
        $sendData = [];
        $broker   = $this->getBroker();
        $topics   = $broker->getTopics(); // syncMeta获取 broker和topics数据

        foreach ($recordSet as $record) {
            $this->recordValidator->validate($record, $topics);

            $topicMeta = $topics[$record['topic']];
            $partNums  = array_keys($topicMeta);
            shuffle($partNums);

            $partId = isset($record['partId'], $topicMeta[$record['partId']]) ? $record['partId'] : $partNums[0];

            $brokerId  = $topicMeta[$partId];
            $topicData = [];
            if (isset($sendData[$brokerId][$record['topic']])) {
                $topicData = $sendData[$brokerId][$record['topic']];
            }

            $partition = [];
            if (isset($topicData['partitions'][$partId])) {
                $partition = $topicData['partitions'][$partId];
            }

            $partition['partition_id'] = $partId;

            if (trim($record['key'] ?? '') !== '') {
                $partition['messages'][] = ['value' => $record['value'], 'key' => $record['key']];
            } else {
                $partition['messages'][] = $record['value'];
            }

            $topicData['partitions'][$partId]      = $partition;
            $topicData['topic_name']               = $record['topic'];
            $sendData[$brokerId][$record['topic']] = $topicData;
        }

        return $sendData;
    }

    /**
     * @return ProducerConfig
     */
    protected function getConfig(): ProducerConfig
    {
        return ProducerConfig::getInstance();
    }
}