<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/18
 * Time: 下午3:53
 */
namespace EasySwoole\Kafka\Producer;

use EasySwoole\Kafka\Broker;
use EasySwoole\Kafka\Exception\ConnectionException;
use EasySwoole\Kafka\Exception\Exception;
use EasySwoole\Kafka\ProducerConfig;
use EasySwoole\Kafka\Protocol;
use EasySwoole\Log\Logger;

class Process
{
    private $recordValidator;

    private $logger;

    /**
     * SyncProcess constructor.
     * SyncProcess constructor.
     * @param RecordValidator|null $recordValidator
     * @throws Exception
     */
    public function __construct(?RecordValidator $recordValidator = null)
    {
        $this->recordValidator = $recordValidator ?? new RecordValidator();

        $this->logger = new Logger();

        $config = $this->getConfig();
        Protocol::init($config->getBrokerVersion());

        $broker = $this->getBroker();
        $broker->setConfig($config);
        $broker->setLogger($this->logger);

        $this->getMeta();
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
            $client = $broker->getDataConnect((string) $brokerId, true);
            if ($client === null || ! $client->isConnected()) {
                return [];
            }

            $params = [
                'required_ack' => $requiredAck,
                'timeout'      => $timeout,
                'data'         => $topicList,
                'compression'  => $compression,
            ];
            $this->logger->log('Send message start, params:' . json_encode($params), 1);
            $requestData = Protocol::encode(Protocol::PRODUCE_REQUEST, $params);
            $data = $client->send($requestData);
            if ($requiredAck !== 0) { // If it is 0 the server will not send any response
                // todo 解包失败
                $ret = Protocol::decode(Protocol::PRODUCE_REQUEST, substr($data, 4));

                $result[] = $ret;
            }
        }
        return $result;
    }

    /**
     * 元数据meta
     * 获取topics和brokers信息
     * @throws ConnectionException
     * @throws Exception
     */
    public function getMeta(): void
    {
        $this->logger->log('Start sync metadata request', 1);

        $brokerList = ProducerConfig::getInstance()->getMetadataBrokerList();
        $brokerHost = [];
        foreach (explode(',', $brokerList) as $key => $val) {
            if (trim($val)) {
                $brokerHost[] = $val;
            }
        }
        if (count($brokerHost) === 0) {
            throw new Exception('No valid broker configured');
        }

        shuffle($brokerHost);
        $broker = $this->getBroker();

        foreach ($brokerHost as $host) {
            $client = $broker->getMetaConnect($host, true);
            if (! $client->isConnected()) {
                continue;
            }

            $params = [];
            $this->logger->log('Start sync metadata request params:' . json_encode($params));

            $requestData = Protocol::encode(Protocol::METADATA_REQUEST, $params);
            $data = $client->send($requestData);
            $dataLen = Protocol\Protocol::unpack(Protocol\Protocol::BIT_B32, substr($data, 0, 4));
            $correlationId = Protocol\Protocol::unpack(Protocol\Protocol::BIT_B32, substr($data, 4, 4));
            // 0-4字节是包头长度
            // 4-8字节是数组元素个数
            // 从第9个字节开始才是数据正文
            $result = Protocol::decode(Protocol::METADATA_REQUEST, substr($data, 8));
            if (! isset($result['brokers'], $result['topics'])) {
                throw new Exception("Get metadata is fail, brokers or topics is null.");
            }
            // 更新 topics和brokers
            $broker->setData($result['topics'], $result['brokers']);

            return;
        }

        throw new ConnectionException($brokerList);
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
        $topics     = $broker->getTopics();// syncMeta获取 broker和topics数据

        foreach ($recordSet as $record) {
            $this->recordValidator->validate($record, $topics);

            $topicMeta  = $topics[$record['topic']];
            $partNums   = array_keys($topicMeta);
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
                $partition['messages'][] = ['value' => $record['value'], 'key' => $record['key']];
            } else {
                $partition['messages'][] = $record['value'];
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