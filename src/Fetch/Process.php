<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 上午8:48
 */
namespace EasySwoole\Kafka\Fetch;

use EasySwoole\Kafka\BaseProcess;
use EasySwoole\Kafka\Exception;
use EasySwoole\Kafka\Config\FetchConfig;
use EasySwoole\Kafka\Protocol;
use EasySwoole\Log\Logger;

class Process extends BaseProcess
{
    /**
     * Process constructor.
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function __construct()
    {
        parent::__construct();

        $this->config = $this->getConfig();
        Protocol::init($this->config->getBrokerVersion());
        $this->getBroker()->setConfig($this->config);

//        $this->syncMeta();
    }

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

        $broker = $this->getBroker();

        $result = [];
        foreach ($this->brokerHost as $host) {
            $connect = $broker->getMetaConnect($host);

            if ($connect === null) {
                continue;
            }

            $data = [];

            foreach ($this->config->getTopics() as $topicName) {
                if (empty($offsets[$host])) {
                    continue;
                }

                $item = [
                    'topic_name' => $topicName,
                    'partitions' => [],
                ];

                foreach ($offsets[$host] as $partition => $offset) {
                    $item['partitions'][] = [
                        'partition_id'      => $partition,
                        'offset'            => $offset,
                        'max_bytes'         => $this->getConfig()->getMaxBytes(),
                    ];
                }

                $data[] = $item;
            }

            $params = [
                'max_wait_time'     => $this->getConfig()->getMaxWaitTime(),
                'min_bytes'         => $this->getConfig()->getMinBytes(),
                'replica_id'        => -1,
                'data'              => $data,
            ];

            $this->logger->log('Fetch message start, params:' . json_encode($params), Logger::LOG_LEVEL_INFO);
            $requestData = Protocol::encode(Protocol::FETCH_REQUEST, $params);
            $data = $connect->send($requestData);
            $ret = Protocol::decode(Protocol::FETCH_REQUEST, substr($data, 8));
            $result[$host] = $ret;
        }

        return $result;
    }

    protected function getConfig(): FetchConfig
    {
        return FetchConfig::getInstance();
    }
}
