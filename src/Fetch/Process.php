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
use EasySwoole\Kafka\Exception;
use EasySwoole\Kafka\Config\FetchConfig;
use EasySwoole\Kafka\Protocol;
use EasySwoole\Log\Logger;

class Process extends BaseProcess
{
    use Singleton;

    /**
     * Process constructor.
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

        $connect = $broker->getMetaConnect($broker->getGroupBrokerId());

        if ($connect === null) {
            return [];
        }

        $data = [];

        foreach ($this->config->getTopics() as $topicName) {
            if (empty($offsets[$topicName])) {
                continue;
            }

            $item = [
                'topic_name' => $topicName,
                'partitions' => [],
            ];

            foreach ($offsets[$topicName] as $partId => $offset) {
                $item['partitions'][] = [
                    'partition_id'      => $partId,
                    'offset'            => $offset > 0 ? $offset: 0,
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

        return $ret;
    }

    protected function getConfig(): FetchConfig
    {
        return FetchConfig::getInstance();
    }
}
