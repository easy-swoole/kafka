<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 下午1:49
 */
namespace EasySwoole\Kafka\Heartbeat;

use EasySwoole\Component\Singleton;
use EasySwoole\Kafka\BaseProcess;
use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Consumer\Assignment;
use EasySwoole\Kafka\Protocol;
use EasySwoole\Log\Logger;

class Process extends BaseProcess
{
    use Singleton;
    /**
     * Process constructor.
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function __construct()
    {
        parent::__construct();

        $this->config = $this->getConfig();
        Protocol::init($this->config->getBrokerVersion());
        $this->getBroker()->setConfig($this->config);
    }

    /**
     * @return array
     * @throws \EasySwoole\Kafka\Exception\ConnectionException
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function heartbeat(): array
    {
        $connect = $this->getBroker()->getMetaConnect($this->getBroker()->getGroupBrokerId());
        if ($connect === null) {
            return [];
        }

        $params = [
            'group_id'      => ConsumerConfig::getInstance()->getGroupId(),
            'generation_id' => Assignment::getInstance()->getGenerationId(),
            'member_id'     => Assignment::getInstance()->getMemberId(),
        ];

        $this->logger->log('Heartbeat params:' . json_encode($params), Logger::LOG_LEVEL_INFO);
        $requestData = Protocol::encode(Protocol::HEART_BEAT_REQUEST, $params);
        $data = $connect->send($requestData);
        $ret = Protocol::decode(Protocol::HEART_BEAT_REQUEST, substr($data, 8));
        return $ret;
    }
}
