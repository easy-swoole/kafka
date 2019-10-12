<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/18
 * Time: 上午10:31
 */
namespace EasySwoole\Kafka\Consumer;

use EasySwoole\Kafka\BaseProcess;
use EasySwoole\Kafka\Broker;
use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Exception;
use EasySwoole\Kafka\Protocol;
use EasySwoole\Kafka;
use EasySwoole\Kafka\Protocol\Protocol as ProtocolTool;
use EasySwoole\Log\Logger;
use function count;
use function end;
use function explode;
use function in_array;
use function json_encode;
use function shuffle;
use function sprintf;
use function substr;
use function trim;

class Process extends BaseProcess
{
    /**
     * @var callable|null
     */
    protected $consumer;

    /**
     * @var string[][][]
     */
    protected $messages = [];

    protected $topics;

    protected $brokers;

    protected $isAutoCommit = true;

    public function __construct(?callable $consumer = null)
    {
        parent::__construct();

        $this->config = $this->getConfig();
        Protocol::init($this->config->getBrokerVersion());
        $this->getBroker()->setConfig($this->config);

        $this->consumer = $consumer;
    }

    public function setAutoCommit(bool $isAuto)
    {
        $this->isAutoCommit = $isAuto;
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function subscribe()
    {
        while (true) {
            $this->syncMeta();

            $this->getGroupBrokerId();

            $this->joinGroup();

            $this->syncGroup();

            $this->getListOffset();

            $this->fetchOffset();

            $this->fetchMsg();

            $this->commit();

            break;
        }
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function getGroupBrokerId()
    {
        $results = Kafka\Group\Process::getInstance()->getGroupBrokerId();

        if (! isset($results['errorCode'], $results['nodeId'])
            || $results['errorCode'] !== Protocol::NO_ERROR
        ) {
            // todo 错误处理
        }

        $this->getBroker()->setGroupBrokerId($results['nodeId']);
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function joinGroup()
    {
        $ret = Kafka\Group\Process::getInstance()->joinGroup();

        $this->getAssignment()->setMemberId($ret['memberId']);
        $this->getAssignment()->setGenerationId($ret['generationId']);
        $this->getAssignment()->assign($ret['members']);
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function syncGroup()
    {
        $result = Kafka\Group\Process::getInstance()->syncGroup();
        $this->logger->log(sprintf('Sync group sucess, params: %s', json_encode($result)));

        $topics = $this->getBroker()->getTopics();

        $brokerToTopics = [];

        foreach ($result['partitionAssignments'] as $topic) {
            foreach ($topic['partitions'] as $partId) {
                $brokerId = $topics[$topic['topicName']][$partId];

                $brokerToTopics[$brokerId] = $brokerToTopics[$brokerId] ?? [];

                $topicInfo = $brokerToTopics[$brokerId][$topic['topicName']] ?? [];

                $topicInfo['topic_name'] = $topic['topicName'];

                $topicInfo['partitions']   = $topicInfo['partitions'] ?? [];
                $topicInfo['partitions'][] = $partId;

                $brokerToTopics[$brokerId][$topic['topicName']] = $topicInfo;
            }
        }
        $assign = $this->getAssignment();
        $assign->setTopics($brokerToTopics);
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function getListOffset()
    {
        // 获取分区的offset列表
        $results        = Kafka\Offset\Process::getInstance()->listOffset();
        $offsets        = $this->getAssignment()->getOffsets();
        $lastOffsets    = $this->getAssignment()->getLastOffsets();

        foreach ($results as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] !== Protocol::NO_ERROR) {
                    // todo 错误处理
                }

                $offsets[$topic['topicName']][$part['partition']]       = end($part['offsets']);
                $lastOffsets[$topic['topicName']][$part['partition']]   = $part['offsets'][0];
            }
        }

        $this->getAssignment()->setOffsets($offsets);
        $this->getAssignment()->setLastOffsets($lastOffsets);
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function fetchOffset()
    {
        $result = Kafka\Offset\Process::getInstance()->fetchOffset();

        $offsets = $this->getAssignment()->getFetchOffsets();
        foreach ($result as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] !== Protocol::NO_ERROR) {
                    // todo 错误处理
                }

                $offsets[$topic['topicName']][$part['partition']] = $part['offset'];
            }
        }

        $this->getAssignment()->setFetchOffsets($offsets);

        $consumerOffsets = $this->getAssignment()->getConsumerOffsets();
        $lastOffsets = $this->getAssignment()->getLastOffsets();

        if (empty($consumerOffsets)) {
            $consumerOffsets = $this->getAssignment()->getFetchOffsets();
            foreach ($consumerOffsets as $topic => $value) {
                foreach ($value as $partId => $offset) {
                    if (isset($lastOffsets[$topic][$partId]) && $lastOffsets[$topic][$partId] > $offset) {
                        $consumerOffsets[$topic][$partId] = $offset + 1;
                    }
                }
            }

            $this->getAssignment()->setConsumerOffsets($consumerOffsets);
            $this->getAssignment()->setCommitOffsets($this->getAssignment()->getFetchOffsets());
        }
        var_dump($offsets);
        var_dump($lastOffsets);
        var_dump($consumerOffsets);
    }

    /**
     * @return array
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function fetchMsg()
    {
        $results = Kafka\Fetch\Process::getInstance()->fetch($this->getAssignment()->getConsumerOffsets());
        $this->logger->log('Fetch success, result:' . json_encode($results));

        foreach ($results['topics'] as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] !== 0) {
                    // todo 错误处理
                    continue;
                }

                $offset = $this->getAssignment()->getConsumerOffset($topic['topicName'], $part['partition']);

                if ($offset === null) {
                    return [];
                }

                foreach ($part['messages'] as $message) {
                    $this->messages[$topic['topicName']][$part['partition']][] = $message;
                    $offset = $message['offset'];// 当前消息的偏移量
                }

                $consumerOffset = ($part['highwaterMarkOffset'] > $offset) ? ($offset + 1) : $offset;
                $this->getAssignment()->setConsumerOffset($topic['topicName'], $part['partition'], $consumerOffset);
                $this->getAssignment()->setCommitOffset($topic['topicName'], $part['partition'], $offset);
            }
        }
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function commit()
    {
        // 先消费，再提交
        if ($this->getConfig()->getConsumeMode() === ConsumerConfig::CONSUME_BEFORE_COMMIT_OFFSET) {
            $this->consumeMessage();
        }
        $results = Kafka\Offset\Process::getInstance()->commit($this->getAssignment()->getCommitOffsets());
        $this->logger->log('Commit success, result:' . json_encode($results));

        // todo 错误处理

        // 先提交，再消费。默认此项
        if ($this->getConfig()->getConsumeMode() === ConsumerConfig::CONSUME_AFTER_COMMIT_OFFSET) {
            $this->consumeMessage();
        }
    }

    protected function stateConvert(int $errorCode, ?array $context = null): bool
    {
        $this->logger->log(Protocol::getError($errorCode), Logger::LOG_LEVEL_ERROR);

        $rejoinCodes = [
            Protocol::ILLEGAL_GENERATION,
            Protocol::INVALID_SESSION_TIMEOUT,
            Protocol::REBALANCE_IN_PROGRESS,
            Protocol::UNKNOWN_MEMBER_ID,
        ];

        if (in_array($errorCode, $rejoinCodes, true)) {
            if ($errorCode === Protocol::UNKNOWN_MEMBER_ID) {
                $this->getAssignment()->setMemberId('');
            }

            $this->getAssignment()->clearOffset();
//            $this->state->rejoin();
            return false;
        }

        return true;
    }

    /**
     * 消费消息
     */
    private function consumeMessage(): void
    {
        foreach ($this->messages as $topic => $value) {
            foreach ($value as $partition => $messages) {
                foreach ($messages as $message) {
                    if ($this->consumer !== null) {
                        ($this->consumer)($topic, $partition, $message);
                    }
                }
            }
        }

        $this->messages = [];
    }

    /**
     * @return ConsumerConfig
     */
    protected function getConfig(): ConsumerConfig
    {
        return ConsumerConfig::getInstance();
    }

    /**
     * @return Assignment
     */
    private function getAssignment(): Assignment
    {
        return Assignment::getInstance();
    }
}
