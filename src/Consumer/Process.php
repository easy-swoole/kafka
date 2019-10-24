<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/18
 * Time: 上午10:31
 */
namespace EasySwoole\Kafka\Consumer;

use EasySwoole\Kafka\BaseProcess;
use EasySwoole\Kafka\Config\ConsumerConfig;
use EasySwoole\Kafka\Exception;
use EasySwoole\Kafka\Protocol;
use EasySwoole\Kafka;
use SebastianBergmann\CodeCoverage\Report\PHP;

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

    public function __construct(?callable $consumer = null)
    {
        parent::__construct();

        $this->config = $this->getConfig();
        Protocol::init($this->config->getBrokerVersion());
        $this->getBroker()->setConfig($this->config);

        $this->consumer = $consumer;
    }

    /**
     * @throws \Throwable
     */
    public function subscribe()
    {
        while (true) {
            try {
                if ($this->getAssignment()->isJoinFuture()) {
                    $this->syncMeta();

                    $this->getGroup();

                    $this->initiateJoinGroup();
                }

                $this->heartbeat();

                $this->getListOffset();

                $this->fetchOffset();

                $this->fetchMsg();

                $this->commit();

                \Co::sleep($this->getConfig()->getRefreshIntervalMs() / 1000);

            } catch (Exception\ErrorCodeException $codeException) {
                $this->getAssignment()->setJoinFuture(true);
//                echo '----------------group 成员 或者 partition数量变更 需要重新入组与分配partition'.PHP_EOL;
            } catch (\Throwable $throwable) {
                throw  $throwable;
            }
        }
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function syncMeta()
    {
        Kafka\SyncMeta\Process::getInstance()->syncMeta();
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\ErrorCodeException
     * @throws Exception\Exception
     */
    public function getGroup()
    {
        $results = Kafka\Group\Process::getInstance()->getGroupBrokerId();
        if (! isset($results['errorCode'], $results['nodeId'])
            || $results['errorCode'] !== Protocol::NO_ERROR
        ) {
            $this->stateConvert($results['errorCode']);
        }
        $this->getBroker()->setGroupBrokerId($results['nodeId']);
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\ErrorCodeException
     * @throws Exception\Exception
     */
    public function initiateJoinGroup()
    {
        if ($this->getAssignment()->isJoinFuture()) {
            $isLeader = $this->joinGroup();
            $this->syncGroup($isLeader);
        }
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\ErrorCodeException
     * @throws Exception\Exception
     * @return bool
     */
    protected function joinGroup() : bool
    {
        $result = Kafka\Group\Process::getInstance()->joinGroup();

        if (isset($result['errorCode']) && $result['errorCode'] !== Protocol::NO_ERROR) {
            $this->stateConvert($result['errorCode']);
        }
        $this->getAssignment()->setMemberId($result['memberId']);
        $this->getAssignment()->setGenerationId($result['generationId']);
        $this->getAssignment()->setLeaderId($result['leaderId']);
        $this->getAssignment()->assign($result['members']);

        return $result['leaderId'] == $result['memberId'] ? true : false;
    }

    /**
     * @param  bool $isLeader
     * @throws Exception\ConnectionException
     * @throws Exception\ErrorCodeException
     * @throws Exception\Exception
     */
    protected function syncGroup(bool $isLeader)
    {
        if ($isLeader) {
            $result = Kafka\Group\Process::getInstance()->syncGroupOnJoinLeader();
        } else {
            $result = Kafka\Group\Process::getInstance()->syncGroupOnJoinFollower();
        }

        if (isset($result['errorCode']) && $result['errorCode'] !== Protocol::NO_ERROR) {
            $this->stateConvert($result['errorCode']);
        }
        $topics = $this->getBroker()->getTopics();
        $brokerToTopics = [];
        foreach ($result['partitionAssignments'] as $topic) {
            foreach ($topic['partitions'] as $partId) {
                if (isset($topics[$topic['topicName']][$partId])) {
                    $brokerId = $topics[$topic['topicName']][$partId];
                    $brokerToTopics[$brokerId] = $brokerToTopics[$brokerId] ?? [];
                    $topicInfo = $brokerToTopics[$brokerId][$topic['topicName']] ?? [];
                    $topicInfo['topic_name'] = $topic['topicName'];
                    $topicInfo['partitions'] = $topicInfo['partitions'] ?? [];
                    $topicInfo['partitions'][] = $partId;
                    $brokerToTopics[$brokerId][$topic['topicName']] = $topicInfo;
                }
            }
        }
        $assign = $this->getAssignment();
        if (!empty($brokerToTopics)) {
            $assign->setTopics($brokerToTopics);
            $assign->setJoinFuture(false);
        }
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\ErrorCodeException
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
                    $this->stateConvert($part['errorCode']);
                    continue;
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
     * @throws Exception\ErrorCodeException
     * @throws Exception\Exception
     */
    public function heartbeat()
    {
        $result = Kafka\Heartbeat\Process::getInstance()->heartbeat();
        if (isset($result['errorCode']) && $result['errorCode'] !== Protocol::NO_ERROR) {
            $this->stateConvert($result['errorCode']);
        }
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\ErrorCodeException
     * @throws Exception\Exception
     */
    public function fetchOffset()
    {
        $result = Kafka\Offset\Process::getInstance()->fetchOffset();
        $offsets = $this->getAssignment()->getFetchOffsets();
        foreach ($result as $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] !== Protocol::NO_ERROR) {
                    $this->stateConvert($part['errorCode']);
                    continue;
                }

                $offsets[$topic['topicName']][$part['partition']] = $part['offset'];
            }
        }
        $this->getAssignment()->setFetchOffsets($offsets);

        $consumerOffsets    = $this->getAssignment()->getConsumerOffsets();
        $lastOffsets        = $this->getAssignment()->getLastOffsets();

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
            //$this->getAssignment()->setCommitOffsets($this->getAssignment()->getFetchOffsets());
        }
    }

    /**
     * @return array
     * @throws Exception\ConnectionException
     * @throws Exception\ErrorCodeException
     * @throws Exception\Exception
     */
    public function fetchMsg()
    {
        $results = Kafka\Fetch\Process::getInstance()->fetch($this->getAssignment()->getConsumerOffsets());
        if (!isset($results['topics'])) {
            return [];
        }
        foreach ($results['topics'] as $k => $topic) {
            foreach ($topic['partitions'] as $part) {
                if ($part['errorCode'] !== 0) {
                    $this->stateConvert($part['errorCode'], [
                        $topic['topicName'],
                        $part['partition']
                    ]);
                    continue;
                }
                $offset = $this->getAssignment()->getConsumerOffset($topic['topicName'], $part['partition']);
                if ($offset === null) {
                    return [];
                }

                foreach ($part['messages'] as $message) {
                    if (!empty($message)) {
                        $this->messages[$topic['topicName']][$part['partition']][] = $message;
                        $offset = $message['offset'];// 当前消息的偏移量
//                        echo '-----------------订阅到新的消息需要处理-----topic:'.$topic['topicName'].'-------partition:'.$part['partition'].'------offset:'.$offset.'--------------'.PHP_EOL;
                        $this->getAssignment()->setCommitOffset($topic['topicName'], $part['partition'], $offset);
                    }
                }
                $consumerOffset = ($part['highwaterMarkOffset'] > $offset) ? ($offset + 1) : $offset;
                $this->getAssignment()->setConsumerOffset($topic['topicName'], $part['partition'], $consumerOffset);
            }
        }
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\ErrorCodeException
     * @throws Exception\Exception
     */
    public function commit()
    {
        // 先消费，再提交
        if ($this->getConfig()->getConsumeMode() === ConsumerConfig::CONSUME_BEFORE_COMMIT_OFFSET) {
            $this->consumeMessage();
        }

        $commitOffset = $this->getAssignment()->getCommitOffsets();
        if (!empty($commitOffset)) {
//            echo '--------------有消费需要提交'.PHP_EOL;
            $results = Kafka\Offset\Process::getInstance()->commit($commitOffset);
            foreach ($results as $topic) {
                foreach ($topic['partitions'] as $part) {
                    if ($part['errorCode'] !== Protocol::NO_ERROR) {
                        $this->stateConvert($part['errorCode']);
                    }
                }
            }
            $this->getAssignment()->setCommitOffsets([]);
        }
        // 先提交，再消费。默认此项
        if ($this->getConfig()->getConsumeMode() === ConsumerConfig::CONSUME_AFTER_COMMIT_OFFSET) {
            $this->consumeMessage();
        }
    }

    /**
     * @param int        $errorCode
     * @param array|null $context
     * @throws Exception\ErrorCodeException
     */
    protected function stateConvert(int $errorCode, ?array $context = null)
    {
        $recoverCodes = [
            Protocol::UNKNOWN_TOPIC_OR_PARTITION,
            Protocol::NOT_LEADER_FOR_PARTITION,
            Protocol::BROKER_NOT_AVAILABLE,
            Protocol::GROUP_LOAD_IN_PROGRESS,
            Protocol::GROUP_COORDINATOR_NOT_AVAILABLE,
            Protocol::NOT_COORDINATOR_FOR_GROUP,
            Protocol::INVALID_TOPIC,
            Protocol::INCONSISTENT_GROUP_PROTOCOL,
            Protocol::INVALID_GROUP_ID,
        ];

        $rejoinCodes = [
            Protocol::ILLEGAL_GENERATION,
            Protocol::INVALID_SESSION_TIMEOUT,
            Protocol::REBALANCE_IN_PROGRESS,
            Protocol::UNKNOWN_MEMBER_ID,
        ];

        if (in_array($errorCode, $recoverCodes, true)) {
            $this->getAssignment()->clearOffset();
        }

        if (in_array($errorCode, $rejoinCodes, true)) {
            if ($errorCode === Protocol::UNKNOWN_MEMBER_ID) {
                $this->getAssignment()->setMemberId('');
            }
            $this->getAssignment()->clearOffset();
        }
        if ($errorCode === Protocol::OFFSET_OUT_OF_RANGE) {
            $resetOffset      = $this->getConfig()->getOffsetReset();
            $offsets          = $resetOffset === 'latest' ?
                $this->getAssignment()->getLastOffsets() : $this->getAssignment()->getOffsets();

            [$topic, $partId] = $context;

            if (isset($offsets[$topic][$partId])) {
                $this->getAssignment()->setConsumerOffset($topic, (int) $partId, $offsets[$topic][$partId]);
            }
        }
//        echo '--------------'.$errorCode.'--------'.Protocol::getError($errorCode).PHP_EOL;
        throw new Exception\ErrorCodeException(Protocol::getError($errorCode));
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
     * @param \Throwable $throwable
     * @param mixed      ...$args
     * @throws \Throwable
     */
    protected function onException(\Throwable $throwable, ...$args)
    {
        throw $throwable;
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
