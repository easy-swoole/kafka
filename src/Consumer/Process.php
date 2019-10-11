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
     * @param callable $func
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function subscribe(callable $func)
    {
        $this->syncMeta();

        $this->joinGroup();

        $this->getListOffset();

        $this->fetchOffset();


        // TODO: 如果没有指定分区，则随机选择有消息的分区
        $specifyPartition = $this->getConfig()->getSpecifyPartition();

        // TODO: 如果按key，则只选择key对应的消息
        $key = $this->getConfig()->getKey();

        while (1) {
            $messages = $this->fetchMsg();
            $this->consumeMessage();

            var_dump($messages);

            foreach ($this->brokerHost as $host) {
                if (empty($messages[$host])) {
                    continue;
                }

                $topic = $messages[$host]['topics'][0]['topicName']; // topicName

                $partitions = $messages[$host]['topics'][0]['partitions'];

                foreach ($partitions as $part) {
                    if (empty($part['messages'])) {
                        continue;
                    }

                    if ($part['errorCode'] !== 0) {
                        continue;
                    }

                    $partition = $part['partition'];
                    $offset = $this->getConsumerOffset($topic, $partition);

                    if ($offset === null) {
                        return;
                    }

                    foreach ($part['messages'] as $message) {
                        $offset = $message['offset'];
                    }

                    $consumerOffset = ($part['highwaterMarkOffset'] > $offset) ? ($offset + 1) : $offset;
                    $this->setConsumerOffset($topic, $partition, $consumerOffset);
                    $this->setCommitOffset($topic, $partition, $offset);


                    // 指定消费分区
                    if ($specifyPartition >= 0 && $specifyPartition !== $partition) {
                        continue;
                    }

                    $ret = call_user_func($func, $part);
                    if ($this->isAutoCommit && $ret) {
                        $this->commit($host, $topic, $partition, $offset);
                    }
                }
            }

            break;
        }
    }

    /**
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function joinGroup()
    {
        $groupProcess = new Kafka\Group\Process();
        $ret = $groupProcess->joinGroup();
        $this->getAssignment()->setMemberId($ret[0]['memberId']);
        $this->getAssignment()->setGenerationId($ret[0]['generationId']);
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

        foreach ($results as $host => $topic) {
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
        foreach ($result as $host => $topic) {
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
        $results = Kafka\Fetch\Process::getInstance()->fetch($this->getConsumerOffsets());
        $this->logger->log('Fetch success, result:' . json_encode($results));

        foreach ($results as $host => $result) {
            foreach ($result['topics'] as $topic) {
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
        $results = Kafka\Offset\Process::getInstance()->commit($this->getCommitOffsets());
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
                $this->getConfig()->setMemberId('');
            }

            $this->getAssignment()->clearOffset();
            $this->subscribe();
            $this->state->rejoin();
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
