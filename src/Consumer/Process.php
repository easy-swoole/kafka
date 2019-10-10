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

    /**
     * array(1) {
    ["127.0.0.1:9092"]=>
        array(3) {
        [0]=>
        int(-1)
        [1]=>
        int(-1)
        [2]=>
        int(-1)
        }
    }
     * @var array
     */
    private $partitions = [];

    public function __construct(?callable $consumer = null)
    {
        parent::__construct();

        $this->config = $this->getConfig();
        Protocol::init($this->config->getBrokerVersion());
        $this->getBroker()->setConfig($this->config);

        $this->syncMeta();
    }

    public function setAutoCommit(bool $isAuto)
    {
        $this->isAutoCommit = $isAuto;
    }

    /**
     * @param callable $func
     * @throws Exception\Config
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function subscribe(callable $func)
    {
        $this->joinGroup();

        $this->fetchOffset();


        // TODO: 如果没有指定分区，则随机选择有消息的分区
        $specifyPartition = $this->getConfig()->getSpecifyPartition();

        // TODO: 如果按key，则只选择key对应的消息
        $key = $this->getConfig()->getKey();

        while (1) {
            $messages = $this->fetchMsg();

            foreach ($this->brokerHost as $host) {
                if (empty($messages[$host])) {
                    continue;
                }

                $topic = $messages[$host]['topics'][0]['topicName']; // topicName

                $partitions = $messages[$host]['topics'][0]['partitions'];

                foreach ($partitions as $message) {
                    if (empty($message['messages'])) {
                        continue;
                    }

                    $partition = $message['partition'];
                    $offset = $message['messages'][0]['offset'];

                    // 指定消费分区
                    if ($specifyPartition >= 0 && $specifyPartition !== $partition) {
                        continue;
                    }

                    $ret = call_user_func($func, $message);
                    if ($this->isAutoCommit && $ret) {
                        $this->commit($host, $topic, $partition, $offset);
                        var_dump($this->partitions);
                    }
                }
            }

            break;
        }
    }

    /**
     * @param float $timeout
     * @return array
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function fetchMsg(float $timeout = 3.0)
    {
        $fetch = new Kafka\Fetch\Process();
        return $fetch->fetch($this->partitions);
    }

    /**
     * @param float $timeout
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function fetchOffset(float $timeout = 3.0)
    {
        $offsetProcess = new Kafka\Offset\Process();

        $offsets = $offsetProcess->fetchOffset();

        $partitions = [];
        foreach ($offsets as $host => $offset) {
            foreach ($offset['partitions'] as $partition) {
                $partitions[$host][$partition['partition']] = $partition['offset'];
            }
        }
        // 设置 分区及对应的偏移量
        $this->setPartition($partitions);
        var_dump($this->partitions);
    }

    public function getListOffset()
    {
        // 获取分区的offset列表
    }

    /**
     * @param $host
     * @param $topic
     * @param $partition
     * @param $offset
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function commit($host, $topic, $partition, $offset)
    {

        $ret = Kafka\Offset\Process::getInstance()->commit($host, $topic, $partition, $offset);

        $this->partitions[$host][$partition] = $offset + 1;
        var_dump($ret);
    }

    /**
     * @throws Exception\Config
     * @throws Exception\ConnectionException
     * @throws Exception\Exception
     */
    public function joinGroup()
    {
        $groupProcess = new Kafka\Group\Process();
        $ret = $groupProcess->joinGroup();
        $this->getConfig()->setMemberId($ret[0]['memberId']);
        $this->getConfig()->setGenerationId($ret[0]['generationId']);
    }

    public function setPartition($partitions)
    {
        if (is_string($partitions)) {
            $this->partitions[$partitions] = 0;
        } else {
            foreach ($partitions as $host => $value) {
                $this->partitions[$host] = [];
                foreach ($value as $partition => $offset) {
                    $offset = (int)$offset < 0 ? 0 : (int)$offset;
                    $this->partitions[$host][$partition] = $offset;
                }
            }
        }
    }

    protected function getConfig(): ConsumerConfig
    {
        return ConsumerConfig::getInstance();
    }

    private function getAssignment(): Assignment
    {
        return Assignment::getInstance();
    }
}
