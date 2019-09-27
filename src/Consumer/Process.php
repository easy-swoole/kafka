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

    /**
     * @var State
     */
    private $state;

    public function __construct(?callable $consumer = null)
    {
        parent::__construct();

        $this->config = $this->getConfig();
        Protocol::init($this->config->getBrokerVersion());
        $this->getBroker()->setConfig($this->config);
    }

    public function subscribe(callable $func, $timeout = 3.0)
    {
        $this->init();
        $this->state->start();
    }

    private function init2()
    {
        $this->syncMeta();

        $this->brokers = $this->getBroker()->getBrokers();

        $topics = $this->getBroker()->getTopics();
        foreach ($topics as $topic => $partitions) {
            foreach ($this->config->getTopics() as $topicName) {
                if ($topic !== $topicName) {
                    continue;
                }
                $this->topics = [$topic => $partitions];
            }
        }
        var_dump($this->topics);
        var_dump($this->brokers);
    }

    public function init(): void
    {
        $config = $this->getConfig();

        $broker = $this->getBroker();
        $broker->setConfig($config);
        $broker->setProcess(function (string $data, int $fd): void {
            $this->processRequest($data, $fd);
        });

        $this->state = State::getInstance();

        $this->state->setCallback(
            [
                State::REQUEST_METADATA      => function (): void {
                    $this->syncMeta();
                },
                State::REQUEST_GETGROUP      => function (): void {
                    $groupProcess = new \EasySwoole\Kafka\Group\Process();
                    $groupProcess->getGroupBrokerId();
                },
                State::REQUEST_JOINGROUP     => function (): void {
                    $groupProcess = new \EasySwoole\Kafka\Group\Process();
                    $groupProcess->joinGroup();
                },
                State::REQUEST_SYNCGROUP     => function (): void {
                    $groupProcess = new \EasySwoole\Kafka\Group\Process();
                    $groupProcess->syncGroup();
                },
                State::REQUEST_HEARTGROUP    => function (): void {
                    $hearBeatProcess = new \EasySwoole\Kafka\Heartbeat\Process();
                    $hearBeatProcess->heartbeat();
                },
                State::REQUEST_OFFSET        => function (): array {
                    $offsetProcess = new \EasySwoole\Kafka\Offset\Process();
                    return $offsetProcess->listOffset();
                },
                State::REQUEST_FETCH_OFFSET  => function (): void {
                    $offsetProcess = new \EasySwoole\Kafka\Offset\Process();
                    $offsetProcess->fetchOffset();
                },
                State::REQUEST_FETCH         => function (): array {
                    $fetchProcess = new \EasySwoole\Kafka\Fetch\Process();
                    return $fetchProcess->fetch();
                },
                State::REQUEST_COMMIT_OFFSET => function (): void {
                    $offsetProcess = new \EasySwoole\Kafka\Offset\Process();
                    $offsetProcess->commit();
                },
            ]
        );
        $this->state->init();
    }

    /**
     * @param string $data
     * @param int    $fd
     * @throws Exception\Exception
     */
    protected function processRequest(string $data, int $fd): void
    {
        $correlationId = ProtocolTool::unpack(ProtocolTool::BIT_B32, substr($data, 0, 4));

        switch ($correlationId) {
            case Protocol::METADATA_REQUEST:
                $result = Protocol::decode(Protocol::METADATA_REQUEST, substr($data, 4));

                if (! isset($result['brokers'], $result['topics'])) {
                    $this->error('Get metadata is fail, brokers or topics is null.');
                    $this->state->failRun(State::REQUEST_METADATA);
                    break;
                }

                /** @var Broker $broker */
                $broker   = $this->getBroker();
                $isChange = $broker->setData($result['topics'], $result['brokers']);
                $this->state->succRun(State::REQUEST_METADATA, $isChange);

                break;
            case Protocol::GROUP_COORDINATOR_REQUEST:
                $result = Protocol::decode(Protocol::GROUP_COORDINATOR_REQUEST, substr($data, 4));

                if (! isset($result['errorCode'], $result['coordinatorId']) || $result['errorCode'] !== Protocol::NO_ERROR) {
                    $this->state->failRun(State::REQUEST_GETGROUP);
                    break;
                }

                /** @var Broker $broker */
                $broker = $this->getBroker();
                $broker->setGroupBrokerId($result['coordinatorId']);

                $this->state->succRun(State::REQUEST_GETGROUP);

                break;
            case Protocol::JOIN_GROUP_REQUEST:
                $result = Protocol::decode(Protocol::JOIN_GROUP_REQUEST, substr($data, 4));
                if (isset($result['errorCode']) && $result['errorCode'] === Protocol::NO_ERROR) {
                    $this->succJoinGroup($result);
                    break;
                }

                $this->failJoinGroup($result['errorCode']);
                break;
            case Protocol::SYNC_GROUP_REQUEST:
                $result = Protocol::decode(Protocol::SYNC_GROUP_REQUEST, substr($data, 4));
                if (isset($result['errorCode']) && $result['errorCode'] === Protocol::NO_ERROR) {
                    $this->succSyncGroup($result);
                    break;
                }

                $this->failSyncGroup($result['errorCode']);
                break;
            case Protocol::HEART_BEAT_REQUEST:
                $result = Protocol::decode(Protocol::HEART_BEAT_REQUEST, substr($data, 4));
                if (isset($result['errorCode']) && $result['errorCode'] === Protocol::NO_ERROR) {
                    $this->state->succRun(State::REQUEST_HEARTGROUP);
                    break;
                }

                $this->failHeartbeat($result['errorCode']);
                break;
            case Protocol::OFFSET_REQUEST:
                $result = Protocol::decode(Protocol::OFFSET_REQUEST, substr($data, 4));
                $this->succOffset($result, $fd);
                break;
            case ProtocolTool::OFFSET_FETCH_REQUEST:
                $result = Protocol::decode(Protocol::OFFSET_FETCH_REQUEST, substr($data, 4));
                $this->succFetchOffset($result);
                break;
            case ProtocolTool::FETCH_REQUEST:
                $result = Protocol::decode(Protocol::FETCH_REQUEST, substr($data, 4));
                $this->succFetch($result, $fd);
                break;
            case ProtocolTool::OFFSET_COMMIT_REQUEST:
                $result = Protocol::decode(Protocol::OFFSET_COMMIT_REQUEST, substr($data, 4));
                $this->succCommit($result);
                break;
            default:
                $this->error('Error request, correlationId:' . $correlationId);
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
