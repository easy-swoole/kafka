<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/18
 * Time: 下午1:28
 */
namespace EasySwoole\Kafka\Consumer;

use EasySwoole\Component\Singleton;
use EasySwoole\Kafka\Broker;
use function count;

class Assignment
{
    private $joinFuture = true;

    /**
     * @var string
     */
    private $memberId = '';

    /**
     * @var string
     */
    private $leaderId = '';

    /**
     * @var int|null
     */
    private $generationId;

    /**
     * @var int[][][][]
     */
    private $assignments = [];

    /**
     * @var mixed[][]
     */
    private $topics = [];

    /**
     * 初始偏移量
     * @var int[][]
     */
    private $offsets = [];

    /**
     * 分区中可支持的最大的偏移量
     * @var int[][]
     */
    private $lastOffsets = [];

    /**
     * fetchOffset返回的对应偏移量
     * @var int[][]
     */
    private $fetchOffsets = [];

    /**
     * 当前消费偏移量
     * @var int[][]
     */
    private $consumerOffsets = [];

    /**
     * 当前提交偏移量
     * @var int[][]
     */
    private $commitOffsets = [];

    /**
     * @var int[][]
     */
    private $preCommitOffsets = [];

    /**
     * @return bool
     */
    public function isJoinFuture(): bool
    {
        return $this->joinFuture;
    }

    /**
     * @param bool $joinFuture
     */
    public function setJoinFuture(bool $joinFuture): void
    {
        $this->joinFuture = $joinFuture;
    }


    public function setMemberId(string $memberId): void
    {
        $this->memberId = $memberId;
    }

    public function getMemberId(): string
    {
        return $this->memberId;
    }

    /**
     * @return string
     */
    public function getLeaderId(): string
    {
        return $this->leaderId;
    }

    /**
     * @param string $leaderId
     */
    public function setLeaderId(string $leaderId): void
    {
        $this->leaderId = $leaderId;
    }

    public function setGenerationId(int $generationId): void
    {
        $this->generationId = $generationId;
    }

    public function getGenerationId(): ?int
    {
        return $this->generationId;
    }

    /**
     * @return int[][][][]
     */
    public function getAssignments(): array
    {
        return $this->assignments;
    }

    /**
     * @param array $assignments
     */
    public function setAssignments(array $assignments): void
    {
        $this->assignments = $assignments;
    }

    /**
     * @param array  $result
     * @param Broker $broker
     */
    public function assign(array $result, Broker $broker): void
    {
        $allTopics = $broker->getTopics();
        $subscribeTopics = $broker->getConfig()->getTopics();
        $memberCount = count($result);
        if ($this->memberId == $this->leaderId) {
            $retTopics = [];
            foreach ($allTopics as $topicName => $topics) {
                foreach ($subscribeTopics as $topic) {
                    if ($topicName == $topic) {
                        $retTopics[$topicName] = $topics;
                    }
                }
            }

            $members = [];
            foreach ($result as $mk => $member) {
                foreach ($retTopics as $topicName => $topic) {
                    $m = ceil(count($topic) / $memberCount);
                    $startIndex = $m * $mk;
                    $endIndex = ($mk + 1) * $m - 1;
                    if (! isset($members[$mk])) {
                        $members[$mk] = [];
                    }
                    if (! isset($members[$mk][$topicName])) {
                        $members[$mk][$topicName] = [];
                    }
                    $members[$mk][$topicName]['topic_name'] = $topicName;
                    if (! isset($members[$mk][$topicName]['partitions'])) {
                        $members[$mk][$topicName]['partitions'] = [];
                    }

                    for ($i = $startIndex; $i <= $endIndex; $i++) {
                        $members[$mk][$topicName]['partitions'][] = $i;
                    }

                }
            }
            $data = [];
            foreach ($result as $key => $member) {
                $data[] = [
                    'version'     => 0,
                    'member_id'   => $member['memberId'],
                    'assignments' => $members[$key] ?? [],
                ];
            }
            $this->setAssignments($data);
        } else {
            $data = [];
            $this->setAssignments($data);
        }
    }

    /**
     * @param mixed[][] $topics
     */
    public function setTopics(array $topics): void
    {
        $this->topics = $topics;
    }

    /**
     * @return mixed[][]
     */
    public function getTopics(): array
    {
        return $this->topics;
    }

    /**
     * @param int[][] $offsets
     */
    public function setOffsets(array $offsets): void
    {
        $this->offsets = $offsets;
    }

    /**
     * @return int[][]
     */
    public function getOffsets(): array
    {
        return $this->offsets;
    }

    /**
     * @param int[][] $offsets
     */
    public function setLastOffsets(array $offsets): void
    {
        $this->lastOffsets = $offsets;
    }

    /**
     * @return int[][]
     */
    public function getLastOffsets(): array
    {
        return $this->lastOffsets;
    }

    /**
     * @param int[][] $offsets
     */
    public function setFetchOffsets(array $offsets): void
    {
        $this->fetchOffsets = $offsets;
    }

    /**
     * @return int[][]
     */
    public function getFetchOffsets(): array
    {
        return $this->fetchOffsets;
    }

    /**
     * @param int[][] $offsets
     */
    public function setConsumerOffsets(array $offsets): void
    {
        $this->consumerOffsets = $offsets;
    }

    /**
     * @return int[][]
     */
    public function getConsumerOffsets(): array
    {
        return $this->consumerOffsets;
    }

    public function setConsumerOffset(string $topic, int $partition, int $offset): void
    {
        $this->consumerOffsets[$topic][$partition] = $offset;
    }

    public function getConsumerOffset(string $topic, int $partition): ?int
    {
        if (! isset($this->consumerOffsets[$topic][$partition])) {
            return null;
        }

        return $this->consumerOffsets[$topic][$partition];
    }

    /**
     * @param int[][] $offsets
     */
    public function setCommitOffsets(array $offsets): void
    {
        $this->commitOffsets = $offsets;
    }

    /**
     * @return int[][]
     */
    public function getCommitOffsets(): array
    {
        return $this->commitOffsets;
    }

    public function setCommitOffset(string $topic, int $partition, int $offset): void
    {
        $this->commitOffsets[$topic][$partition] = $offset;
    }

    /**
     * @param int[][] $offsets
     */
    public function setPreCommitOffsets(array $offsets): void
    {
        $this->preCommitOffsets = $offsets;
    }

    /**
     * @return int[][]
     */
    public function getPreCommitOffsets(): array
    {
        return $this->preCommitOffsets;
    }

    public function setPreCommitOffset(string $topic, int $partition, int $offset): void
    {
        $this->preCommitOffsets[$topic][$partition] = $offset;
    }

    public function clearOffset(): void
    {
        $this->offsets          = [];
        $this->lastOffsets      = [];
        $this->fetchOffsets     = [];
        $this->consumerOffsets  = [];
        $this->commitOffsets    = [];
        $this->preCommitOffsets = [];
    }
}
