<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/18
 * Time: 上午10:24
 */
namespace EasySwoole\Kafka\Config;

use EasySwoole\Kafka\Exception;

/**
 * @method string|false ietGroupId()
 * @method int getSessionTimeout()
 * @method int getRebalanceTimeout()
 * @method string getOffsetReset()
 * @method int getMaxBytes()
 * @method int getMinBytes()
 * @method int getMaxWaitTime()
 * @method array getOffsets()
 * @method string getKey()
 * @method int getSpecifyPartition()
 * @method array getTopics()
 * @method bool getConsumeStatus()
 */
class ConsumerConfig extends Config
{

    public const CONSUME_AFTER_COMMIT_OFFSET  = 1;
    public const CONSUME_BEFORE_COMMIT_OFFSET = 2;

    /**
     * @var mixed[]
     */
    protected $runtimeOptions = [
        'consume_mode' => self::CONSUME_AFTER_COMMIT_OFFSET,
    ];

    /**
     * @var mixed[]
     */
    protected static $defaults = [
        'groupId'          => '',
        'sessionTimeout'   => 30000,
        'rebalanceTimeout' => 30000,
        'offsetReset'      => 'latest', // earliest
        'maxBytes'         => 65536, // 64kb
        'minBytes'         => 0,
        'maxWaitTime'      => 100,
        'offsets'          => [],// offset by peer partitions on the brokers
        'key'              => '',
        'specifyPartition' => -1,
        'topics'           => [],
        'consumeStatus'    => true,//todo
    ];

    /**
     * @return string
     * @throws Exception\Config
     */
    public function getGroupId(): string
    {
        $groupId = trim($this->ietGroupId());

        if ($groupId === false || $groupId === '') {
            throw new Exception\Config('Get group id value is invalid, must set it not empty string');
        }

        return $groupId;
    }

    /**
     * @param string $groupId
     * @throws Exception\Config
     */
    public function setGroupId(string $groupId): void
    {
        $groupId = trim($groupId);

        if ($groupId === false || $groupId === '') {
            throw new Exception\Config('Set group id value is invalid, must set it not empty string');
        }

        static::$options['groupId'] = $groupId;
    }

    /**
     * @param int $sessionTimeout
     * @throws Exception\Config
     */
    public function setSessionTimeout(int $sessionTimeout): void
    {
        if ($sessionTimeout < 1 || $sessionTimeout > 3600000) {
            throw new Exception\Config('Set session timeout value is invalid, must set it 1 .. 3600000');
        }

        static::$options['sessionTimeout'] = $sessionTimeout;
    }

    /**
     * @param int $rebalanceTimeout
     * @throws Exception\Config
     */
    public function setRebalanceTimeout(int $rebalanceTimeout): void
    {
        if ($rebalanceTimeout < 1 || $rebalanceTimeout > 3600000) {
            throw new Exception\Config('Set rebalance timeout value is invalid, must set it 1 .. 3600000');
        }

        static::$options['rebalanceTimeout'] = $rebalanceTimeout;
    }

    /**
     * @param string $offsetReset
     * @throws Exception\Config
     */
    public function setOffsetReset(string $offsetReset): void
    {
        if (! in_array($offsetReset, ['latest', 'earliest'], true)) {
            throw new Exception\Config('Set offset reset value is invalid, must set it `latest` or `earliest`');
        }

        static::$options['offsetReset'] = $offsetReset;
    }

    /**
     * @param int $bytes
     * @throws Exception\Config
     */
    public function setMaxBytes(int $bytes): void
    {
        if (empty($bytes)) {
            throw new Exception\Config('Set consumer maxBytes value is invalid, must set it not empty int');
        }

        static::$options['maxBytes'] = $bytes;
    }

    /**
     * @param int $bytes
     * @throws Exception\Config
     */
    public function setMinBytes(int $bytes): void
    {
        if (empty($bytes)) {
            throw new Exception\Config('Set consumer minBytes value is invalid, must set it not empty int');
        }

        static::$options['minBytes'] = $bytes;
    }

    /**
     * @param int $mode
     * @throws Exception\Config
     */
    public function setConsumeMode(int $mode): void
    {
        if (! in_array($mode, [self::CONSUME_AFTER_COMMIT_OFFSET, self::CONSUME_BEFORE_COMMIT_OFFSET], true)) {
            throw new Exception\Config(
                'Invalid consume mode given, it must be either "ConsumerConfig::CONSUME_AFTER_COMMIT_OFFSET" or '
                . '"ConsumerConfig::CONSUME_BEFORE_COMMIT_OFFSET"'
            );
        }

        $this->runtimeOptions['consume_mode'] = $mode;
    }

    /**
     * @return int
     */
    public function getConsumeMode(): int
    {
        return $this->runtimeOptions['consume_mode'];
    }

    /**
     * @param array $offsets
     * @throws Exception\Config
     */
    public function setOffsets(array $offsets): void
    {
        if (empty($offsets)) {
            throw new Exception\Config('Set consumer offsets value is invalid, must set it not empty array');
        }

        static::$options['offsets'] = $offsets;
    }

    /**
     * @param string $key
     * @throws Exception\Config
     */
    public function setKey(string $key): void
    {
        if (empty($key)) {
            throw new Exception\Config('Set consumer key value is invalid, must set it not empty string');
        }

        static::$options['key'] = $key;
    }

    /**
     * @param int $specifyPartition
     * @throws Exception\Config
     */
    public function setSpecifyPartition(int $specifyPartition): void
    {
        if (empty($specifyPartition)) {
            throw new Exception\Config('Set consumer $specifyPartition value is invalid, must set it 0 ... max partition');
        }

        static::$options['specifyPartition'] = $specifyPartition;
    }

    /**
     * @param array $topics
     * @throws Exception\Config
     */
    public function setTopics(array $topics): void
    {
        if (empty($topics)) {
            throw new Exception\Config('Set consumer topics value is invalid, must set it not empty array');
        }

        static::$options['topics'] = $topics;
    }

    /**
     * @param bool $consumeStatus
     * @throws Exception\Config
     */
    public function setConsumeStatus(bool $consumeStatus)
    {
        if (! in_array($consumeStatus, [true, false], true)) {
            throw new Exception\Config('Set consumer $specifyPartition value is invalid, must set bool');
        }

        static::$options['consumeStatus'] = $consumeStatus;
    }
}
