<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/19
 * Time: 上午9:04
 */
namespace EasySwoole\Kafka\Config;

use EasySwoole\Component\Singleton;
use EasySwoole\Kafka\Exception;

/**
 * @method array|false getTopics()
 * @method string getOffsetReset()
 * @method int getMaxBytes()
 * @method int getMaxWaitTime()
 * @method int getMinBytes()
 */
class FetchConfig extends Config
{
    use Singleton;

    protected static $defaults = [
        'topics'           => [],
        'offsetReset'      => 'latest', // earliest
        'maxBytes'         => 65536, // 64kb
        'maxWaitTime'      => 1000,
        'minBytes'         => 1000
    ];

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
     * @param int $maxWaitTime
     * @throws Exception\Config
     */
    public function setMaxWaitTime(int $maxWaitTime): void
    {
        if (empty($bytes)) {
            throw new Exception\Config('Set consumer maxWaitTime value is invalid, must set it not empty int');
        }

        static::$options['maxWaitTime'] = $maxWaitTime;
    }
}
