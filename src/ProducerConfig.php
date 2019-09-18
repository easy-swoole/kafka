<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/17
 * Time: 下午11:06
 */
namespace EasySwoole\Kafka;

use EasySwoole\Component\Singleton;
use EasySwoole\Kafka\Protocol\Protocol;
use EasySwoole\Kafka\Protocol\Produce;
use EasySwoole\Kafka\Exception;

/**
 * Class ProducerConfig
 * @package EasySwoole\Kafka
 * @method int getRequestTimeout()
 * @method int getProduceInterval()
 * @method int getTimeout()
 * @method int getRequiredAck()
 * @method bool getIsAsyn()
 * @method int getCompression()
 */
class ProducerConfig extends Config
{

    use Singleton;

    private const COMPRESSION_OPTIONS = [
        Produce::COMPRESSION_NONE,
        Produce::COMPRESSION_GZIP,
        Produce::COMPRESSION_SNAPPY,
    ];

    protected static $defaults = [
        'requiredAck'    => 1,
        'timeout'       => 5000,
        'isAsyn'        => false,
        'requestTimeout'    => 6000,
        'produceInterval'   => 100,
        'compression'       => Protocol::COMPRESSION_NONE,
    ];

    /**
     * @param int $requestTimeout
     * @throws Exception\Config
     */
    public function setRequestTimeout(int $requestTimeout): void
    {
        if ($requestTimeout < 1 || $requestTimeout > 900000) {
            throw new Exception\Config("Set Request timeout value is invalid, must set it 1 .. 900000.");
        }

        static::$options['requestTimeout'] = $requestTimeout;
    }

    /**
     * @param int $produceInterval
     * @throws Exception\Config
     */
    public function setProduceInterval(int $produceInterval): void
    {
        if ($produceInterval < 1 || $produceInterval > 900000) {
            throw new Exception\Config("Set produce interval timeout value is invalid, must set it 1.. 900000.");
        }

        static::$options['produceInterval'] = $produceInterval;
    }

    /**
     * @param int $timeout
     * @throws Exception\Config
     */
    public function setTimeout(int $timeout): void
    {
        if ($timeout < 1 || $timeout > 900000) {
            throw new Exception\Config("Set timeout is invalid, mudt set it 1 .. 900000.");
        }

        static::$options['timeout'] = $timeout;
    }

    /**
     * @param int $requiredAck
     * @throws Exception\Config
     */
    public function setRequiredAck(int $requiredAck): void
    {
        if ($requiredAck < -1 || $requiredAck > 900000) {
            throw new Exception\Config("Set required ack value is invalid, must set it -1 .. 10000.");
        }

        static::$options['requiredAck'] = $requiredAck;
    }

    /**
     * @param bool $asyn
     */
    public function setIsAsyn(bool $asyn): void
    {
        static::$options['isAsyn'] = $asyn;
    }

    /**
     * @param int $compression
     * @throws Exception\Config
     */
    public function setCompression(int $compression): void
    {
        if (! in_array($compression, self::COMPRESSION_OPTIONS, true)) {
            throw new Exception\Config(
                "Compression must be one the EasySwoole\Kafka\Protocol\Produce::COMPRESSION_OPTIONS_* constants."
            );
        }

        static::$options['compression'] = $compression;
    }

}