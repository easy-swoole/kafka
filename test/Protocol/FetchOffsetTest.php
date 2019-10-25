<?php
declare(strict_types=1);

namespace EasySwoole\test\Protocol;

use EasySwoole\Kafka\Protocol\FetchOffset;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class FetchOffsetTest extends TestCase
{
    /**
     * @var FetchOffset
     */
    private $offset;

    public function setUp(): void
    {
        $this->offset = new FetchOffset('0.9.0.1');
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncode(): void
    {
        $data = [
            'group_id' => 'test',
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [0],
                ],
            ],
        ];

        $expected = '00000032000900010000000900104561737973776f6f6c652d6b61666b61000474657374000000010004746573740000000100000000';
        $test     = $this->offset->encode($data);

        self::assertSame($expected, bin2hex($test));
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoData(): void
    {
        $this->offset->encode();
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoGroupId(): void
    {
        $data = [
            'data' => [],
        ];

        $this->offset->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoTopicName(): void
    {
        $data = [
            'group_id' => 'test',
            'data' => [
                [],
            ],
        ];

        $this->offset->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoPartitions(): void
    {
        $data = [
            'group_id' => 'test',
            'data' => [
                ['topic_name' => 'test'],
            ],
        ];

        $this->offset->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function testDecode(): void
    {
        $data     = '000000010004746573740000000100000000ffffffffffffffff00000000';
        $expected = '[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"metadata":"","offset":-1}]}]';

        $test = $this->offset->decode(hex2bin($data));
        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
