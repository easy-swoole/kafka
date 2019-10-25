<?php
declare(strict_types=1);

namespace EasySwoole\test\Protocol;

use EasySwoole\Kafka\Protocol\Offset;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class OffsetTest extends TestCase
{
    /**
     * @var Offset
     */
    private $offset;

    public function setUp(): void
    {
        $this->offset   = new Offset('0.9.0.1');
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncode(): void
    {
        $data = [
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                            'partition_id' => 0,
                            'offset'       => 100,
                        ],
                    ],
                ],
            ],
        ];

        $expected = '0000003c000200000000000200104561737973776f6f6c652d6b61666b61ffffffff000000010004746573740000000100000000ffffffffffffffff000186a0';

        self::assertSame($expected, bin2hex($this->offset->encode($data)));
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
    public function testEncodeNoTopicName(): void
    {
        $data = [
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
            'data' => [
                ['topic_name' => 'test'],
            ],
        ];

        $this->offset->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoPartitionId(): void
    {
        $data = [
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [],
                    ],
                ],
            ],
        ];

        $this->offset->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function testDecode(): void
    {
        $data     = '000000010004746573740000000100000000000000000001000000000000002a';
        $expected = '[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"timestamp":0,"offsets":[42]}]}]';

        $test = $this->offset->decode(hex2bin($data));

        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
