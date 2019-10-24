<?php
declare(strict_types=1);

namespace EasySwoole\test\Protocol;

use EasySwoole\Kafka\Protocol\Produce;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class ProduceTest extends TestCase
{
    /**
     * @var Produce
     */
    private $produce;

    /**
     * @var Produce
     */
    private $produce10;

    public function setUp(): void
    {
        $this->produce   = new Produce('0.9.0.1');
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncode(): void
    {
        $data = [
            'required_ack' => 1,
            'timeout' => '1000',
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                            'partition_id' => 0,
                            'messages' => [
                                'test...',
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $expected = '00000057000000010000000000104561737973776f6f6c652d6b61666b610001000003e8000000010004746573740000000100000000000000210000000000000000000000153c1950a800000000000000000007746573742e2e2e';

        self::assertSame($expected, bin2hex($this->produce->encode($data)));
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeForMessageKey(): void
    {
        $data = [
            'required_ack' => 1,
            'timeout' => '1000',
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                            'partition_id' => 0,
                            'messages' => [
                                ['key' => 'testkey', 'value' => 'test...'],
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $expected = '0000005e000000010000000000104561737973776f6f6c652d6b61666b610001000003e80000000100047465737400000001000000000000002800000000000000000000001c4ad6c67a000000000007746573746b657900000007746573742e2e2e';

        self::assertSame($expected, bin2hex($this->produce->encode($data)));
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeForMessage(): void
    {
        $data = [
            'required_ack' => 1,
            'timeout' => '1000',
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                            'partition_id' => 0,
                            'messages' => 'test...',
                        ],
                    ],
                ],
            ],
        ];

        $expected = '00000057000000010000000000104561737973776f6f6c652d6b61666b610001000003e8000000010004746573740000000100000000000000210000000000000000000000153c1950a800000000000000000007746573742e2e2e';
        self::assertSame($expected, bin2hex($this->produce->encode($data)));
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNotTimeoutAndRequired(): void
    {
        $data = [
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        [
                            'partition_id' => 0,
                            'messages' => [
                                'test...',
                                'test...',
                                'test...',
                            ],
                        ],
                    ],
                ],
            ],
        ];

        $expected = '00000099000000010000000000104561737973776f6f6c652d6b61666b61000000000064000000010004746573740000000100000000000000630000000000000000000000153c1950a800000000000000000007746573742e2e2e0000000000000001000000153c1950a800000000000000000007746573742e2e2e0000000000000002000000153c1950a800000000000000000007746573742e2e2e';

        self::assertSame($expected, bin2hex($this->produce->encode($data)));
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoData(): void
    {
        $this->produce->encode();
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

        $this->produce->encode($data);
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

        $this->produce->encode($data);
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

        $this->produce->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoMessage(): void
    {
        $data = [
            'required_ack' => 1,
            'timeout' => '1000',
            'data' => [
                [
                    'topic_name' => 'test',
                    'partitions' => [
                        ['partition_id' => 0],
                    ],
                ],
            ],
        ];

        $this->produce->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function testDecode(): void
    {
        $data     = '0000000100047465737400000001000000000000000000000000002a00000000';
        $expected = '{"throttleTime":0,"data":[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"offset":14,"timestamp":0}]}]}';

        self::assertJsonStringEqualsJsonString($expected, json_encode($this->produce->decode(hex2bin($data))));
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function testDecodeKafka10(): void
    {
        $data     = '0000000100047465737400000001000000000000000000000000006effffffffffffffff00000000';
        $expected = '{"throttleTime":0,"data":[{"topicName":"test","partitions":[{"partition":0,"errorCode":0,"offset":14,"timestamp":0}]}]}';

        self::assertJsonStringEqualsJsonString($expected, json_encode($this->produce->decode(hex2bin($data))));
    }
}
