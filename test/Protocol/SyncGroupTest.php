<?php
declare(strict_types=1);

namespace EasySwoole\test\Protocol;

use EasySwoole\Kafka\Protocol\SyncGroup;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_decode;
use function json_encode;

final class SyncGroupTest extends TestCase
{
    /**
     * @var SyncGroup
     */
    private $sync;

    public function setUp(): void
    {
        $this->sync = new SyncGroup('0.9.0.1');
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncode(): void
    {
        $data = json_decode(
            '{"group_id":"test","generation_id":1,"member_id":"Easyswoole-kafka-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c","data":[{"version":0,"member_id":"Easyswoole-kafka-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c","assignments":[{"topic_name":"test","partitions":[0]}]}]}',
            true
        );

        $expected = '000000b2000e00000000000e00104561737973776f6f6c652d6b61666b610004746573740000000100354561737973776f6f6c652d6b61666b612d62643564356262322d326131662d343364342d623833312d6231353130643831616335630000000100354561737973776f6f6c652d6b61666b612d62643564356262322d326131662d343364342d623833312d62313531306438316163356300000018000000000001000474657374000000010000000000000000';

        self::assertSame($expected, bin2hex($this->sync->encode($data)));
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoGroupId(): void
    {
        $this->sync->encode();
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoGenerationId(): void
    {
        $data = ['group_id' => 'test'];

        $this->sync->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoMemberId(): void
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
        ];

        $this->sync->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoData(): void
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'Easyswoole-kafka-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
        ];

        $this->sync->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoVersion(): void
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'Easyswoole-kafka-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => [
                [],
            ],
        ];

        $this->sync->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoDataMemberId(): void
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'Easyswoole-kafka-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => [
                ['version' => 0],
            ],
        ];

        $this->sync->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoDataAssignments(): void
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'Easyswoole-kafka-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => [
                [
                    'version' => 0 ,
                    'member_id' => 'Easyswoole-kafka-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
                ],
            ],
        ];

        $this->sync->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoTopicName(): void
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'Easyswoole-kafka-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => [
                [
                    'version' => 0 ,
                    'member_id' => 'Easyswoole-kafka-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
                    'assignments' => [
                        [],
                    ],
                ],
            ],
        ];

        $this->sync->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoPartitions(): void
    {
        $data = [
            'group_id' => 'test',
            'generation_id' => '1',
            'member_id' => 'Easyswoole-kafka-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
            'data' => [
                [
                    'version' => 0 ,
                    'member_id' => 'Easyswoole-kafka-bd5d5bb2-2a1f-43d4-b831-b1510d81ac5c',
                    'assignments' => [
                        ['topic_name' => 'test'],
                    ],
                ],
            ],
        ];

        $this->sync->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function testDecode(): void
    {
        $data     = '000000000018000000000001000474657374000000010000000000000000';
        $expected = '{"errorCode":0,"partitionAssignments":[{"topicName":"test","partitions":[0]}],"version":0,"userData":""}';

        self::assertJsonStringEqualsJsonString($expected, json_encode($this->sync->decode(hex2bin($data))));
        self::assertJsonStringEqualsJsonString('{"errorCode":0}', json_encode($this->sync->decode(hex2bin('000000000000'))));
    }
}
