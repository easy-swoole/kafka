<?php
declare(strict_types=1);

namespace EasySwoole\test\Protocol;

use EasySwoole\Kafka\Protocol\LeaveGroup;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class LeaveGroupTest extends TestCase
{
    /**
     * @var LeaveGroup
     */
    private $leave;

    public function setUp(): void
    {
        $this->leave = new LeaveGroup('0.9.0.1');
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncode(): void
    {
        $data = [
            'group_id'  => 'test',
            'member_id' => 'Easyswoole-kafka-eb19c0ea-4b3e-4ed0-bada-c873951c8eea',
        ];

        $expected = '00000057000d00000000000d00104561737973776f6f6c652d6b61666b6100047465737400354561737973776f6f6c652d6b61666b612d65623139633065612d346233652d346564302d626164612d633837333935316338656561';
        $test     = $this->leave->encode($data);

        self::assertSame($expected, bin2hex($test));
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoGroupId(): void
    {
        $this->leave->encode();
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoMemberId(): void
    {
        $data = ['group_id' => 'test'];

        $this->leave->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function testDecode(): void
    {
        $test     = $this->leave->decode(hex2bin('0000'));
        $expected = '{"errorCode":0}';

        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
