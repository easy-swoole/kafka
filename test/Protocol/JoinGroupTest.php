<?php
declare(strict_types=1);

namespace EasySwoole\test\Protocol;

use EasySwoole\Kafka\Protocol\JoinGroup;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class JoinGroupTest extends TestCase
{
    /**
     * @var JoinGroup
     */
    private $group;

    public function setUp(): void
    {
        $this->group  = new JoinGroup('0.9.0.1');
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncode(): void
    {
        $data = [
            'group_id'        => 'test',
            'session_timeout' => 6000,
            'member_id'       => '',
            'data'            => [
                [
                    'protocol_name' => 'group',
                    'version'       => 0,
                    'subscription'  => ['test'],
                ],
            ],
        ];

        $expected  = '0000004f000b00000000000b00104561737973776f6f6c652d6b61666b610004746573740000177000000008636f6e73756d657200000001000567726f75700000001000000000000100047465737400000000';
        $test9      = $this->group->encode($data);

        self::assertSame($expected, bin2hex($test9));
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoGroupId(): void
    {
        $this->group->encode();
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoSessionTimeout(): void
    {
        $data = ['group_id' => 'test'];

        $this->group->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoMemberId(): void
    {
        $data = [
            'group_id'        => 'test',
            'session_timeout' => 6000,
        ];

        $test = $this->group->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoData(): void
    {
        $data = [
            'group_id'        => 'test',
            'session_timeout' => 6000,
            'member_id'       => '',
        ];

        $this->group->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeHasProtocolType(): void
    {
        $data = [
            'group_id'          => 'test',
            'session_timeout'   => 6000,
            'rebalance_timeout' => 6000,
            'member_id'         => '',
            'protocol_type'     => 'testtype',
            'data'              => [
                [
                    'protocol_name' => 'group',
                    'version'       => 0,
                    'subscription'  => ['test'],
                    'user_data'     => '',
                ],
            ],
        ];

        $expected  = '0000004f000b00000000000b00104561737973776f6f6c652d6b61666b610004746573740000177000000008746573747479706500000001000567726f75700000001000000000000100047465737400000000';
        $test      = $this->group->encode($data);

        self::assertSame($expected, bin2hex($test));
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoProtocolName(): void
    {
        $data = [
            'group_id'          => 'test',
            'session_timeout'   => 6000,
            'rebalance_timeout' => 6000,
            'member_id'         => '',
            'data'              => [
                [],
            ],
        ];

        $this->group->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoVersion(): void
    {
        $data = [
            'group_id'          => 'test',
            'session_timeout'   => 6000,
            'rebalance_timeout' => 6000,
            'member_id'         => '',
            'data'              => [
                ['protocol_name' => 'group'],
            ],
        ];

        $test = $this->group->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoSubscription(): void
    {
        $data = [
            'group_id'          => 'test',
            'session_timeout'   => 6000,
            'rebalance_timeout' => 6000,
            'member_id'         => '',
            'data'              => [
                [
                    'protocol_name' => 'group',
                    'version'       => 0,
                ],
            ],
        ];

        $this->group->encode($data);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function testDecode(): void
    {
        $data     = '000000000001000567726f7570002e6b61666b612d7068702d31313433353333332d663663342d346663622d616532642d396134393335613934663366002e6b61666b612d7068702d31313433353333332d663663342d346663622d616532642d39613439333561393466336600000001002e6b61666b612d7068702d31313433353333332d663663342d346663622d616532642d3961343933356139346633660000001000000000000100047465737400000000';
        $expected = '{"errorCode":0,"generationId":1,"groupProtocol":"group","leaderId":"kafka-php-11435333-f6c4-4fcb-ae2d-9a4935a94f3f","memberId":"kafka-php-11435333-f6c4-4fcb-ae2d-9a4935a94f3f","members":[{"memberId":"kafka-php-11435333-f6c4-4fcb-ae2d-9a4935a94f3f","memberMeta":{"version":0,"topics":["test"],"userData":""}}]}';

        $test = $this->group->decode(hex2bin($data));

        self::assertJsonStringEqualsJsonString($expected, json_encode($test));
    }
}
