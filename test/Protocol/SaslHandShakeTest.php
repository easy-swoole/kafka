<?php
declare(strict_types=1);

namespace EasySwoole\test\Protocol;

use EasySwoole\Kafka\Protocol\SaslHandShake;
use PHPUnit\Framework\TestCase;
use function bin2hex;
use function hex2bin;
use function json_encode;

final class SaslHandShakeTest extends TestCase
{
    /**
     * @var SaslHandShake
     */
    private $sasl;

    public function setUp(): void
    {
        $this->sasl = new SaslHandShake('0.10.0.0');
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncode(): void
    {
        $test = $this->sasl->encode(['PLAIN']);

        self::assertSame('00000021001100000000001100104561737973776f6f6c652d6b61666b610005504c41494e', bin2hex($test));
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeNoMechanismGiven(): void
    {
        $this->sasl->encode();
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     * @throws \EasySwoole\Kafka\Exception\Protocol
     */
    public function testEncodeInvalidMechanism(): void
    {
        $this->sasl->encode(['NOTALLOW']);
    }

    /**
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function testDecode(): void
    {
        $data     = '0022000000010006475353415049';
        $expected = '{"mechanisms":["GSSAPI"],"errorCode":34}';
        self::assertJsonStringEqualsJsonString($expected, json_encode($this->sasl->decode(hex2bin($data))));
    }
}
