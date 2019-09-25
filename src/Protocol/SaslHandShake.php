<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/24
 * Time: 下午5:44
 */
namespace EasySwoole\Kafka\Protocol;

use EasySwoole\Kafka\Exception\NotSupported;
use EasySwoole\Kafka\Exception\Protocol as ProtocolException;
use function array_shift;
use function implode;
use function in_array;
use function is_string;
use function substr;

class SaslHandShake extends Protocol
{
    private const ALLOW_SASL_MECHANISMS = [
        'GSSAPI',
        'PLAIN',
        'SCRAM-SHA-256',
        'SCRAM-SHA-512',
    ];

    /**
     * @param array $payloads
     * @return string
     * @throws NotSupported
     * @throws ProtocolException
     */
    public function encode(array $payloads = []): string
    {
        $mechanism = array_shift($payloads);

        if (! is_string($mechanism)) {
            throw new ProtocolException('Invalid request SASL hand shake mechanism given. ');
        }

        if (! in_array($mechanism, self::ALLOW_SASL_MECHANISMS, true)) {
            throw new ProtocolException(
                'Invalid request SASL hand shake mechanism given, it must be one of: ' . implode('|', self::ALLOW_SASL_MECHANISMS)
            );
        }

        $header = $this->requestHeader('Easyswoole-kafka', self::SASL_HAND_SHAKE_REQUEST, self::SASL_HAND_SHAKE_REQUEST);
        $data   = self::encodeString($mechanism, self::PACK_INT16);
        $data   = self::encodeString($header . $data, self::PACK_INT32);

        return $data;
    }

    /**
     * @param string $data
     * @return array
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function decode(string $data): array
    {
        $offset            = 0;
        $errcode           = self::unpack(self::BIT_B16_SIGNED, substr($data, $offset, 2));
        $offset           += 2;
        $enabledMechanisms = $this->decodeArray(substr($data, $offset), [$this, 'mechanism']);
        $offset           += $enabledMechanisms['length'];

        return [
            'mechanisms' => $enabledMechanisms['data'],
            'errorCode'  => $errcode,
        ];
    }

    /**
     * @param string $data
     * @return array
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    protected function mechanism(string $data): array
    {
        $offset        = 0;
        $mechanismInfo = $this->decodeString(substr($data, $offset), self::BIT_B16);
        $offset       += $mechanismInfo['length'];

        return [
            'length' => $offset,
            'data'   => $mechanismInfo['data'],
        ];
    }
}
