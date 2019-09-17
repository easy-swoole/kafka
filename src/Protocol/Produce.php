<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/17
 * Time: 下午11:24
 */
namespace EasySwoole\Kafka\Protocol;

use EasySwoole\Kafka\Exception\Protocol as ProtocolException;

class Produce extends Protocol
{
    /**
     * @param array $payloads
     * @return string
     * @throws ProtocolException
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     */
    public function encode(array $payloads = []): string
    {
        if (! isset($payloads['data'])) {
            throw new ProtocolException("given produce data invalid, 'data' is undefined.");
        }

        $header = $this->requestHeader('Easyswoole-kafka', 0, self::PRODUCE_REQUEST);
        $data = self::pack(self::BIT_B16, (string) ($payloads['required_ack'] ?? 0));
        $data .= self::pack(self::BIT_B32, (string) ($payloads['timout'] ?? 100));
        $data .= self::encodeArray(
            $payloads['data'],
            [$this, 'encodeProduceTopic'],
            $payloads['compression'] ?? self::COMPRESSION_NONE
        );

        return self::encodeString($header, $data, self::PACK_INT32);
    }

    public function decode(string $data): array
    {
        // TODO: Implement decode() method.
    }


}