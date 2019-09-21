<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/9/20
 * Time: 上午10:16
 */
namespace EasySwoole\Kafka\Protocol;

use function substr;

class ApiVersions extends Protocol
{
    /**
     * @param array $payloads
     * @return string
     * @throws \EasySwoole\Kafka\Exception\NotSupported
     */
    public function encode(array $payloads = []): string
    {
        $header = $this->requestHeader('Easyswoole-kafka', self::API_VERSIONS_REQUEST, self::API_VERSIONS_REQUEST);

        return self::encodeString($header, self::PACK_INT32);
    }

    /**
     * @param string $data
     * @return array
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    public function decode(string $data): array
    {
        $offset      = 0;
        $errcode     = self::unpack(self::BIT_B16, substr($data, $offset, 2));
        $offset     += 2;
        $apiVersions = $this->decodeArray(substr($data, $offset), [$this, 'apiVersion']);
        $offset     += $apiVersions['length'];

        $throttleTime = 0;
        $version      = $this->getApiVersion(self::API_VERSIONS_REQUEST);
        if ($version === self::API_VERSION2 || $version === self::API_VERSION1) {
            $throttleTime = self::unpack(self::BIT_B32, substr($data, $offset, 4));
        }
        return [
            'errorCode'   => $errcode,
            'apiVersions' => $apiVersions['data'],
            'throttleTime' => $throttleTime,
        ];
    }

    /**
     * @param string $data
     * @return array
     * @throws \EasySwoole\Kafka\Exception\Exception
     */
    protected function apiVersion(string $data): array
    {
        $offset     = 0;
        $apiKey     = self::unpack(self::BIT_B16, substr($data, $offset, 2));
        $offset    += 2;
        $minVersion = self::unpack(self::BIT_B16, substr($data, $offset, 2));
        $offset    += 2;
        $maxVersion = self::unpack(self::BIT_B16, substr($data, $offset, 2));
        $offset    += 2;

        return [
            'length' => $offset,
            'data'   => [$apiKey, $minVersion, $maxVersion],
        ];
    }
}
