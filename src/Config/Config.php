<?php

namespace EasySwoole\Kafka\Config;

use EasySwoole\Spl\SplBean;
use EasySwoole\Kafka\Exception;

/**
 * Class Config
 * @package EasySwoole\Kafka
 *
 * @method string getClientId()
 * @method string getBrokerVersion()
 * @method string getMetadataBrokerList()
 * @method int getRequestTimeoutMs()
 * @method int getRefreshIntervalMs()
 * @method string getSecurityProtocol()
 * @method string getSaslMechanism()
 * @method string getSaslUsername()
 * @method string getSaslPassword()
 * @method string getSaslKeytab()
 * @method string getSaslPrincipal()
 * @method bool getSslEnable()
 * @method string getSslLocalCert()
 * @method string getSslLocalPk()
 * @method bool getSslVerifyPeer()
 * @method string getSslPassPhrase()
 * @method string getSslCafile()
 * @method string getSslPeerName()
 */
class Config extends SplBean
{
    public const SECURITY_PROTOCOL_PLAINTEXT = 'PLAINTEXT';
    public const SECURITY_PROTOCOL_SSL = 'SSL';
    public const SECURITY_PROTOCOL_SASL_PLAINTEXT = 'SASL_PLAINTEXT';
    public const SECURITY_PROTOCOL_SASL_SSL = 'SASL_SSL';

    public const SASL_MECHANISMS_PLAIN = 'PLAIN';
    public const SASL_MECHANISMS_GSSAPI = 'GSSAPI';
    public const SASL_MECHANISMS_SCRAM_SHA_256 = 'SCRAM_SHA_256';
    public const SASL_MECHANISMS_SCRAM_SHA_512 = 'SCRAM_SHA_512';

    private const ALLOW_SECURITY_PROTOCOLS = [
        self::SECURITY_PROTOCOL_PLAINTEXT,
        self::SECURITY_PROTOCOL_SSL,
        self::SECURITY_PROTOCOL_SASL_PLAINTEXT,
        self::SECURITY_PROTOCOL_SASL_SSL,
    ];

    private const ALLOW_MECHANISMS = [
        self::SASL_MECHANISMS_PLAIN,
        self::SASL_MECHANISMS_GSSAPI,
        self::SASL_MECHANISMS_SCRAM_SHA_256,
        self::SASL_MECHANISMS_SCRAM_SHA_512,
    ];

    protected static $options = [];

    private static $defaults = [
        'clientId'                      => 'Easyswoole-kafka',
        'brokerVersion'                 => '0.10.1.0',
        'metadataBrokerList'            => '',
        'requestTimeoutMs'              => 60000,// todo
        'refreshIntervalMs'             => 1000,
        'securityProtocol'              => self::SECURITY_PROTOCOL_PLAINTEXT,
        'saslMechanism'                 => self::SASL_MECHANISMS_PLAIN,
        'saslUsername'                  => '',
        'saslPassword'                  => '',
        'saslKeytab'                    => '',
        'saslPrincipal'                 => '',

        // if use ssl connect
        'sslEnable'                     => false,
        'sslLocalCert'                  => '',
        'sslLocalPk'                    => '',
        'sslVerifyPeer'                 => false,
        'sslPassPhrase'                 => '',
        'sslCafile'                     => '',
        'sslPeerName'                   => '',
    ];

    /**
     * @param string $name
     * @param array  $args
     * @return bool|mixed
     */
    public function __call(string $name, array $args)
    {
        $isGetter = strpos($name, 'get') === 0 || strpos($name, 'iet') === 0;
        $isSetter = strpos($name, 'set') === 0;

        if (! $isGetter && ! $isSetter) {
            return false;
        }

        // key
        $options = lcfirst(substr($name, 3));

        if ($isGetter) {
            if (isset(self::$options[$options])) {
                return self::$options[$options];
            }

            // config $defaults
            if (isset(self::$defaults[$options])) {
                return self::$defaults[$options];
            }

            // 继续子类 $defaults
            if (isset(static::$defaults[$options])) {
                return static::$defaults[$options];
            }

            return false;
        }

        if (count($args) !== 1) {
            return false;
        }

        // setter
        static::$options[$options] = array_shift($args);

        return true;
    }

    /**
     * clear options
     */
    public function clear(): void
    {
        static::$options = [];
    }

    /**
     * @param string $client
     * @throws Exception\Config
     */
    public function setClientId(string $client): void
    {
        $client = trim($client);

        if ($client === '') {
            throw new Exception\Config("Set ClientId value is invalid, must is not empty string.");
        }

        static::$options['clientId'] = $client;
    }

    /**
     * @param string $version
     * @throws Exception\Config
     */
    public function setBrokerVersion(string $version): void
    {
        $version = trim($version);

        if ($version === '' || version_compare($version, '0.8.0', '<')) {
            throw new Exception\Config("Set broker version value is invalid, must is not empty string and gt 0.8.0.");
        }

        static::$options['brokerVersion'] = $version;
    }

    /**
     * @param string $brokerList
     * @throws Exception\Config
     */
    public function setMetadataBrokerList(string $brokerList): void
    {
        $brokerList = trim($brokerList);

        // 拆分数组，检验每一个成员是否符合规则。最后仍存储string
        $brokers = array_filter(
            explode(',', $brokerList),
            function (string $broker): bool {
                return preg_match('/^(.*:[\d]+)$/', $broker) === 1;
            }
        );

        if (empty($brokers)) {
            throw new Exception\Config("Broker list must be a comma-separated list of brokers (format: 'host:port'), with at least one broker.");
        }
        static::$options['metadataBrokerList'] = $brokerList;
    }

    /**
     * @param int $requestTimeoutMs
     * @throws Exception\Config
     */
    public function setRequestTimeoutMs(int $requestTimeoutMs): void
    {
        if ($requestTimeoutMs < 10 || $requestTimeoutMs > 900000) {
            throw new Exception\Config("Set request timeout value is invalid, must set it 10 .. 900000.");
        }

        static::$options['requestTimeoutMs'] = $requestTimeoutMs;
    }

    /**
     * @param int $refreshIntervalMs
     * @throws Exception\Config
     */
    public function setRefreshIntervalMs(int $refreshIntervalMs): void
    {
        if ($refreshIntervalMs < 10 || $refreshIntervalMs > 360000) {
            throw new Exception\Config("Set refresh interval value is invalid, must set it 10 .. 360000.");
        }

        static::$options['refreshIntervalMs'] = $refreshIntervalMs;
    }

    /**
     * @param string $securityProtocol
     * @throws Exception\Config
     */
    public function setSecurityProtocol(string $securityProtocol): void
    {
        if (! in_array($securityProtocol, self::ALLOW_SECURITY_PROTOCOLS, true)) {
            throw new Exception\Config("Invalid security protocol given.");
        }

        static::$options['securityProtocol'] = $securityProtocol;
    }

    /**
     * @param string $username
     * @throws Exception\Config
     */
    public function setSaslUsername(string $username): void
    {
        $username = trim($username);
        if ($username === '') {
            throw new Exception\Config("Set sasl username value is invalid, must is not empty string.");
        }

        static::$options['saslUsername'] = $username;
    }

    /**
     * @param string $password
     * @throws Exception\Config
     */
    public function setSaslPassword(string $password): void
    {
        $password = trim($password);
        if ($password === '') {
            throw new Exception\Config("Set sasl password value is invalid, must is not empty string.");
        }

        static::$options['saslPassword'] = $password;
    }

    /**
     * @param string $principal
     * @throws Exception\Config
     */
    public function setSaslPrincipal(string $principal): void
    {
        $principal = trim($principal);
        if ($principal === '') {
            throw new Exception\Config("Set sasl principal value is invalid, must is not empty string.");
        }

        static::$options['saslPrincipal'] = $principal;
    }

    /**
     * @param string $keytab
     * @throws Exception\Config
     */
    public function setSaslKeytab(string $keytab): void
    {
        if (! is_file($keytab)) {
            throw new Exception\Config("Set sasl gssapi keytab file is invalid.");
        }

        static::$options['saslKeytab'] = $keytab;
    }

    /**
     * @param string $mechnism
     * @throws Exception\Config
     */
    public function setSaslMechanism(string $mechnism): void
    {
        if (! in_array($mechnism, self::ALLOW_MECHANISMS, true)) {
            throw new Exception\Config("Invalid security sasl mechanism given");
        }

        static::$options['saslMechanism'] = $mechnism;
    }

    /**
     * if use ssl connect
     * ---------------------------------------------------------------------
     */
    /**
     * @param bool $enable
     * @throws Exception\Config
     */
    public function setSslEnable(bool $enable): void
    {
        if (! is_bool($enable)) {
            throw new Exception\Config("Invalid ss enbale given");
        }

        static::$options['sslEnable'] = $enable;
    }

    /**
     * @param string $localCert
     * @throws Exception\Config
     */
    public function setSslLocalCert(string $localCert): void
    {
        if (! is_file($localCert)) {
            throw new Exception\Config("Set ssl local cert file is invalid.");
        }

        static::$options['sslLocalCert'] = $localCert;
    }

    /**
     * @param string $localPk
     * @throws Exception\Config
     */
    public function setSslLocalPk(string $localPk): void
    {
        if (! is_file($localPk)) {
            throw new Exception\Config("Set ssl local private key file is invalid.");
        }

        static::$options['sslLocalPk'] = $localPk;
    }

    /**
     * @param bool $verifyPeer
     * @throws Exception\Config
     */
    public function setSslVerifyPeer(bool $verifyPeer): void
    {
        if (! is_bool($verifyPeer)) {
            throw new Exception\Config("Set ssl verify peer values is invalid, must is not empty string.");
        }

        static::$options['sslVerifyPeer'] = $verifyPeer;
    }

    /**
     * @param string $passPhrase
     * @throws Exception\Config
     */
    public function setSslPassPhrase(string $passPhrase): void
    {
        $passPhrase = trim($passPhrase);
        if ($passPhrase === '') {
            throw new Exception\Config("Set ssl passPhare value is invalid, must is not empty string.");
        }

        static::$options['sslPassPhrase'] = $passPhrase;
    }

    /**
     * @param string $cafile
     * @throws Exception\Config
     */
    public function setSslCafile(string $cafile): void
    {
        if (! is_file($cafile)) {
            throw new Exception\Config("Set ssl ca file is invalid.");
        }

        static::$options['sslCafile'] = $cafile;
    }

    /**
     * @param string $peerName
     * @throws Exception\Config
     */
    public function setSslPeerName(string $peerName): void
    {
        $peerName = trim($peerName);
        if ($peerName === '') {
            throw new Exception\Config();
        }

        static::$options['sslPeerName'] = $peerName;
    }
}
