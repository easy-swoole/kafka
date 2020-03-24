<?php
/**
 * Created by PhpStorm.
 * User: Manlin
 * Date: 2019/8/21
 * Time: 上午9:11
 */
namespace EasySwoole\Kafka;

use EasySwoole\Kafka\Config\Config;
use EasySwoole\Kafka\Exception\ConnectionException;
use EasySwoole\Kafka\Exception\Exception;
use Swoole\Coroutine\Client as CoroutineClient;

class Client
{
    /*
     * @var string
     */
    protected $host;

    /**
     * @var int
     */
    protected $port = -1;

    /**
     * @var Config
     */
    protected $config;

    /**
     * @var \swoole_client
     */
    protected $client;
    /**
     * @var SaslMechanism
     */
    private $saslProvider;

    /**
     * @var array
     */
    protected $clientConfig;

    /**
     * CommonClient constructor.
     * @param string             $host
     * @param int                $port
     * @param Config|null        $config
     * @param SaslMechanism|null $saslProvider
     * @throws Exception
     */
    public function __construct(string $host, int $port, ?Config $config = null, ?SaslMechanism $saslProvider = null)
    {
        $this->host         = $host;
        $this->port         = $port;
        $this->config       = $config;
        $this->saslProvider = $saslProvider;
    }

    /**
     * @return bool
     * @throws ConnectionException
     * @throws Exception
     */
    public function connect(): bool
    {
        if (trim($this->host) === '') {
            throw new Exception("Cannot open null host.");
        }

        if ($this->port <= 0) {
            throw new Exception("Cannot open without port.");
        }

        $settings = [
            'open_length_check'     => 1,
            'package_length_type'   => 'N',
            'package_length_offset' => 0,
            'package_body_offset'   => 4,
            'package_max_length'    => 1024 * 1024 * 3,
        ];

        if (!$this->client instanceof Client) {
            $this->client = new CoroutineClient(SWOOLE_TCP);
            $this->client->set($settings);
        }

        // ssl connection
        if ($this->config !== null && $this->config->getSslEnable()) {
            array_push($settings, [
                'ssl_verify_peer'   => $this->config->getSslVerifyPeer(),
                'ssl_cafile'        => $this->config->getSslCafile(),
            ]);
        }

        if (!$this->client->isConnected()) {
            $connected = $this->client->connect($this->host, $this->port);
            if (!$connected) {
                $connectStr = "tcp://{$this->host}:{$this->port}";
                throw new ConnectionException("Connect to Kafka server {$connectStr} failed: {$this->client->errMsg}");
            }
        }

        // todo 未实现接口
        if ($this->saslProvider !== null) {
            $this->saslProvider->autheticate($this);
        }

        return (bool)$this->client->isConnected();
    }

    /**
     * @return bool
     */
    public function isConnected(): bool
    {
        if ($this->client->isConnected()) {
            return true;
        }

        return false;
    }

    /**
     * @param null|string $data
     * @param int         $tries
     * @return mixed
     * @throws ConnectionException
     * @throws Exception
     */
    public function send(?string $data = null, $tries = 2)
    {
        for ($try = 0; $try <= $tries; $try++) {
            if ($this->isConnected()) {
                $this->client->send($data);
                return $this->client->recv();
            }
            $this->connect();
            continue;
        }
        $connectStr = "tcp://{$this->host}:{$this->port}";
        throw new ConnectionException("Connect to Kafka server {$connectStr} failed: {$this->client->errMsg}");
    }

    /**
     * @param null|string $data
     * @param int         $tries
     * @return mixed
     * @throws ConnectionException
     * @throws Exception
     */
    public function sendWithNoResponse(?string $data = null, $tries = 2)
    {
        for ($try = 0; $try <= $tries; $try++) {
            if ($this->isConnected()) {
                return $this->client->send($data);
            }
            $this->connect();
            continue;
        }
        $connectStr = "tcp://{$this->host}:{$this->port}";
        throw new ConnectionException("Connect to Kafka server {$connectStr} failed: {$this->client->errMsg}");
    }

    /**
     * @param float $timeout
     * @return string
     * @throws ConnectionException
     * @throws Exception
     */
    public function recv(float $timeout = -1): string
    {
        if ($this->connect()) {
            $data = $this->client->recv($timeout);
            return $data;
        }
        $connectStr = "tcp://{$this->host}:{$this->port}";
        throw new ConnectionException("Connect to Kafka server {$connectStr} failed: {$this->client->errMsg}");
    }

    public function close()
    {
        $this->client->close();
    }

    /**
     * @return bool
     * @throws ConnectionException
     * @throws Exception
     */
    public function reconnect()
    {
        if ($this->client) {
            $this->client->close();
        }

        return $this->connect();
    }
}
