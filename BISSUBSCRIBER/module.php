<?
/**
 * @file
 *
 * MQTT Subscriber (BIS): MQTT-Broker → IPS (Grundgerüst)
 *
 * @author Martin Hilbert
 */

include_once(__DIR__ . "/../lib/module_helper.php");
include_once(__DIR__ . "/../lib/IPSphpMQTT.php");

/** @class BISSubscriber
 *
 * IPSymcon PHP-Modul: Subscriber-Grundgerüst für BIS
 */
class BISSubscriber extends T2DModule
{
    /**
     * MQTT QOS constant "At Most once" (Fire and forget)
     */
    const MQTT_QOS_0_AT_MOST_ONCE = 0;

    /**
     * MQTT keepalive in seconds.
     */
    const MQTT_KEEPALIVE_SECONDS = 10;

    /**
     * Constructor.
     * @param int $InstanceID
     */
    public function __construct($InstanceID)
    {
        $json = __DIR__ . "/module.json";
        parent::__construct($InstanceID, $json);
    }

    public function Create()
    {
        parent::Create();

        $this->RegisterPropertyInteger('Port', 1883);
        $this->RegisterPropertyString('Host', 'mqttbroker');
        $this->RegisterPropertyString('ClientID', 'symcon-bis-sub');
        $this->RegisterPropertyString('User', '');
        $this->RegisterPropertyString('Password', '');
        $this->RegisterPropertyBoolean('Debug', false);
        $this->RegisterPropertyBoolean('Active', false);
        $this->RegisterPropertyString('SubscribeTopic', '/BIS/IPS/+/set');

        $this->RegisterMessage(0, self::IPS_KERNELMESSAGE);

        IPS_SetName($this->InstanceID, 'BISSubscriber');
    }

    public function Destroy()
    {
        parent::Destroy();
    }

    public function ApplyChanges()
    {
        $this->RegisterMessage(0, self::IPS_KERNELMESSAGE);
        parent::ApplyChanges();

        if ($this->isActive()) {
            $this->SetStatus(self::ST_AKTIV);
        } else {
            $this->SetStatus(self::ST_INACTIV);
        }
    }

    public function MessageSink($TimeStamp, $SenderID, $Message, $Data)
    {
        if ($Message == self::IPS_KERNELMESSAGE && isset($Data[0]) && $Data[0] == self::KR_READY) {
            $this->RegisterMessage(0, self::IPS_KERNELMESSAGE);
            if ($this->isActive()) {
                $this->SetStatus(self::ST_AKTIV);
            }
        }
    }

    /**
     * Start MQTT listening loop.
     * If $durationSeconds is 0, it will run indefinitely.
     *
     * @param int $durationSeconds
     * @return bool
     */
    public function Listen(int $durationSeconds = 0): bool
    {
        if (!$this->isActive()) {
            $this->debug(__FUNCTION__, 'Subscriber is inactive');
            return false;
        }

        $mqtt = $this->buildMqttClient();
        $username = $this->GetUser();
        $password = $this->GetPassword();

        if (!$mqtt->connect(true, null, $username, $password)) {
            IPS_LogMessage(__CLASS__, __FUNCTION__ . ':: Verbindung zum Broker fehlgeschlagen');
            return false;
        }

        $subscribeTopic = $this->normalizeSubscribeTopic($this->GetSubscribeTopic());
        $topics = array(
            $subscribeTopic => array(
                'qos'      => self::MQTT_QOS_0_AT_MOST_ONCE,
                'function' => array($this, 'onMqttMessage')
            )
        );
        $mqtt->subscribe($topics, self::MQTT_QOS_0_AT_MOST_ONCE);
        $this->debug(__FUNCTION__, 'Subscribed to topic ' . $subscribeTopic);

        $started = time();
        while (true) {
            $mqtt->proc();
            if ($durationSeconds > 0 && (time() - $started) >= $durationSeconds) {
                break;
            }
        }

        $mqtt->close();
        return true;
    }

    /**
     * MQTT callback for incoming messages.
     *
     * @param string $topic
     * @param string $message
     * @return void
     */
    public function onMqttMessage($topic, $message)
    {
        $this->debug(__FUNCTION__, 'Incoming topic: ' . $topic . ' payload: ' . $message);

        $topicRegex = '#^/?BIS/IPS/(\d+)/set$#';
        if (!preg_match($topicRegex, $topic, $matches)) {
            $this->debug(__FUNCTION__, 'Topic ignored (no match): ' . $topic);
            return;
        }

        $variableId = (int)$matches[1];
        $targetValue = $this->parseBooleanPayload($message);
        if ($targetValue === null) {
            $this->debug(__FUNCTION__, 'Payload ignored (expected 0|1): ' . $message);
            return;
        }

        $this->setVariableToBoolean($variableId, $targetValue);
    }

    /**
     * Try switching a variable by id.
     *
     * @param int $variableId
     * @param bool $targetValue
     * @return void
     */
    private function setVariableToBoolean(int $variableId, bool $targetValue): void
    {
        if (!IPS_VariableExists($variableId)) {
            IPS_LogMessage(__CLASS__, __FUNCTION__ . ":: Variable $variableId existiert nicht");
            return;
        }

        $valueForIps = $targetValue ? 1 : 0;

        // Bevorzugt RequestAction, damit zugeordnete Instanz-Logik (z.B. Aktor) ausgeführt wird.
        if (@RequestAction($variableId, $valueForIps)) {
            $this->debug(__FUNCTION__, "RequestAction erfolgreich: ID $variableId -> $valueForIps");
            return;
        }

        // Fallback: direkt die Variable setzen (falls kein RequestAction verfügbar ist).
        if (@SetValue($variableId, $valueForIps)) {
            $this->debug(__FUNCTION__, "SetValue erfolgreich: ID $variableId -> $valueForIps");
            return;
        }

        IPS_LogMessage(__CLASS__, __FUNCTION__ . ":: Schalten fehlgeschlagen: ID $variableId -> $valueForIps");
    }

    /**
     * Parse incoming payload to bool.
     *
     * @param string $payload
     * @return bool|null
     */
    private function parseBooleanPayload(string $payload): ?bool
    {
        $normalized = trim($payload);
        if ($normalized === '1') {
            return true;
        }
        if ($normalized === '0') {
            return false;
        }
        return null;
    }

    /**
     * Create MQTT client from instance properties.
     *
     * @return IPSphpMQTT
     */
    private function buildMqttClient(): IPSphpMQTT
    {
        $host = $this->GetHost();
        $port = $this->GetPort();
        $clientId = $this->GetClientID();

        $mqtt = new IPSphpMQTT($host, $port, $clientId);
        $mqtt->keepalive = self::MQTT_KEEPALIVE_SECONDS;
        return $mqtt;
    }

    private function normalizeSubscribeTopic(string $topic): string
    {
        $topic = trim($topic);
        if ($topic === '') {
            return 'BIS/IPS/+/set';
        }
        return ltrim($topic, '/');
    }

    private function GetHost(): string
    {
        return (string)IPS_GetProperty($this->InstanceID, 'Host');
    }

    private function GetPort(): int
    {
        return (int)IPS_GetProperty($this->InstanceID, 'Port');
    }

    private function GetClientID(): string
    {
        $clientId = (string)IPS_GetProperty($this->InstanceID, 'ClientID');
        return $clientId . '@' . gethostname();
    }

    private function GetUser(): string
    {
        return (string)IPS_GetProperty($this->InstanceID, 'User');
    }

    private function GetPassword(): string
    {
        return (string)IPS_GetProperty($this->InstanceID, 'Password');
    }

    private function GetSubscribeTopic(): string
    {
        return (string)IPS_GetProperty($this->InstanceID, 'SubscribeTopic');
    }
}
