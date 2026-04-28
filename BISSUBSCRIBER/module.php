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
        $this->RegisterPropertyString('SubscribeTopic', 'BIS/IPS/#');

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

        $this->debug(__FUNCTION__, 'Starting listener loop');
        $this->debug(__FUNCTION__, 'Config Host=' . $this->GetHost() . ' Port=' . $this->GetPort() . ' ClientID=' . $this->GetClientID());

        $mqtt = $this->buildMqttClient();
        $username = $this->GetUser();
        $password = $this->GetPassword();
        $this->debug(__FUNCTION__, 'Credentials User=' . ($username !== '' ? $username : '<leer>') . ' Password=' . ($password !== '' ? '<gesetzt>' : '<leer>'));

        if (!$mqtt->connect(true, null, $username, $password)) {
            $this->printMqttDebug($mqtt);
            IPS_LogMessage(__CLASS__, __FUNCTION__ . ':: Verbindung zum Broker fehlgeschlagen');
            return false;
        }
        $this->debug(__FUNCTION__, 'Broker connection established');
        $this->printMqttDebug($mqtt);

        $subscribeTopic = $this->normalizeSubscribeTopic($this->GetSubscribeTopic());
        $topics = array(
            $subscribeTopic => array(
                'qos'      => self::MQTT_QOS_0_AT_MOST_ONCE,
                'function' => array($this, 'onMqttMessage')
            )
        );
        $mqtt->subscribe($topics, self::MQTT_QOS_0_AT_MOST_ONCE);
        $this->debug(__FUNCTION__, 'Subscribed to topic ' . $subscribeTopic);
        $this->debug(__FUNCTION__, 'Expected command base topic ' . $this->getExpectedCommandBaseTopic());
        $this->printMqttDebug($mqtt);

        $started = time();
        $lastLoopDebug = $started;
        $loopCounter = 0;
        while (true) {
            $mqtt->proc();
            $loopCounter++;

            // Keepalive/processing visibility while waiting for messages.
            $now = time();
            if (($now - $lastLoopDebug) >= 5) {
                $this->debug(__FUNCTION__, 'Listener active, loop=' . $loopCounter . ' runtime=' . ($now - $started) . 's');
                $this->printMqttDebug($mqtt);
                $lastLoopDebug = $now;
            }

            if ($durationSeconds > 0 && (time() - $started) >= $durationSeconds) {
                $this->debug(__FUNCTION__, 'Duration reached, stopping listener');
                break;
            }
        }

        $this->printMqttDebug($mqtt);
        $mqtt->close();
        $this->debug(__FUNCTION__, 'Listener stopped, MQTT closed');
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
        $this->debug(__FUNCTION__, 'Incoming topic: ' . $topic . ' payload: "' . $message . '"');
        $this->debug(__FUNCTION__, 'Payload length=' . strlen((string)$message));

        $variableId = $this->extractVariableIdFromTopic((string)$topic);
        if ($variableId === null) {
            $this->debug(__FUNCTION__, 'Topic ignored (no valid <id>/set below SubscribeTopic): ' . $topic);
            return;
        }
        $this->debug(__FUNCTION__, 'Resolved variable id=' . $variableId);

        $targetValue = $this->parseBooleanPayload($message);
        if ($targetValue === null) {
            $this->debug(__FUNCTION__, 'Payload ignored (expected 0|1): ' . $message);
            return;
        }
        $this->debug(__FUNCTION__, 'Resolved target value=' . ($targetValue ? '1' : '0'));

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
        $this->debug(__FUNCTION__, 'Switch requested for ID ' . $variableId . ' -> ' . ($targetValue ? '1' : '0'));

        if (!IPS_VariableExists($variableId)) {
            IPS_LogMessage(__CLASS__, __FUNCTION__ . ":: Variable $variableId existiert nicht");
            $this->debug(__FUNCTION__, "Variable $variableId does not exist");
            return;
        }

        $valueForIps = $targetValue ? 1 : 0;

        // Bevorzugt RequestAction, damit zugeordnete Instanz-Logik (z.B. Aktor) ausgeführt wird.
        $this->debug(__FUNCTION__, 'Trying RequestAction');
        if (@RequestAction($variableId, $valueForIps)) {
            $this->debug(__FUNCTION__, "RequestAction erfolgreich: ID $variableId -> $valueForIps");
            return;
        }
        $this->debug(__FUNCTION__, "RequestAction failed for ID $variableId");

        // Fallback: direkt die Variable setzen (falls kein RequestAction verfügbar ist).
        $this->debug(__FUNCTION__, 'Trying SetValue fallback');
        if (@SetValue($variableId, $valueForIps)) {
            $this->debug(__FUNCTION__, "SetValue erfolgreich: ID $variableId -> $valueForIps");
            return;
        }

        $this->debug(__FUNCTION__, "SetValue failed for ID $variableId");
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
            return 'BIS/IPS/#';
        }
        return ltrim($topic, '/');
    }

    /**
     * Extract variable id from "<base>/<id>/set", where <base> comes from SubscribeTopic.
     *
     * @param string $topic
     * @return int|null
     */
    private function extractVariableIdFromTopic(string $topic): ?int
    {
        $normalizedTopic = trim($topic, " \t\n\r\0\x0B/");
        $this->debug(__FUNCTION__, 'Normalized topic=' . $normalizedTopic);
        if (!preg_match('#^(.+)/(\d+)/set$#', $normalizedTopic, $matches)) {
            $this->debug(__FUNCTION__, 'Regex mismatch for expected pattern <base>/<id>/set');
            return null;
        }

        $incomingBase = $matches[1];
        $variableId = (int)$matches[2];
        $this->debug(__FUNCTION__, 'Incoming base=' . $incomingBase . ' extracted id=' . $variableId);

        $expectedBase = $this->getExpectedCommandBaseTopic();
        $this->debug(__FUNCTION__, 'Expected base=' . $expectedBase);
        if ($expectedBase !== '' && $incomingBase !== $expectedBase) {
            $this->debug(__FUNCTION__, 'Base mismatch, topic ignored');
            return null;
        }

        return $variableId;
    }

    /**
     * Convert configured SubscribeTopic into base command path.
     * Examples:
     * - "IPS/BM/Beleuchtung/#" -> "IPS/BM/Beleuchtung"
     * - "BIS/IPS/+/set" -> "BIS/IPS"
     *
     * @return string
     */
    private function getExpectedCommandBaseTopic(): string
    {
        $topic = $this->normalizeSubscribeTopic($this->GetSubscribeTopic());
        $topic = trim($topic, " \t\n\r\0\x0B/");

        if (substr($topic, -2) === '/#') {
            return substr($topic, 0, -2);
        }

        if (substr($topic, -6) === '/+/set') {
            return substr($topic, 0, -6);
        }

        if (substr($topic, -2) === '/+') {
            return substr($topic, 0, -2);
        }

        return rtrim($topic, '/');
    }

    private function printMqttDebug(IPSphpMQTT $mqtt): void
    {
        if (!is_array($mqtt->debugmsg)) {
            return;
        }

        while (count($mqtt->debugmsg) > 0) {
            $msg = array_shift($mqtt->debugmsg);
            $this->debug('IPSphpMQTT', $msg);
        }
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
