<?
/**
 * @file
 * MQTT Subscriber (BIS): Symcon-MQTT-Gateway (Splitter) -> IPS
 *
 * Als Geräte-Instanz unter den integrierten MQTT-Client von Symcon (oder kompatible
 * Splitter mit gleichem Datenfluss) hängen.
 *
 * @author Martin Hilbert
 */

include_once(__DIR__ . "/../lib/module_helper.php");

class BISSubscriber extends T2DModule
{
    /** Simple RX (vom Parent zu Kind, typisch I/O / Splitter-Kette) */
    const DATA_SIMPLE_RX = '{018EF6B5-AB94-40C6-AA53-46943E824ACF}';
    /** TX zum Parent (z. B. Subscribe-Befehl an MQTT-Splitter) */
    const DATA_SPLITTER_TX = '{97475B04-67C3-A74D-C970-E9409B0EFA1D}';
    /** MQTT-Splitter -> Gerät (z. B. Schnittcher MQTTClient / kompatible Weiterleitung) */
    const DATA_MQTT_CHILD_RX = '{DBDA9DF7-5D04-F49D-370A-2B9153D00D9B}';

    public function __construct($InstanceID)
    {
        $json = __DIR__ . "/module.json";
        parent::__construct($InstanceID, $json);
    }

    public function Create()
    {
        parent::Create();

        $this->RegisterPropertyBoolean('Debug', false);
        $this->RegisterPropertyBoolean('Active', false);
        $this->RegisterPropertyString('SubscribeTopic', 'BIS/IPS/#');
        $this->RegisterPropertyBoolean('SubscribeOnApply', true);

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

        if (!$this->isActive()) {
            $this->SetStatus(self::ST_INACTIV);
            return;
        }

        if (!$this->HasActiveParent()) {
            $this->SetStatus(self::ST_NOPARENT);
            $this->debug(__FUNCTION__, 'Kein aktiver Parent (MQTT-Gateway). Verbindung im Objektbaum setzen.');
            return;
        }

        $this->SetStatus(self::ST_AKTIV);
        $this->sendSubscribeToParent();
    }

    public function MessageSink($TimeStamp, $SenderID, $Message, $Data)
    {
        if ($Message == self::IPS_KERNELMESSAGE && isset($Data[0]) && $Data[0] == self::KR_READY) {
            $this->RegisterMessage(0, self::IPS_KERNELMESSAGE);
            if ($this->isActive() && $this->HasActiveParent()) {
                $this->SetStatus(self::ST_AKTIV);
                $this->sendSubscribeToParent();
            }
        }
    }

    /**
     * Daten vom übergeordneten MQTT-Gateway (Splitter).
     *
     * @param string $JSONString
     * @return void
     */
    public function ReceiveData($JSONString)
    {
        if (!$this->isActive()) {
            return;
        }

        $this->debug(__FUNCTION__, 'Raw JSON: ' . $JSONString);
        $data = json_decode($JSONString, true);
        if (!is_array($data)) {
            $this->debug(__FUNCTION__, 'Ungültiges JSON vom Parent');
            return;
        }

        $dataId = isset($data['DataID']) ? (string)$data['DataID'] : '';
        $inner = $this->normalizePayloadArray($data, $dataId);

        $topic = $this->extractTopicFromPayload($inner);
        $payload = $this->extractPayloadStringFromPayload($inner);
        if ($topic === '') {
            $this->debug(__FUNCTION__, 'Kein Topic ermittelbar (DataID=' . $dataId . ')');
            return;
        }

        $this->onMqttMessage($topic, $payload);
    }

    public function onMqttMessage(string $topic, string $message): void
    {
        $this->debug(__FUNCTION__, 'Incoming topic: ' . $topic . ' payload: "' . $message . '"');
        $this->debug(__FUNCTION__, 'Payload length=' . strlen($message));

        $variableId = $this->extractVariableIdFromTopic($topic);
        if ($variableId === null) {
            $this->debug(__FUNCTION__, 'Topic ignoriert (kein gültiges <id>/set zum SubscribeTopic): ' . $topic);
            return;
        }
        $this->debug(__FUNCTION__, 'Variable id=' . $variableId);

        $targetValue = $this->parseBooleanPayload($message);
        if ($targetValue === null) {
            $this->debug(__FUNCTION__, 'Payload ignoriert (erwartet 0|1): ' . $message);
            return;
        }
        $this->debug(__FUNCTION__, 'Zielwert=' . ($targetValue ? '1' : '0'));

        $this->setVariableToBoolean($variableId, $targetValue);
    }

    private function setVariableToBoolean(int $variableId, bool $targetValue): void
    {
        $this->debug(__FUNCTION__, 'Schalten ID ' . $variableId . ' -> ' . ($targetValue ? '1' : '0'));

        if (!IPS_VariableExists($variableId)) {
            IPS_LogMessage(__CLASS__, __FUNCTION__ . ":: Variable $variableId existiert nicht");
            $this->debug(__FUNCTION__, "Variable $variableId existiert nicht");
            return;
        }

        $valueForIps = $targetValue ? 1 : 0;

        $this->debug(__FUNCTION__, 'RequestAction');
        if (@RequestAction($variableId, $valueForIps)) {
            $this->debug(__FUNCTION__, "RequestAction OK: ID $variableId -> $valueForIps");
            return;
        }
        $this->debug(__FUNCTION__, "RequestAction fehlgeschlagen für ID $variableId");

        $this->debug(__FUNCTION__, 'SetValue Fallback');
        if (@SetValue($variableId, $valueForIps)) {
            $this->debug(__FUNCTION__, "SetValue OK: ID $variableId -> $valueForIps");
            return;
        }

        $this->debug(__FUNCTION__, "SetValue fehlgeschlagen für ID $variableId");
        IPS_LogMessage(__CLASS__, __FUNCTION__ . ":: Schalten fehlgeschlagen: ID $variableId -> $valueForIps");
    }

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

    private function normalizePayloadArray(array $data, string $dataId): array
    {
        $inner = array();

        if (!isset($data['Buffer'])) {
            return $data;
        }

        $buffer = $data['Buffer'];
        if (!is_string($buffer)) {
            return $data;
        }

        $decoded = utf8_decode($buffer);
        $parsed = json_decode($decoded, true);

        if ($dataId === self::DATA_MQTT_CHILD_RX && is_array($parsed)) {
            return $parsed;
        }

        if (is_array($parsed)) {
            return array_merge($data, $parsed);
        }

        return $data;
    }

    private function extractTopicFromPayload(array $inner): string
    {
        $keys = array('Topic', 'topic', 'TOPIC');
        foreach ($keys as $k) {
            if (isset($inner[$k]) && (string)$inner[$k] !== '') {
                return (string)$inner[$k];
            }
        }
        return '';
    }

    private function extractPayloadStringFromPayload(array $inner): string
    {
        $keys = array('Payload', 'payload', 'Message', 'message', 'Value', 'value');
        foreach ($keys as $k) {
            if (isset($inner[$k])) {
                if (is_scalar($inner[$k])) {
                    return (string)$inner[$k];
                }
                return json_encode($inner[$k]);
            }
        }
        return '';
    }

    private function normalizeSubscribeTopic(string $topic): string
    {
        $topic = trim($topic);
        if ($topic === '') {
            return 'BIS/IPS/#';
        }
        return ltrim($topic, '/');
    }

    private function extractVariableIdFromTopic(string $topic): ?int
    {
        $normalizedTopic = trim($topic, " \t\n\r\0\x0B/");
        $this->debug(__FUNCTION__, 'Normalized topic=' . $normalizedTopic);
        if (!preg_match('#^(.+)/(\d+)/set$#', $normalizedTopic, $matches)) {
            return null;
        }

        $incomingBase = $matches[1];
        $variableId = (int)$matches[2];
        $this->debug(__FUNCTION__, 'Base=' . $incomingBase . ' id=' . $variableId);

        $expectedBase = $this->getExpectedCommandBaseTopic();
        if ($expectedBase !== '' && $incomingBase !== $expectedBase) {
            $this->debug(__FUNCTION__, 'Base passt nicht, ignoriert');
            return null;
        }

        return $variableId;
    }

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

    private function sendSubscribeToParent(): void
    {
        if (!$this->HasActiveParent()) {
            $this->debug(__FUNCTION__, 'Kein Parent – Subscribe übersprungen');
            return;
        }

        if (!(bool)IPS_GetProperty($this->InstanceID, 'SubscribeOnApply')) {
            $this->debug(__FUNCTION__, 'SubscribeOnApply aus');
            return;
        }

        $topic = $this->normalizeSubscribeTopic($this->GetSubscribeTopic());

        $body = array(
            'Function' => 'Subscribe',
            'Topic'    => $topic,
        );
        $inner = json_encode($body, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        $packet = json_encode(array(
            'DataID' => self::DATA_SPLITTER_TX,
            'Buffer' => utf8_encode($inner),
        ), JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);
        $this->debug(__FUNCTION__, 'SendDataToParent: ' . $packet);
        @$this->SendDataToParent($packet);
    }

    private function GetSubscribeTopic(): string
    {
        return (string)IPS_GetProperty($this->InstanceID, 'SubscribeTopic');
    }
}
