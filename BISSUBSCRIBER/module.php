<?
/**
 * @file
 * MQTT Subscriber (BIS): Symcon MQTT-Splitter -> IPS
 *
 * @author Martin Hilbert
 */

include_once(__DIR__ . "/../lib/module_helper.php");

class BISSubscriber extends T2DModule
{
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

    public function ApplyChanges()
    {
        $this->RegisterMessage(0, self::IPS_KERNELMESSAGE);
        parent::ApplyChanges();

        if (!$this->isActive()) {
            $this->SetStatus(self::ST_INACTIV);
            return;
        }

        $this->SetStatus(self::ST_AKTIV);
        $this->sendSubscribeToParent();
    }

    public function MessageSink($TimeStamp, $SenderID, $Message, $Data)
    {
        if ($Message == self::IPS_KERNELMESSAGE && isset($Data[0]) && $Data[0] == self::KR_READY) {
            $this->RegisterMessage(0, self::IPS_KERNELMESSAGE);
            if ($this->isActive()) {
                $this->SetStatus(self::ST_AKTIV);
                $this->sendSubscribeToParent();
            }
        }
    }

    /**
     * Receives data from Symcon parent (MQTT Splitter chain).
     * The payload format may vary by parent implementation, therefore fields are resolved robustly.
     *
     * @param string $JSONString
     */
    public function ReceiveData($JSONString)
    {
        if (!$this->isActive()) {
            return;
        }

        $this->debug(__FUNCTION__, 'Raw JSON: ' . $JSONString);
        $data = json_decode($JSONString, true);
        if (!is_array($data)) {
            $this->debug(__FUNCTION__, 'Invalid JSON payload from parent');
            return;
        }

        $topic = $this->extractTopicFromParentData($data);
        $payload = $this->extractPayloadFromParentData($data);
        if ($topic === '') {
            $this->debug(__FUNCTION__, 'No topic found in parent payload');
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

    private function extractTopicFromParentData(array $data): string
    {
        $candidates = array('Topic', 'topic', 'TOPIC');
        foreach ($candidates as $key) {
            if (isset($data[$key])) {
                return (string)$data[$key];
            }
        }

        if (isset($data['Buffer']) && is_string($data['Buffer'])) {
            $inner = json_decode($data['Buffer'], true);
            if (is_array($inner)) {
                foreach ($candidates as $key) {
                    if (isset($inner[$key])) {
                        return (string)$inner[$key];
                    }
                }
            }
        }

        return '';
    }

    private function extractPayloadFromParentData(array $data): string
    {
        $candidates = array('Payload', 'payload', 'Value', 'value');
        foreach ($candidates as $key) {
            if (isset($data[$key])) {
                return (string)$data[$key];
            }
        }

        if (isset($data['Buffer']) && is_string($data['Buffer'])) {
            $inner = json_decode($data['Buffer'], true);
            if (is_array($inner)) {
                foreach ($candidates as $key) {
                    if (isset($inner[$key])) {
                        return (string)$inner[$key];
                    }
                }
            }
            return (string)$data['Buffer'];
        }

        return '';
    }

    private function sendSubscribeToParent(): void
    {
        if (!(bool)IPS_GetProperty($this->InstanceID, 'SubscribeOnApply')) {
            $this->debug(__FUNCTION__, 'SubscribeOnApply disabled');
            return;
        }

        $topic = $this->normalizeSubscribeTopic($this->GetSubscribeTopic());
        $payload = array(
            'Command' => 'Subscribe',
            'Topic'   => $topic,
            'QoS'     => 0
        );
        $json = json_encode($payload);
        $this->debug(__FUNCTION__, 'SendDataToParent subscribe: ' . $json);
        @$this->SendDataToParent($json);
    }

    private function GetSubscribeTopic(): string
    {
        return (string)IPS_GetProperty($this->InstanceID, 'SubscribeTopic');
    }
}
