<?
/**
 * @file
 * MQTT Subscriber (BIS): Symcon-MQTT-Gateway (Splitter) -> IPS
 *
 * Nur IP-Symcon-Basisklasse IPSModule (ohne externes module_helper).
 *
 * @author Martin Hilbert
 */
class BISSubscriber extends IPSModule
{
    const IPS_KERNELMESSAGE = 10100;
    const KR_READY = 10103;

    const ST_AKTIV = 102;
    const ST_INACTIV = 104;
    const ST_NOPARENT = 202;

    /** Kind <- integrierter Symcon „MQTT Client“ (IPS_GetModule ChildRequirements) */
    const DATA_MQTT_CLIENT_CHILD_RX = '{7F7632D9-FA40-4F38-8DEA-C83CD4325A32}';
    /** Kind -> Parent MQTT Client (IPS_GetModule Implemented, u. a. für ForwardData) */
    const DATA_MQTT_CLIENT_TX_TO_PARENT = '{043EA491-0325-4ADD-8FC2-A30C8EEB4D3F}';

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

        if (!$this->isModuleActive()) {
            $this->SetStatus(self::ST_INACTIV);
            return;
        }

        if (!$this->hasActiveParentConnection()) {
            $this->SetStatus(self::ST_NOPARENT);
            $this->moduleDebug(__FUNCTION__, 'Kein aktiver Parent (MQTT-Gateway). Verbindung im Objektbaum setzen.');
            return;
        }

        $this->SetStatus(self::ST_AKTIV);
        $this->sendSubscribeToParent();
    }

    public function MessageSink($TimeStamp, $SenderID, $Message, $Data)
    {
        if ($Message == self::IPS_KERNELMESSAGE && isset($Data[0]) && $Data[0] == self::KR_READY) {
            $this->RegisterMessage(0, self::IPS_KERNELMESSAGE);
            if ($this->isModuleActive() && $this->hasActiveParentConnection()) {
                $this->SetStatus(self::ST_AKTIV);
                $this->sendSubscribeToParent();
            }
        }
    }

    /**
     * @param string $JSONString
     */
    public function ReceiveData($JSONString)
    {
        if (!$this->isModuleActive()) {
            return;
        }

        $this->moduleDebug(__FUNCTION__, 'Raw JSON: ' . $JSONString);
        $data = json_decode($JSONString, true);
        if (!is_array($data)) {
            $this->moduleDebug(__FUNCTION__, 'Ungültiges JSON vom Parent');
            return;
        }

        $dataId = isset($data['DataID']) ? (string)$data['DataID'] : '';
        if (isset($data['Topic']) && (string)$data['Topic'] !== '') {
            $inner = $data;
        } else {
            $inner = $this->normalizePayloadArray($data, $dataId);
        }

        $topic = $this->extractTopicFromPayload($inner);
        $payload = $this->extractPayloadStringFromPayload($inner);
        if ($topic === '') {
            $this->moduleDebug(__FUNCTION__, 'Kein Topic ermittelbar (DataID=' . $dataId . ')');
            return;
        }

        $this->onMqttMessage($topic, $payload);
    }

    public function onMqttMessage(string $topic, string $message): void
    {
        $this->moduleDebug(__FUNCTION__, 'Incoming topic: ' . $topic . ' payload: "' . $message . '"');
        $this->moduleDebug(__FUNCTION__, 'Payload length=' . strlen($message));

        $variableId = $this->extractVariableIdFromTopic($topic);
        if ($variableId === null) {
            $this->moduleDebug(__FUNCTION__, 'Topic ignoriert (kein gültiges <id>/set zum SubscribeTopic): ' . $topic);
            return;
        }
        $this->moduleDebug(__FUNCTION__, 'Variable id=' . $variableId);

        $targetValue = $this->parseBooleanPayload($message);
        if ($targetValue === null) {
            $this->moduleDebug(__FUNCTION__, 'Payload ignoriert (erwartet 0|1): ' . $message);
            return;
        }
        $this->moduleDebug(__FUNCTION__, 'Zielwert=' . ($targetValue ? '1' : '0'));

        $this->setVariableToBoolean($variableId, $targetValue);
    }

    private function isModuleActive(): bool
    {
        return (bool)IPS_GetProperty($this->InstanceID, 'Active');
    }

    private function hasActiveParentConnection(): bool
    {
        if (!IPS_InstanceExists($this->InstanceID)) {
            return false;
        }
        $parentId = IPS_GetInstance($this->InstanceID)['ConnectionID'];
        if ($parentId <= 0 || !IPS_InstanceExists($parentId)) {
            return false;
        }
        return IPS_GetInstance($parentId)['InstanceStatus'] === self::ST_AKTIV;
    }

    /**
     * Debug nur bei Property „Debug“, über Symcon SendDebug oder Logmeldung.
     */
    private function moduleDebug(string $context, string $message): void
    {
        if (!(bool)IPS_GetProperty($this->InstanceID, 'Debug')) {
            return;
        }
        if (method_exists($this, 'SendDebug')) {
            $this->SendDebug($context, $message, 0);
            return;
        }
        IPS_LogMessage('BISSubscriber::' . $context, $message);
    }

    private function setVariableToBoolean(int $variableId, bool $targetValue): void
    {
        $this->moduleDebug(__FUNCTION__, 'Schalten ID ' . $variableId . ' -> ' . ($targetValue ? '1' : '0'));

        if (!IPS_VariableExists($variableId)) {
            IPS_LogMessage(__CLASS__, __FUNCTION__ . ":: Variable $variableId existiert nicht");
            $this->moduleDebug(__FUNCTION__, "Variable $variableId existiert nicht");
            return;
        }

        $valueForIps = $targetValue ? 1 : 0;

        $this->moduleDebug(__FUNCTION__, 'RequestAction');
        if (@RequestAction($variableId, $valueForIps)) {
            $this->moduleDebug(__FUNCTION__, "RequestAction OK: ID $variableId -> $valueForIps");
            return;
        }
        $this->moduleDebug(__FUNCTION__, "RequestAction fehlgeschlagen für ID $variableId");

        $this->moduleDebug(__FUNCTION__, 'SetValue Fallback');
        if (@SetValue($variableId, $valueForIps)) {
            $this->moduleDebug(__FUNCTION__, "SetValue OK: ID $variableId -> $valueForIps");
            return;
        }

        $this->moduleDebug(__FUNCTION__, "SetValue fehlgeschlagen für ID $variableId");
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
        if (!isset($data['Buffer'])) {
            return $data;
        }

        $buffer = $data['Buffer'];
        if (!is_string($buffer)) {
            return $data;
        }

        $decoded = utf8_decode($buffer);
        $parsed = json_decode($decoded, true);

        if ($dataId === self::DATA_MQTT_CLIENT_CHILD_RX && is_array($parsed)) {
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
        $this->moduleDebug(__FUNCTION__, 'Normalized topic=' . $normalizedTopic);
        if (!preg_match('#^(.+)/(\d+)/set$#', $normalizedTopic, $matches)) {
            return null;
        }

        $incomingBase = $matches[1];
        $variableId = (int)$matches[2];
        $this->moduleDebug(__FUNCTION__, 'Base=' . $incomingBase . ' id=' . $variableId);

        $expectedBase = $this->getExpectedCommandBaseTopic();
        if ($expectedBase !== '' && $incomingBase !== $expectedBase) {
            $this->moduleDebug(__FUNCTION__, 'Base passt nicht, ignoriert');
            return null;
        }

        return $variableId;
    }

    private function getExpectedCommandBaseTopic(): string
    {
        $topic = $this->normalizeSubscribeTopic($this->getSubscribeTopicProperty());
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
        if (!$this->hasActiveParentConnection()) {
            $this->moduleDebug(__FUNCTION__, 'Kein Parent – Subscribe übersprungen');
            return;
        }

        if (!(bool)IPS_GetProperty($this->InstanceID, 'SubscribeOnApply')) {
            $this->moduleDebug(__FUNCTION__, 'SubscribeOnApply aus');
            return;
        }

        $topic = $this->normalizeSubscribeTopic($this->getSubscribeTopicProperty());

        $packet = json_encode(
            array(
                'DataID'  => self::DATA_MQTT_CLIENT_TX_TO_PARENT,
                'Function'=> 'Subscribe',
                'Topic'   => $topic,
            ),
            JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE
        );
        $this->moduleDebug(__FUNCTION__, 'SendDataToParent: ' . $packet);
        @$this->SendDataToParent($packet);
    }

    private function getSubscribeTopicProperty(): string
    {
        return (string)IPS_GetProperty($this->InstanceID, 'SubscribeTopic');
    }
}
