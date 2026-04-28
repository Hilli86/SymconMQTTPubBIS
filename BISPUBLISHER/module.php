<?

/**

 * @file

 *

 * MQTT Publisher (BIS): registrierte Variablen zum integrierten Symcon-MQTT-Client (Parent-Instanz).

 *

 * @author Thomas Dressler (Original), Martin Hilbert (Fork / MQTT-Client-Anbindung)

 */



class BISPublisher extends IPSModule

{

    const IPS_KERNELMESSAGE = 10100;

    const KR_READY = 10103;

    const KR_UNINIT = 10104;



    const ST_AKTIV = 102;

    const ST_INACTIV = 104;

    const ST_NOPARENT = 202;



    const VM_DELETE = 10602;

    const VM_UPDATE = 10603;



    /** MQTT QoS 0 – reicht für reines Publizieren */

    const MQTT_QOS_0_AT_MOST_ONCE = 0;



    /** Kind -> Parent: ForwardData / Publish */

    const DATA_MQTT_CLIENT_TX_TO_PARENT = '{043EA491-0325-4ADD-8FC2-A30C8EEB4D3F}';

    const MQTT_PACKET_TYPE_PUBLISH = 3;

    /** Integrierter Symcon-MQTT-Client (Gateway), Modul-{F7A0DD2E-…} */
    const MODULEID_MQTT_CLIENT_NATIVE = '{F7A0DD2E-7684-95C0-64C2-D2A9DC47577B}';



    private $fieldlist = array('TS', 'VariableID', 'VariableType', 'VariableUpdated', 'VariableChanged', 'Value', 'Path');



    private $qos = self::MQTT_QOS_0_AT_MOST_ONCE;

    private $retained = false;



    public function Create()

    {

        parent::Create();



        $this->RegisterPropertyString('Topic', 'BIS/IPS/%varid%/%varident%/%path%');

        $this->RegisterPropertyString('LogFile', '');

        $this->RegisterPropertyBoolean('Debug', false);

        $this->RegisterPropertyBoolean('Active', false);

        $this->RegisterPropertyBoolean('PublishSchnittcherBuffer', false);

        $this->RegisterPropertyString('Subscriptions', json_encode(array()));



        $vid = $this->RegisterVariableInteger('MsgID', 'MessageID', '');

        IPS_SetHidden($vid, true);



        $this->RegisterMessage(0, self::IPS_KERNELMESSAGE);

        IPS_SetName($this->InstanceID, 'BISPublisher');

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

            $this->Unregister_All();

            return;

        }



        if (!$this->hasActiveParentConnection()) {

            $this->SetStatus(self::ST_NOPARENT);

            $this->debug(__FUNCTION__, 'Kein aktiver Parent (MQTT Client). Verbindung im Objektbaum setzen.');

            return;

        }



        $this->SetStatus(self::ST_AKTIV);

        $this->Register_All();

    }



    /**

     * Von Parent (MQTT Client); reiner Publisher nutzt den Kanal nicht.

     *

     * @param string $JSONString

     */

    public function ReceiveData($JSONString)

    {

    }



    public function MessageSink($TimeStamp, $SenderID, $Message, $Data)

    {

        $this->debug(__FUNCTION__, 'entered');

        $id = $SenderID;

        $this->debug(__FUNCTION__, 'TS: ' . $TimeStamp . ' SenderID ' . $SenderID . ' MessageID ' . $Message . ' Data: ' . print_r($Data, true));

        switch ($Message) {

            case self::VM_UPDATE:

                $this->Publish($id);

                break;

            case self::VM_DELETE:

                $this->UnSubscribe($id);

                break;

            case self::IPS_KERNELMESSAGE:

                $kmsg = $Data[0];

                switch ($kmsg) {

                    case self::KR_READY:

                        IPS_LogMessage(__CLASS__, __FUNCTION__ . ' KR_Ready -> Register_All');

                        if ($this->isModuleActive() && $this->hasActiveParentConnection()) {

                            $this->SetStatus(self::ST_AKTIV);

                            $this->Register_All();

                        }

                        break;

                    case self::KR_UNINIT:

                        IPS_LogMessage(__CLASS__, __FUNCTION__ . ' KR_UNINIT');

                        break;

                    default:

                        IPS_LogMessage(__CLASS__, __FUNCTION__ . ' Kernelmessage unhandled, ID ' . $kmsg);

                        break;

                }

                break;

            default:

                IPS_LogMessage(__CLASS__, __FUNCTION__ . ' Unknown Message ' . $Message);

                break;

        }

        $this->debug(__FUNCTION__, 'leaved');

    }



    private function isModuleActive(): bool

    {

        return (bool) IPS_GetProperty($this->InstanceID, 'Active');

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
     * Modul-GUID laut IPS_GetInstance unter ModuleInfo['ModuleID'] (aktuell);
     * Fallback Root ['ModuleID'] für ältere Aufrufe.
     */
    private function getInstanceModuleId(int $instanceId): string

    {

        if ($instanceId <= 0 || !IPS_InstanceExists($instanceId)) {

            return '';

        }

        $inst = IPS_GetInstance($instanceId);

        if (isset($inst['ModuleInfo']['ModuleID']) && (string) $inst['ModuleInfo']['ModuleID'] !== '') {

            return (string) $inst['ModuleInfo']['ModuleID'];

        }

        if (isset($inst['ModuleID']) && (string) $inst['ModuleID'] !== '') {

            return (string) $inst['ModuleID'];

        }

        return '';

    }



    private function debug($topic, $data)

    {

        if (!(bool) IPS_GetProperty($this->InstanceID, 'Debug')) {

            return;

        }

        if (method_exists($this, 'SendDebug')) {

            $this->SendDebug($topic, $data, 0);

        }

    }



    private function GetLogFile()

    {

        return (string) IPS_GetProperty($this->InstanceID, 'LogFile');

    }



    private function GetTopic()

    {

        return (string) IPS_GetProperty($this->InstanceID, 'Topic');

    }



    private function GetSubscriptions()

    {

        $prop = (string) IPS_GetProperty($this->InstanceID, 'Subscriptions');

        $data = json_decode($prop, true);

        if (!is_array($data)) {

            $data = array();

        }

        $subs = array();

        foreach ($data as $vid) {

            $subs[(int) $vid] = 1;

        }

        $this->debug(__FUNCTION__, 'know ' . count($subs) . ' subscribed IDs');

        return $subs;

    }



    private function SetSubscriptions($subs)

    {

        $data = array_unique(array_keys($subs));

        $prop = json_encode(array_values($data));

        IPS_SetProperty($this->InstanceID, 'Subscriptions', $prop);

        $this->debug(__FUNCTION__, count($data) . ' subscribed IDs stored');

        IPS_ApplyChanges($this->InstanceID);

    }



    private function GetNextMsgID()

    {

        $vid = @$this->GetIDForIdent('MsgID');

        $msgid = GetValueInteger($vid);

        $msgid += 1;

        SetValueInteger($vid, $msgid);

        return $msgid;

    }



    public function Subscribe(int $id)

    {

        $this->debug(__FUNCTION__, 'entered for ID ' . $id);

        $subs = $this->GetSubscriptions();

        if (IPS_VariableExists($id)) {

            $this->RegisterMessage($id, self::VM_UPDATE);

            $this->RegisterMessage($id, self::VM_DELETE);

            if (!isset($subs[$id])) {

                $subs[$id] = 1;

                $this->SetSubscriptions($subs);

                $this->debug(__FUNCTION__, 'Variable (' . $id . ') subscribed');

            } else {

                $this->debug(__FUNCTION__, 'Variable (' . $id . ') already subscribed');

            }

        } else {

            $this->debug(__FUNCTION__, 'ID (' . $id . ') is not a variable');

        }

        $this->debug(__FUNCTION__, 'leaved');

    }



    public function Subscribe_All(int $id, string $ident = null)

    {

        $this->debug(__FUNCTION__, 'starting with ID ' . $id);

        if (IPS_VariableExists($id)) {

            if ($ident) {

                $obj = IPS_GetObject($id);

                $i = $obj['ObjectIdent'];

                $n = $obj['ObjectName'];

                if (!$i) {

                    $i = $n;

                }

                if ($i === $ident) {

                    $this->Subscribe($id);

                } else {

                    $this->debug(__FUNCTION__, " ID $id($i) not matching condition '$ident'");

                }

            } else {

                $this->Subscribe($id);

            }

        }



        if (IPS_HasChildren($id)) {

            $childs = @IPS_GetChildrenIDs($id);

            foreach ($childs as $child) {

                $this->debug(__FUNCTION__, 'found Child ID ' . $child);

                $this->Subscribe_All($child, $ident);

            }

        }

        $this->debug(__FUNCTION__, 'leaved');

    }



    public function UnSubscribe(int $id)

    {

        $this->debug(__FUNCTION__, 'entered for ID ' . $id);

        $this->UnRegister($id);

        $subs = $this->GetSubscriptions();

        if (isset($subs[$id])) {

            unset($subs[$id]);

            $this->SetSubscriptions($subs);

        } else {

            $this->debug(__FUNCTION__, "ID $id was not subscribed");

        }

        $this->debug(__FUNCTION__, 'leaved');

    }



    public function UnSubscribe_All(int $id, string $ident = null)

    {

        $this->debug(__FUNCTION__, 'starting with ID ' . $id);

        if (IPS_VariableExists($id)) {

            if ($ident) {

                $obj = IPS_GetObject($id);

                $i = $obj['ObjectIdent'];

                $n = $obj['ObjectName'];

                if (!$i) {

                    $i = $n;

                }

                if ($i === $ident) {

                    $this->UnSubscribe($id);

                } else {

                    $this->debug(__FUNCTION__, " ID $id($i) not matching condition '$ident'");

                }

            } else {

                $this->UnSubscribe($id);

            }

        }

        if (IPS_HasChildren($id)) {

            $childs = @IPS_GetChildrenIDs($id);

            foreach ($childs as $child) {

                $this->debug(__FUNCTION__, ' found Child ID' . $child);

                $this->UnSubscribe_All($child, $ident);

            }

        }

        $this->debug(__FUNCTION__, 'leaved');

    }



    public function Publish(int $id)

    {

        $this->debug(__FUNCTION__, 'entered for ID ' . $id);



        if (!$this->isModuleActive() || !$this->hasActiveParentConnection()) {

            $this->debug(__FUNCTION__, 'Publish übersprungen (inaktiv oder kein MQTT-Parent)');

            return;

        }



        $data = array();

        if (IPS_VariableExists($id)) {

            $val = GetValue($id);

            $var = IPS_GetVariable($id);

            $obj = IPS_GetObject($id);

            $ident = $obj['ObjectIdent'];

            $name = $obj['ObjectName'];

            if (!$ident) {

                $ident = $name;

            }

            if ($var['VariableType'] == 2) {

                $val = str_replace(',', '.', $val);

            }

            $path = $this->create_path(IPS_GetParent($id));

            $path = $path . $name;

            $data['Path'] = $path;

            $data['VariableID'] = $id;

            $data['VariableType'] = $var['VariableType'];

            $data['VariableUpdated'] = $var['VariableUpdated'];

            $data['VariableChanged'] = $var['VariableChanged'];

            $data['VariableIdent'] = $ident;

            $data['UTF8Value'] = utf8_encode($val);

            $data['Value'] = $val;

            $data['TS'] = time();

            $payload = json_encode($data);

            $path = str_replace(array(' '), '_', $path);

            $topic = $this->GetTopic();

            $topic = str_replace('%varid%', $id, $topic);

            $topic = str_replace('%varident%', $ident, $topic);

            $topic = str_replace('%path%', $path, $topic);

            $this->debug(__FUNCTION__, $topic . ' = ' . $payload);

            $this->GetNextMsgID();

            $this->mqtt_publish_via_parent($topic, $payload, $id);

            $this->log_data($data);

        } else {

            $this->debug(__FUNCTION__, 'Variable (' . $id . ') not found');

        }

        $this->debug(__FUNCTION__, 'leaved');

    }



    /**
     * Nativer IP-Symcon-MQTT-Client: flaches SendDataToParent-JSON (Community / Module):
     * DataID 043EA491, PacketType 3 (Publish), QualityOfService, Retain, Topic, Payload.
     * Schnittcher-MQTTClient (alt): JSON in Buffer mit utf8_encode, siehe Eigenschaft.
     */
    private function mqtt_publish_via_parent(string $topic, string $content, $objectID)

    {

        $this->debug(__FUNCTION__, "entered with topic '$topic'");

        if (!$this->hasActiveParentConnection()) {

            IPS_LogMessage(__CLASS__, __FUNCTION__ . '::Kein aktiver MQTT-Client');

            $this->debug(__FUNCTION__, 'Kein aktiver MQTT-Client');

            return;

        }

        $parentId = (int) IPS_GetInstance($this->InstanceID)['ConnectionID'];

        $parentModuleId = $this->getInstanceModuleId($parentId);

        $this->debug(__FUNCTION__, 'Parent InstanceID=' . $parentId . ' ModuleID=' . $parentModuleId);

        if ($parentModuleId !== '' && $parentModuleId !== self::MODULEID_MQTT_CLIENT_NATIVE) {

            $this->debug(__FUNCTION__, 'Hinweis: Parent-Modul-ID ist nicht der integrierte MQTT-Client (' . self::MODULEID_MQTT_CLIENT_NATIVE . '), sondern: ' . $parentModuleId);

        }

        if ((bool) IPS_GetProperty($this->InstanceID, 'PublishSchnittcherBuffer')) {

            $innerPayload = array(
                'Topic'   => $topic,
                'Payload' => $content,
                'Retain'  => $this->retained ? 1 : 0,
                'QoS'     => (int) $this->qos,
            );

            $inner = json_encode($innerPayload, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);

            $packet = json_encode(
                array(
                    'DataID' => self::DATA_MQTT_CLIENT_TX_TO_PARENT,
                    'Buffer' => utf8_encode($inner),
                ),
                JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE
            );

        } else {

            $packet = json_encode(
                array(
                    'DataID'           => self::DATA_MQTT_CLIENT_TX_TO_PARENT,
                    'PacketType'       => self::MQTT_PACKET_TYPE_PUBLISH,
                    'QualityOfService' => (int) $this->qos,
                    'Retain'           => (bool) $this->retained,
                    'Topic'            => $topic,
                    'Payload'          => $content,
                ),
                JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE
            );

        }

        $this->debug(__FUNCTION__, 'SendDataToParent: ' . $packet);

        $result = $this->SendDataToParent($packet);

        $this->debug(__FUNCTION__, 'SendDataToParent Rueckgabe: ' . print_r($result, true));

        $this->debug(__FUNCTION__, 'leaved');

    }



    private function Register_All()

    {

        $this->debug(__FUNCTION__, 'entered');

        $subs = $this->GetSubscriptions();

        foreach ($subs as $vid => $value) {

            $this->Register($vid);

        }

        $this->debug(__FUNCTION__, 'leaved');

    }



    private function Register($id)

    {

        $this->debug(__FUNCTION__, 'entered for ID ' . $id);

        if (IPS_VariableExists($id)) {

            $this->debug(__FUNCTION__, 'register ' . $id);

            $this->RegisterMessage($id, self::VM_UPDATE);

            $this->RegisterMessage($id, self::VM_DELETE);

        } else {

            $this->debug(__FUNCTION__, 'Variable (' . $id . ') not found');

        }

        $this->debug(__FUNCTION__, 'leaved');

    }



    private function UnRegister($id)

    {

        $this->debug(__FUNCTION__, 'entered');

        if (IPS_VariableExists($id)) {

            $this->debug(__FUNCTION__, 'unregister ' . $id);

            $this->UnRegisterMessage($id, self::VM_UPDATE);

            $this->UnRegisterMessage($id, self::VM_DELETE);

        } else {

            $this->debug(__FUNCTION__, 'Variable (' . $id . ') not found');

        }

        $this->debug(__FUNCTION__, 'leaved');

    }



    private function Unregister_All()

    {

        $this->debug(__FUNCTION__, 'entered');

        $subs = $this->GetSubscriptions();

        foreach ($subs as $vid => $value) {

            $this->UnRegister($vid);

        }

        $this->debug(__FUNCTION__, 'leaved');

    }



    private function create_path($id)

    {

        $this->debug(__FUNCTION__, "entered for ID $id");

        $path = '';

        do {

            $obj = IPS_GetObject($id);

            $name = $obj['ObjectName'];

            $path = $name . '/' . $path;

            $id = IPS_GetParent($id);

        } while ($id > 0);

        $this->debug(__FUNCTION__, 'Path=' . $path);

        return $path;

    }



    private function log_data($data)

    {

        $this->debug(__FUNCTION__, 'entered');

        $fname = $this->GetLogFile();

        if ($fname > '') {

            $this->log2file($fname, $data);

        }

        $this->debug(__FUNCTION__, 'leaved');

    }



    private function log2file($fname, $data)

    {

        $this->debug(__FUNCTION__, 'entered');

        if ($fname == '') {

            return;

        }

        $this->debug(__FUNCTION__, 'write to file:' . $fname);

        $exists = file_exists($fname);

        $o = @fopen($fname, 'a');

        if (!$o) {

            IPS_LogMessage(__CLASS__, __FUNCTION__ . '::Cannot open ' . $fname);

            $this->debug(__FUNCTION__, 'Cannot open ' . $fname);

            return;

        }

        $header = implode(';', $this->fieldlist);

        if (!$exists) {

            $this->debug(__FUNCTION__, 'write header to New file');

            fwrite($o, $header . "\r\n");

        }



        $line = '';

        for ($f = 0; $f < count($this->fieldlist); $f++) {

            $field = $this->fieldlist[$f];

            if (isset($data[$field])) {

                $val = $data[$field];

                $line .= $val;

            }

            $line .= ';';

        }

        $line .= "\r\n";

        fwrite($o, $line);

        fclose($o);

        $this->debug(__FUNCTION__, 'leaved');

    }

}

