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
        $this->RegisterPropertyString('SubscribeTopic', 'bis/IPS/#');

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
}
