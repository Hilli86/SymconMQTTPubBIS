<?
/**
 * Internal placeholder module to keep Symcon module validation happy
 * for the shared lib directory. Do not instantiate this module.
 */
class BISInternalLib extends IPSModule
{
    public function Create()
    {
        parent::Create();
    }

    public function ApplyChanges()
    {
        parent::ApplyChanges();
        $this->SetStatus(IS_INACTIVE);
    }
}
