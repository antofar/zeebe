package io.zeebe.broker.clustering.orchestration.generation;

import static io.zeebe.broker.workflow.data.WorkflowInstanceEvent.PROP_STATE;

import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.EnumProperty;
import io.zeebe.msgpack.property.IntegerProperty;

public class IdEvent extends UnpackedObject
{

    private final EnumProperty<IdEventState> stateProp = new EnumProperty<>(PROP_STATE, IdEventState.class);

    private final IntegerProperty id = new IntegerProperty("id");

    public IdEvent()
    {
        this.declareProperty(stateProp).declareProperty(id);
    }

    public Integer getId()
    {
        return id.getValue();
    }

    public void setId(int id)
    {
        this.id.setValue(id);
    }


    public IdEventState getState()
    {
        return stateProp.getValue();
    }

    public void setState(IdEventState state)
    {
        stateProp.setValue(state);
    }
}
