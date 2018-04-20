package io.zeebe.broker.clustering.orchestration.generation;

import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.IntegerProperty;

public class IdEvent extends UnpackedObject
{
    private final IntegerProperty id = new IntegerProperty("id");

    public IdEvent()
    {
        this.declareProperty(id);
    }

    public Integer getId()
    {
        return id.getValue();
    }

    public void setId(int id)
    {
        this.id.setValue(id);
    }
}
