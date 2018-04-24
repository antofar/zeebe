package io.zeebe.broker.clustering.orchestration;

import io.zeebe.util.buffer.BufferUtil;
import org.agrona.DirectBuffer;

import java.util.Objects;

public class TopicName
{
    private final DirectBuffer name;

    public TopicName(final DirectBuffer name)
    {
        this.name = BufferUtil.cloneBuffer(name);
    }

    public DirectBuffer getName()
    {
        return name;
    }

    @Override
    public boolean equals(final Object o)
    {

        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            if (o instanceof  DirectBuffer)
            {
                return BufferUtil.equals(getName(), (DirectBuffer) o);
            }
            return false;
        }

        final TopicName topicName = (TopicName) o;
        return BufferUtil.equals(getName(), topicName.getName());
    }

    @Override
    public String toString()
    {
        return "TopicName{" + "name=" + BufferUtil.bufferAsString(name) + '}';
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name);
    }
}
