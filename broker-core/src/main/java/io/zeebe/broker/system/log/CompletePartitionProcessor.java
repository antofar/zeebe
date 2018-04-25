/*
 * Zeebe Broker Core
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.zeebe.broker.system.log;

import io.zeebe.broker.logstreams.processor.*;
import io.zeebe.broker.system.log.PendingPartitionsIndex.PendingPartition;
import io.zeebe.protocol.clientapi.Intent;

public class CompletePartitionProcessor implements TypedRecordProcessor<PartitionEvent>
{
    private boolean success;

    protected final PendingPartitionsIndex partitions;

    public CompletePartitionProcessor(PendingPartitionsIndex partitions)
    {
        this.partitions = partitions;
    }

    @Override
    public void processRecord(TypedRecord<PartitionEvent> event)
    {
        final PartitionEvent value = event.getValue();
        final PendingPartition partition = partitions.get(value.getPartitionId());
        success = partition != null;
    }

    @Override
    public boolean executeSideEffects(TypedRecord<PartitionEvent> event, TypedResponseWriter responseWriter)
    {
        return true;
    }

    @Override
    public long writeRecord(TypedRecord<PartitionEvent> event, TypedStreamWriter writer)
    {
        if (success)
        {
            return writer.writeEvent(event.getKey(), Intent.CREATED, event.getValue());
        }
        else
        {
            return writer.writeRejection(event);
        }
    }

    @Override
    public void updateState(TypedRecord<PartitionEvent> event)
    {
        final PartitionEvent value = event.getValue();

        if (success)
        {
            partitions.removePartitionKey(value.getPartitionId());
        }
    }

}
