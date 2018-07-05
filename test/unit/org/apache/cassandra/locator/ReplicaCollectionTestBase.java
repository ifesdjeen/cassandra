package org.apache.cassandra.locator;

import java.net.UnknownHostException;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;

public abstract class ReplicaCollectionTestBase
{
    static final InetAddressAndPort EP1;
    static final InetAddressAndPort EP2;
    static final InetAddressAndPort EP3;

    static final Replica BROADCAST_REPLICA;
    static final Replica A;
    static final Replica B;
    static final Replica C;

    static
    {
        try
        {
            EP1 = InetAddressAndPort.getByName("127.0.0.1");
            EP2 = InetAddressAndPort.getByName("127.0.0.2");
            EP3 = InetAddressAndPort.getByName("127.0.0.3");
            BROADCAST_REPLICA = Replica.full(FBUtilities.getBroadcastAddressAndPort(), tk(3), tk(4));
            A = Replica.full(FBUtilities.getLocalAddressAndPort(), tk(0), tk(1));
            B = Replica.full(EP2, tk(1), tk(2));
            C = Replica.trans(EP3, tk(2), tk(3));
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }

    static Token tk(long t)
    {
        return new Murmur3Partitioner.LongToken(t);
    }

    static Range<Token> range(long left, long right)
    {
        return new Range<>(tk(left), tk(right));
    }


}
