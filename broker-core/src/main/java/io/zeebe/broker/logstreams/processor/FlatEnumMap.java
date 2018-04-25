package io.zeebe.broker.logstreams.processor;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class FlatEnumMap<V>
{
    private final Object[] elements;

    private final int enum2Cardinality;
    private final int enum3Cardinality;

    public <R extends Enum<R>, S extends Enum<S>, T extends Enum<T>> FlatEnumMap(
            Class<R> enum1,
            Class<S> enum2,
            Class<T> enum3)
    {

        this.enum2Cardinality = enum2.getEnumConstants().length;
        this.enum3Cardinality = enum3.getEnumConstants().length;

        final int cardinality =
                enum1.getEnumConstants().length
                * enum2Cardinality
                * enum3Cardinality;
        this.elements = new Object[cardinality];
    }

    public V get(Enum key1, Enum key2, Enum key3)
    {
        final int index = mapToIndex(key1, key2, key3);
        return (V) elements[index];
    }

    public void put(Enum key1, Enum key2, Enum key3, V value)
    {
        final int index = mapToIndex(key1, key2, key3);
        elements[index] = value;
    }

    public boolean containsKey(Enum key1, Enum key2, Enum key3)
    {
        final int index = mapToIndex(key1, key2, key3);
        return elements[index] != null;
    }

    private int mapToIndex(Enum key1, Enum key2, Enum key3)
    {
        return (key1.ordinal() * enum2Cardinality * enum3Cardinality) + (key2.ordinal() * enum3Cardinality);
    }

}