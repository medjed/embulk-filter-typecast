package com.jayway.jsonpath.spi.json;

import com.jayway.jsonpath.InvalidJsonException;
import org.msgpack.value.StringValue;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;

import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.msgpack.value.MapValue;
import org.msgpack.value.impl.ImmutableArrayValueImpl;
import org.msgpack.value.impl.ImmutableMapValueImpl;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class MsgpackProvider extends AbstractJsonProvider
{
    /**
     * Parse the given json string
     * @param json json string to parse
     * @return Object representation of json
     * @throws InvalidJsonException
     */
    public Object parse(String json) throws InvalidJsonException
    {
        throw new NotImplementedException();
    }

    /**
     * Parse the given json string
     * @param jsonStream input stream to parse
     * @param charset charset to use
     * @return Object representation of json
     * @throws InvalidJsonException
     */
    public Object parse(InputStream jsonStream, String charset) throws InvalidJsonException
    {
        throw new NotImplementedException();
    }

    /**
     * Convert given json object to a json string
     * @param obj object to transform
     * @return json representation of object
     */
    public String toJson(Object obj)
    {
        return ((Value) obj).toString();
    }

    /**
     * Creates a provider specific json array
     * @return new array
     */
    public Object createArray()
    {
        Value[] array = new Value[1];
        return new ImmutableArrayValueImpl(array);
    }

    /**
     * Creates a provider specific json object
     * @return new object
     */
    public Object createMap()
    {
        Value[] map = new Value[1];
        return new ImmutableMapValueImpl(map);
    }

    /**
     * checks if object is an array
     *
     * @param obj object to check
     * @return true if obj is an array
     */
    public boolean isArray(Object obj)
    {
        return ((Value) obj).isArrayValue();
    }

    /**
     * Get the length of an json array, json object or a json string
     *
     * @param obj an array or object or a string
     * @return the number of entries in the array or object
     */
    public int length(Object obj)
    {
        return ((Value) obj).asArrayValue().size();
    }

    /**
     * Converts given array to an {@link Iterable}
     *
     * @param obj an array
     * @return an Iterable that iterates over the entries of an array
     */
    public Iterable<?> toIterable(Object obj)
    {
        return ((Value) obj).asArrayValue();
    }

    /**
     * Returns the keys from the given object
     *
     * @param obj an object
     * @return the keys for an object
     */
    public Collection<String> getPropertyKeys(Object obj)
    {
        MapValue map = ((Value) obj).asMapValue();
        HashSet<String> keys = new HashSet<>();
        for (Value key : map.keySet()) {
            keys.add(key.toString());
        }
        return keys;
    }

    /**
     * Extracts a value from an array anw unwraps provider specific data type
     *
     * @param obj an array
     * @param idx index
     * @return the entry at the given index
     */
    public Object getArrayIndex(Object obj, int idx)
    {
        return ((Value) obj).asArrayValue().get(idx);
    }

    /**
     * Sets a value in an array. If the array is too small, the provider is supposed to enlarge it.
     *
     * @param array an array
     * @param idx index
     * @param newValue the new value
     */
    public void setArrayIndex(Object array, int idx, Object newValue)
    {
        //List list = ((Value) array).asArrayValue().list();
        //list.set(idx, newValue);
    }

    /**
     * Extracts a value from an map
     *
     * @param obj a map
     * @param key property key
     * @return the map entry or {@link com.jayway.jsonpath.spi.json.JsonProvider#UNDEFINED} for missing properties
     */
    public Object getMapValue(Object obj, String key)
    {
        Map m = ((Value) obj).asMapValue().map();
        StringValue k = ValueFactory.newString(key);
        if(!m.containsKey(k)){
            return JsonProvider.UNDEFINED;
        } else {
            return m.get(k);
        }
    }

    /**
     * Sets a value in an object
     *
     * @param obj   an object
     * @param key   a String key
     * @param value the value to set
     */
    public void setProperty(Object obj, Object key, Object value)
    {
        Map m = ((Value) obj).asMapValue().map();
        StringValue k = ValueFactory.newString((String) key);
        m.put(k, value);
    }

    /**
     * Removes a value in an object or array
     *
     * @param obj   an array or an object
     * @param key   a String key or a numerical index to remove
     */
    public void removeProperty(Object obj, Object key)
    {
        Map m = ((Value) obj).asMapValue().map();
        StringValue k = ValueFactory.newString((String) key);
        m.remove(k);
    }

    /**
     * checks if object is a map (i.e. no array)
     *
     * @param obj object to check
     * @return true if the object is a map
     */
    public boolean isMap(Object obj)
    {
        return ((Value) obj).isMapValue();
    }
}
