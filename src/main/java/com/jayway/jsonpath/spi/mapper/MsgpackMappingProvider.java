package com.jayway.jsonpath.spi.mapper;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.TypeRef;

public class MsgpackMappingProvider
        implements MappingProvider
{
    /**
     *
     * @param source object to map
     * @param targetType the type the source object should be mapped to
     * @param configuration current configuration
     * @param <T> the mapped result type
     * @return return the mapped object
     */
    public <T> T map(Object source, Class<T> targetType, Configuration configuration)
    {
        return null;
    }

    /**
     *
     * @param source object to map
     * @param targetType the type the source object should be mapped to
     * @param configuration current configuration
     * @param <T> the mapped result type
     * @return return the mapped object
     */
    public <T> T map(Object source, TypeRef<T> targetType, Configuration configuration)
    {
        return null;
    }
}
