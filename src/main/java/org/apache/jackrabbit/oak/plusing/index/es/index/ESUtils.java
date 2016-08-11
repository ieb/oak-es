package org.apache.jackrabbit.oak.plusing.index.es.index;

import javax.jcr.PropertyType;

/**
 * Created by ieb on 24/05/2016.
 */
public class ESUtils {

    public static String getSortingField(int tag, String propertyName) {
        switch (tag) {
            case PropertyType.BINARY:
                return propertyName + "_binary_sort";
            case PropertyType.DOUBLE:
                return propertyName + "_double_sort";
            case PropertyType.DECIMAL:
                return propertyName + "_double_sort";
            default:
                return propertyName + "_string_sort";
        }
    }

}
