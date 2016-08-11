package org.apache.jackrabbit.oak.plusing.index.es.index;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Created by ieb on 25/05/2016.
 */
public class OakESConfigurationDefaults {
    public static final String PATH_FIELD_NAME = "path_exact";
    public static final String CHILD_FIELD_NAME = "path_child";
    public static final String DESC_FIELD_NAME = "path_des";
    public static final String ANC_FIELD_NAME = "path_anc";
    public static final String CATCHALL_FIELD = "catch_all";
    public static final int ROWS = Integer.MAX_VALUE;
    public static final boolean PROPERTY_RESTRICTIONS = false;
    public static final boolean PATH_RESTRICTIONS = false;
    public static final boolean PRIMARY_TYPES = false;
    public static final Collection<String> IGNORED_PROPERTIES = Collections.unmodifiableCollection(
            Arrays.asList("rep:members", "rep:authorizableId", "jcr:uuid", "rep:principalName", "rep:password"));
    public static final String TYPE_MAPPINGS = "";
    public static final String PROPERTY_MAPPINGS = "";
    public static final boolean COLLAPSE_JCR_CONTENT_NODES = false;
    public static final String COLLAPSED_PATH_FIELD = "path_collapsed";
    public static final String PATH_DEPTH_FIELD = "path_depth";
}
