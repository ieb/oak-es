package org.apache.jackrabbit.oak.plusing.index.es.index;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import sun.jvm.hotspot.interpreter.Interpreter;

import javax.naming.directory.Attributes;
import java.util.Collection;
import java.util.Set;

/**
 * Created by ieb on 24/05/2016.
 */
public interface ESConfiguration {

    String getPathField();

    String getPathDepthField();

    boolean collapseJcrContentNodes();

    String getCollapsedPathField();

    Collection<String> getUsedProperties();

    Collection<String> getIgnoredProperties();

    String getFieldNameFor(Type<?> type);

    String getIndexName();
}
