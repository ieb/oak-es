package org.apache.jackrabbit.oak.plusing.index.es.index.take2;

import com.google.common.collect.Iterables;
import com.google.common.io.CountingInputStream;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.commons.io.LazyInputStream;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateCallback;
import org.apache.jackrabbit.oak.plugins.index.fulltext.ExtractedText;
import org.apache.jackrabbit.oak.plugins.memory.StringPropertyState;
import org.apache.jackrabbit.oak.plusing.index.es.index.DrainingFutureQueue;
import org.apache.jackrabbit.oak.plusing.index.es.index.ESServer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.util.BlobByteSource;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.WriteOutContentHandler;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;

import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;

/**
 * Created by ieb on 25/05/2016.
 */
public class ESIndexEditorContext {
    private final NodeState root;
    private final ESServer esServer;
    private NodeBuilder definition;
    private final IndexUpdateCallback updateCallback;
    private ExtractedTextCache extractedTextCache;
    private final IndexAugmentorFactory augmentorFactory;
    private final DrainingFutureQueue<ActionFuture<? extends ActionResponse>> responseQueue;
    private boolean reindex;
    private int indexedNodes = 0;

    public ESIndexEditorContext(ESServer esServer,
                                NodeState root,
                                NodeBuilder definition,
                                IndexUpdateCallback updateCallback,
                                ExtractedTextCache extractedTextCache,
                                IndexAugmentorFactory augmentorFactory,
                                DrainingFutureQueue<ActionFuture<? extends ActionResponse>> responseQueue) {
        this.esServer = esServer;
        this.root = root;
        this.definition = definition;
        this.updateCallback = updateCallback;
        this.extractedTextCache = extractedTextCache;
        this.augmentorFactory = augmentorFactory;
        this.responseQueue = responseQueue;
    }


    public IndexDefinition getDefinition() {
        return definition;
    }

    public void enableReindexMode() {

    }

    public ExtractedTextCache getExtractedTextCache() {
        return extractedTextCache;
    }

    public boolean isReindex() {
        return reindex;
    }

    public void recordTextExtractionStats(long l, long bytesRead, int length) {

    }

    public long incIndexedNodes() {
        return indexedNodes++;
    }

    public int getIndexedNodes() {
        return indexedNodes;
    }

    public Client getClient() {
        return esServer.getClient();
    }

    public void queueDeleteFromPath(String path) {
        responseQueue.add(getClient().delete(getDeleteRequestFromPath(path)));
    }

    public void queueDelete(DeleteRequest delete) {
        responseQueue.add(getClient().delete(delete));
    }

    public void queueIndex() {
        responseQueue.add(getClient().index(getIndexRequest()));
    }

    public IndexRequest getIndexRequest() {
        return indexRequest;
    }


    public void indexUpdate() throws CommitFailedException {
        updateCallback.indexUpdate();
    }

    public boolean addOrUpdate(String path, NodeState state, boolean exists, IndexDefinition.IndexingRule indexingRule) {

        boolean facet = false;
        boolean dirty = false;

        //We 'intentionally' are indexing node names only on root state as we don't support indexing relative or
        //regex for node name indexing
        PropertyState nodenamePS =
                new StringPropertyState(FieldNames.NODE_NAME, getName(path));
        for (PropertyState property : Iterables.concat(state.getProperties(), Collections.singleton(nodenamePS))) {
            String pname = property.getName();

            if (!isVisible(pname) && !FieldNames.NODE_NAME.equals(pname)) {
                continue;
            }

            PropertyDefinition pd = indexingRule.getConfig(pname);

            if (pd == null || !pd.index){
                continue;
            }

            if (pd.ordered) {
                dirty |= addTypedOrderedFields(property, pname, pd);
            }

            dirty |= indexProperty(path, state, property, pname, pd);

            facet |= pd.facet;
        }

        dirty |= indexAggregates(path, state);
        dirty |= indexNullCheckEnabledProps(path, state);
        dirty |= indexNotNullCheckEnabledProps(path, state);

        dirty |= augmentCustomFields(path, state);

        // Check if a node having a single property was modified/deleted
        if (!dirty) {
            dirty = indexIfSinglePropertyRemoved();
        }

        if (isUpdate && !dirty) {
            // updated the state but had no relevant changes
            return false;
        }

        String name = getName(path);
        if (indexingRule.isNodeNameIndexed()){
            addNodeNameField(name);
            dirty = true;
        }

        //For property index no use making an empty document if
        //none of the properties are indexed
        if(!indexingRule.indexesAllNodesOfMatchingType() && !dirty){
            return false;
        }

        document.add(newPathField(path));


        if (indexingRule.isFulltextEnabled()) {
            document.add(newFulltextField(name));
        }

        if (getDefinition().evaluatePathRestrictions()){
            document.add(newAncestorsField(PathUtils.getParentPath(path)));
            document.add(newDepthField(path));
        }

        // because of LUCENE-5833 we have to merge the suggest fields into a single one
        Field suggestField = null;
        for (Field f : fields) {
            if (FieldNames.SUGGEST.equals(f.name())) {
                if (suggestField == null) {
                    suggestField = FieldFactory.newSuggestField(f.stringValue());
                } else {
                    suggestField = FieldFactory.newSuggestField(suggestField.stringValue(), f.stringValue());
                }
            } else {
                document.add(f);
            }
        }
        if (suggestField != null) {
            document.add(suggestField);
        }

        if (facet) {
            document = context.getFacetsConfig().build(document);
        }

        //TODO Boost at document level

        // Add the information to the context ready for indexing.
        return true;
    }

    private boolean addFacetFields(PropertyState property, String pname, PropertyDefinition pd) {
        String facetFieldName = FieldNames.createFacetFieldName(pname);
        context.getFacetsConfig().setIndexFieldName(pname, facetFieldName);
        int tag = property.getType().tag();
        int idxDefinedTag = pd.getType();
        // Try converting type to the defined type in the index definition
        if (tag != idxDefinedTag) {
            LOGGER.debug("[{}] Facet property defined with type {} differs from property {} with type {} in "
                            + "path {}",
                    getIndexName(),
                    Type.fromTag(idxDefinedTag, false), property.toString(),
                    Type.fromTag(tag, false), getPath());
            tag = idxDefinedTag;
        }

        boolean fieldAdded = false;
        try {
            if (tag == Type.STRINGS.tag() && property.isArray()) {
                context.getFacetsConfig().setMultiValued(pname, true);
                Iterable<String> values = property.getValue(Type.STRINGS);
                for (String value : values) {
                    if (value != null && value.length() > 0) {
                        context.addSortedSetDocValuesFacetField(pname, value);
                    }
                }
                fieldAdded = true;
            } else if (tag == Type.STRING.tag()) {
                String value = property.getValue(Type.STRING);
                if (value.length() > 0) {
                    context.addSortedSetDocValuesFacetField(pname, value);
                    fieldAdded = true;
                }
            }

        } catch (Throwable e) {
            LOGGER.warn("[{}] Ignoring facet property. Could not convert property {} of type {} to type {} for path {}",
                    getIndexName(), pname,
                    Type.fromTag(property.getType().tag(), false),
                    Type.fromTag(tag, false), getPath(), e);
        }
        return fieldAdded;
    }


    private boolean indexProperty(String path,
                                  List<Field> fields,
                                  NodeState state,
                                  PropertyState property,
                                  String pname,
                                  PropertyDefinition pd) {
        boolean includeTypeForFullText = indexingRule.includePropertyType(property.getType().tag());

        boolean dirty = false;
        if (Type.BINARY.tag() == property.getType().tag()
                && includeTypeForFullText) {
            fields.addAll(newBinary(property, state, null, path + "@" + pname));
            dirty = true;
        } else {
            if (pd.propertyIndex && pd.includePropertyType(property.getType().tag())) {
                dirty |= addTypedFields(fields, property, pname);
            }

            if (pd.fulltextEnabled() && includeTypeForFullText) {
                for (String value : property.getValue(Type.STRINGS)) {
                    if (pd.analyzed && pd.includePropertyType(property.getType().tag())) {
                        String analyzedPropName = constructAnalyzedPropertyName(pname);
                        fields.add(newPropertyField(analyzedPropName, value, !pd.skipTokenization(pname), pd.stored));
                    }

                    if (pd.useInSuggest) {
                        fields.add(FieldFactory.newSuggestField(value));
                    }

                    if (pd.useInSpellcheck) {
                        fields.add(newPropertyField(FieldNames.SPELLCHECK, value, true, false));
                    }

                    if (pd.nodeScopeIndex) {
                        Field field = newFulltextField(value);
                        fields.add(field);
                    }
                    dirty = true;
                }
            }
            if (pd.facet) {
                dirty |= addFacetFields(fields, property, pname, pd);
            }

        }

        return dirty;
    }


    private String constructAnalyzedPropertyName(String pname) {
        if (context.getDefinition().getVersion().isAtLeast(IndexFormatVersion.V2)){
            return FieldNames.createAnalyzedFieldName(pname);
        }
        return pname;
    }

    private boolean addTypedFields(List<Field> fields, PropertyState property, String pname) {
        int tag = property.getType().tag();
        boolean fieldAdded = false;
        for (int i = 0; i < property.count(); i++) {
            Field f;
            if (tag == Type.LONG.tag()) {
                f = new LongField(pname, property.getValue(Type.LONG, i), Field.Store.NO);
            } else if (tag == Type.DATE.tag()) {
                String date = property.getValue(Type.DATE, i);
                f = new LongField(pname, FieldFactory.dateToLong(date), Field.Store.NO);
            } else if (tag == Type.DOUBLE.tag()) {
                f = new DoubleField(pname, property.getValue(Type.DOUBLE, i), Field.Store.NO);
            } else if (tag == Type.BOOLEAN.tag()) {
                f = new StringField(pname, property.getValue(Type.BOOLEAN, i).toString(), Field.Store.NO);
            } else {
                f = new StringField(pname, property.getValue(Type.STRING, i), Field.Store.NO);
            }

            fields.add(f);
            fieldAdded = true;
        }
        return fieldAdded;
    }



    private boolean addTypedOrderedFields(List<Field> fields,
                                          PropertyState property,
                                          String pname,
                                          PropertyDefinition pd) {
        // Ignore and warn if property multi-valued as not supported
        if (property.getType().isArray()) {
            LOGGER.warn(
                    "[{}] Ignoring ordered property {} of type {} for path {} as multivalued ordered property not supported",
                    getIndexName(), pname,
                    Type.fromTag(property.getType().tag(), true), getPath());
            return false;
        }

        int tag = property.getType().tag();
        int idxDefinedTag = pd.getType();
        // Try converting type to the defined type in the index definition
        if (tag != idxDefinedTag) {
            LOGGER.debug(
                    "[{}] Ordered property defined with type {} differs from property {} with type {} in "
                            + "path {}",
                    getIndexName(),
                    Type.fromTag(idxDefinedTag, false), property.toString(),
                    Type.fromTag(tag, false), getPath());
            tag = idxDefinedTag;
        }

        String name = FieldNames.createDocValFieldName(pname);
        boolean fieldAdded = false;
        Field f = null;
        try {
            if (tag == Type.LONG.tag()) {
                //TODO Distinguish fields which need to be used for search and for sort
                //If a field is only used for Sort then it can be stored with less precision
                f = new NumericDocValuesField(name, property.getValue(Type.LONG));
            } else if (tag == Type.DATE.tag()) {
                String date = property.getValue(Type.DATE);
                f = new NumericDocValuesField(name, FieldFactory.dateToLong(date));
            } else if (tag == Type.DOUBLE.tag()) {
                f = new DoubleDocValuesField(name, property.getValue(Type.DOUBLE));
            } else if (tag == Type.BOOLEAN.tag()) {
                f = new SortedDocValuesField(name,
                        new BytesRef(property.getValue(Type.BOOLEAN).toString()));
            } else if (tag == Type.STRING.tag()) {
                f = new SortedDocValuesField(name,
                        new BytesRef(property.getValue(Type.STRING)));
            }

            if (f != null) {
                fields.add(f);
                fieldAdded = true;
            }
        } catch (Exception e) {
            LOGGER.warn(
                    "[{}] Ignoring ordered property. Could not convert property {} of type {} to type {} for path {}",
                    getIndexName(), pname,
                    Type.fromTag(property.getType().tag(), false),
                    Type.fromTag(tag, false), getPath(), e);
        }
        return fieldAdded;
    }


    private static boolean isVisible(String name) {
        return name.charAt(0) != ':';
    }


    private List<Field> newBinary(
            PropertyState property, NodeState state, String nodePath, String path) {
        List<Field> fields = new ArrayList<Field>();
        Metadata metadata = new Metadata();

        //jcr:mimeType is mandatory for a binary to be indexed
        String type = state.getString(JcrConstants.JCR_MIMETYPE);

        if (type == null || !isSupportedMediaType(type)) {
            LOGGER.trace(
                    "[{}] Ignoring binary content for node {} due to unsupported (or null) jcr:mimeType [{}]",
                    getIndexName(), path, type);
            return fields;
        }

        metadata.set(Metadata.CONTENT_TYPE, type);
        if (JCR_DATA.equals(property.getName())) {
            String encoding = state.getString(JcrConstants.JCR_ENCODING);
            if (encoding != null) { // not mandatory
                metadata.set(Metadata.CONTENT_ENCODING, encoding);
            }
        }

        for (Blob v : property.getValue(Type.BINARIES)) {
            String value = parseStringValue(v, metadata, path, property.getName());
            if (value == null){
                continue;
            }

            if (nodePath != null){
                fields.add(newFulltextField(nodePath, value, true));
            } else {
                fields.add(newFulltextField(value, true));
            }
        }
        return fields;
    }


    private boolean augmentCustomFields(final String path, final List<Field> fields,
                                        final NodeState document) {
        boolean dirty = false;

        IndexAugmentorFactory augmentorFactory = context.getAugmentorFactory();
        if (augmentorFactory != null) {
            IndexDefinition defn = getDefinition();
            Iterable<Field> augmentedFields = augmentorFactory
                    .getIndexFieldProvider(indexingRule.getNodeTypeName())
                    .getAugmentedFields(path, document, defn.getDefinitionNodeState());

            for (Field field : augmentedFields) {
                fields.add(field);
                dirty = true;
            }
        }

        return dirty;
    }


    private boolean indexNotNullCheckEnabledProps(String path, List<Field> fields, NodeState state) {
        boolean fieldAdded = false;
        for (PropertyDefinition pd : indexingRule.getNotNullCheckEnabledProperties()) {
            if (isPropertyNotNull(state, pd)) {
                fields.add(new StringField(FieldNames.NOT_NULL_PROPS, pd.name, Field.Store.NO));
                fieldAdded = true;
            }
        }
        return fieldAdded;
    }

    private boolean indexNullCheckEnabledProps(String path, List<Field> fields, NodeState state) {
        boolean fieldAdded = false;
        for (PropertyDefinition pd : indexingRule.getNullCheckEnabledProperties()) {
            if (isPropertyNull(state, pd)) {
                fields.add(new StringField(FieldNames.NULL_PROPS, pd.name, Field.Store.NO));
                fieldAdded = true;
            }
        }
        return fieldAdded;
    }

    private boolean indexIfSinglePropertyRemoved() {
        boolean dirty = false;
        for (PropertyState ps : propertiesModified) {
            PropertyDefinition pd = indexingRule.getConfig(ps.getName());
            if (pd != null
                    && pd.index
                    && (pd.includePropertyType(ps.getType().tag())
                    || indexingRule.includePropertyType(ps.getType().tag()))) {
                dirty = true;
                break;
            }
        }
        return dirty;
    }

    /**
     * Determine if the property as defined by PropertyDefinition exists or not.
     *
     * <p>For relative property if the intermediate nodes do not exist then property is
     * <bold>not</bold> considered to be null</p>
     *
     * @return true if the property does not exist
     */
    private boolean isPropertyNull(NodeState state, PropertyDefinition pd){
        NodeState propertyNode = getPropertyNode(state, pd);
        if (!propertyNode.exists()){
            return false;
        }
        return !propertyNode.hasProperty(pd.nonRelativeName);
    }

    /**
     * Determine if the property as defined by PropertyDefinition exists or not.
     *
     * <p>For relative property if the intermediate nodes do not exist then property is
     * considered to be null</p>
     *
     * @return true if the property exists
     */
    private boolean isPropertyNotNull(NodeState state, PropertyDefinition pd){
        NodeState propertyNode = getPropertyNode(state, pd);
        if (!propertyNode.exists()){
            return false;
        }
        return propertyNode.hasProperty(pd.nonRelativeName);
    }

    private static NodeState getPropertyNode(NodeState nodeState, PropertyDefinition pd) {
        if (!pd.relative){
            return nodeState;
        }
        NodeState node = nodeState;
        for (String name : pd.ancestors) {
            node = node.getChildNode(name);
        }
        return node;
    }


    /**
     * Extracts the local name of the current node ignoring any namespace prefix
     *
     * @param name node name
     */
    private static void addNodeNameField(List<Field> fields, String name) {
        //TODO Need to check if it covers all cases
        int colon = name.indexOf(':');
        String value = colon < 0 ? name : name.substring(colon + 1);

        //For now just add a single term. Later we can look into using different analyzer
        //to analyze the node name and add multiple terms. Like add multiple terms for a
        //cameCase file name to allow faster like search
        fields.add(new StringField(FieldNames.NODE_NAME, value, Field.Store.NO));
    }


    private boolean indexAggregates(final String path, final List<Field> fields,
                                    final NodeState state) {
        final AtomicBoolean dirtyFlag = new AtomicBoolean();
        indexingRule.getAggregate().collectAggregates(state, new Aggregate.ResultCollector() {
            @Override
            public void onResult(Aggregate.NodeIncludeResult result) {
                boolean dirty = indexAggregatedNode(path, fields, result);
                if (dirty) {
                    dirtyFlag.set(true);
                }
            }

            @Override
            public void onResult(Aggregate.PropertyIncludeResult result) {
                boolean dirty = false;
                if (result.pd.ordered) {
                    dirty |= addTypedOrderedFields(fields, result.propertyState,
                            result.propertyPath, result.pd);
                }
                dirty |= indexProperty(path, fields, state, result.propertyState,
                        result.propertyPath, result.pd);

                if (dirty) {
                    dirtyFlag.set(true);
                }
            }
        });
        return dirtyFlag.get();
    }

    /**
     * Create the fulltext field from the aggregated nodes. If result is for aggregate for a relative node
     * include then
     * @param path current node path
     * @param fields indexed fields
     * @param result aggregate result
     * @return true if a field was created for passed node result
     */
    private boolean indexAggregatedNode(String path, List<Field> fields, Aggregate.NodeIncludeResult result) {
        //rule for node being aggregated might be null if such nodes
        //are not indexed on there own. In such cases we rely in current
        //rule for some checks
        IndexDefinition.IndexingRule ruleAggNode = context.getDefinition()
                .getApplicableIndexingRule(getPrimaryTypeName(result.nodeState));
        boolean dirty = false;

        for (PropertyState property : result.nodeState.getProperties()){
            String pname = property.getName();
            String propertyPath = PathUtils.concat(result.nodePath, pname);

            if (!isVisible(pname)) {
                continue;
            }

            //Check if type is indexed
            int type = property.getType().tag();
            if (ruleAggNode != null ) {
                if (!ruleAggNode.includePropertyType(type)) {
                    continue;
                }
            } else if (!indexingRule.includePropertyType(type)){
                continue;
            }

            //Check if any explicit property defn is defined via relative path
            // and is marked to exclude this property from being indexed. We exclude
            //it from aggregation if
            // 1. Its not to be indexed i.e. index=false
            // 2. Its explicitly excluded from aggregation i.e. excludeFromAggregation=true
            PropertyDefinition pdForRootNode = indexingRule.getConfig(propertyPath);
            if (pdForRootNode != null && (!pdForRootNode.index || pdForRootNode.excludeFromAggregate)) {
                continue;
            }

            if (Type.BINARY == property.getType()) {
                String aggreagtedNodePath = PathUtils.concat(path, result.nodePath);
                //Here the fulltext is being created for aggregate root hence nodePath passed
                //should be null
                String nodePath = result.isRelativeNode() ? result.rootIncludePath : null;
                fields.addAll(newBinary(property, result.nodeState, nodePath, aggreagtedNodePath + "@" + pname));
                dirty = true;
            } else {
                PropertyDefinition pd = null;
                if (ruleAggNode != null){
                    pd = ruleAggNode.getConfig(pname);
                }

                if (pd != null && !pd.nodeScopeIndex){
                    continue;
                }

                for (String value : property.getValue(Type.STRINGS)) {
                    Field field = result.isRelativeNode() ?
                            newFulltextField(result.rootIncludePath, value) : newFulltextField(value) ;
                    if (pd != null) {
                        field.setBoost(pd.boost);
                    }
                    fields.add(field);
                    dirty = true;
                }
            }
        }
        return dirty;
    }


    public boolean isSupportedMediaType(String type) {
        return false;
    }



    private String parseStringValue(Blob v, Metadata metadata, String path, String propertyName) {
        String text = context.getExtractedTextCache().get(path, propertyName, v, context.isReindex());
        if (text == null){
            text = parseStringValue0(v, metadata, path);
        }
        return text;
    }

    private String parseStringValue0(Blob v, Metadata metadata, String path) {
        WriteOutContentHandler handler = new WriteOutContentHandler(context.getDefinition().getMaxExtractLength());
        long start = System.currentTimeMillis();
        long bytesRead = 0;
        try {
            CountingInputStream stream = new CountingInputStream(new LazyInputStream(new BlobByteSource(v)));
            try {
                context.getParser().parse(stream, handler, metadata, new ParseContext());
            } finally {
                bytesRead = stream.getCount();
                stream.close();
            }
        } catch (LinkageError e) {
            // Capture and ignore errors caused by extraction libraries
            // not being present. This is equivalent to disabling
            // selected media types in configuration, so we can simply
            // ignore these errors.
        } catch (Throwable t) {
            // Capture and report any other full text extraction problems.
            // The special STOP exception is used for normal termination.
            if (!handler.isWriteLimitReached(t)) {
                LOGGER.debug(
                        "[{}] Failed to extract text from a binary property: {}."
                                + " This is a fairly common case, and nothing to"
                                + " worry about. The stack trace is included to"
                                + " help improve the text extraction feature.",
                        getIndexName(), path, t);
                context.getExtractedTextCache().put(v, ExtractedText.ERROR);
                return TEXT_EXTRACTION_ERROR;
            }
        }
        String result = handler.toString();
        if (bytesRead > 0) {
            context.recordTextExtractionStats(System.currentTimeMillis() - start, bytesRead, result.length());
        }
        context.getExtractedTextCache().put(v,  new ExtractedText(ExtractedText.ExtractionResult.SUCCESS, result));
        return result;
    }

}
