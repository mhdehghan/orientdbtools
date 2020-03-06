package graphdb;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.*;
import org.apache.log4j.Logger;
import java.util.*;

public class OrientdbDAO {

//    public static final Logger log = Logger.getLogger("log");
    private OrientGraph graph;
    private long vertexCount = 0;

    public long getVertexCount() {
        return vertexCount;
    }

    private long edgeCount = 0;

    public long getEdgeCount() {
        return edgeCount;
    }

    private long searchCount=0;
    public long getSearchCount()
    {
        return searchCount;
    }

    private long searchTime=0;

    public long getSearchTime() {
        return searchTime;
    }

    public OrientGraph getGraph() {
        return graph;
    }

    public OrientdbDAO(String url) {
        int cntConnection=0;
        while (true) {
            try {
                OrientdbSAO orientdbSAO = new OrientdbSAO();
                orientdbSAO.setUrl(url);
                graph = orientdbSAO.connectToDB();
                break;
            } catch (Exception e) {
                cntConnection++;
//                log.error(cntConnection + "\t" + e.getMessage(), e);
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e1) {
//                    log.error(e1.getMessage(), e1);
                    System.exit(0);
                }
            }
        }
    }


    public void shutdownGraph() {
        graph.shutdown();
    }

    public void commit() {
        try {
            graph.commit();
        } catch (Exception e) {
            graph.rollback();
            System.out.println(e.getMessage());
            throw e;
        }

    }


    public Vertex upsertVertex(String className, String keyName, Object keyValue,
                               HashMap<String, Object> properties, boolean upsert) {
        Vertex node;
        if (upsert) {
//            log.warn("Upsert is being used! Time is not calculated!");
            searchCount++;
            Iterator<Vertex> iterator = graph.getVertices(className + "." + keyName, keyValue).iterator();
            if (iterator.hasNext()) {
                node = iterator.next();
                Vertex finalNode = node;
                properties.entrySet().forEach(entry -> finalNode.setProperty(entry.getKey(), entry.getValue()));
                return finalNode;
            }
        }
        node = graph.addVertex("class:" + className, properties);
        vertexCount++;
        return node;
    }

    /***
     * Find or insert the vertex
     * @return the node
     */
    private boolean isCreated = false;

    public boolean isCreated()
    {return isCreated;}

    public Vertex fsertVertex(String className, String keyName, Object keyValue,
                              HashMap<String, Object> properties, boolean fsert) {
        isCreated=false;
        Vertex node;
        if (fsert) {
            searchCount++;
            Iterator<Vertex> iterator = graph.getVertices(className + "." + keyName, keyValue).iterator();
            if (iterator.hasNext()) {
                node = iterator.next();
                return node;
            }
        }
        node = graph.addVertex("class:" + className, properties);
        vertexCount++;
        isCreated=true;
        return node;
    }

    @Deprecated
    public Vertex upsertVertexV02(String className, String keyName, Object keyValue,
                                  HashMap<String, Object> properties, boolean upsert) {
        Vertex node;
        if (upsert) {
            searchCount++;
            Iterator<Vertex> iterator = graph.getVertices(className + "." + keyName, keyValue).iterator();
            if (iterator.hasNext()) {
                node = iterator.next();
                OrientVertex finalNode = graph.getVertex(node.getId());
                finalNode.setProperties();
                properties.entrySet().forEach(entry -> finalNode.setProperty(entry.getKey(), entry.getValue()));
                return finalNode;
            }
        }
        /*for (Object key:properties.keySet())
        {
            if(properties.get(key)==null)
                System.out.println("Null");
        }*/
        node = graph.addVertex("class:" + className, properties);
        return node;
    }

    @Deprecated
    public Vertex addNode(Map<String, Object> properties) {
        Vertex node = graph.addVertex(null);
        properties.entrySet().forEach(entry -> node.setProperty(entry.getKey(), entry.getValue()));
//        log.info("Created vertex: " + node.getId());
        return node;
    }


    //UPDATE Profile SET nick = 'Luca' UPSERT WHERE nick = 'Luca'
    //http://orientdb.com/docs/last/SQL-Update.html
    public Vertex UpsertNode(String className, String keyName, Object keyCond, Map<String, Object> properties) {
        Map<String, Object> params = new HashMap<>(properties.size());
        //properties.entrySet().forEach(entry->params.put("the"+entry.getKey(),entry.getValue()));
        String propertiesQuery = "SET ";
        for (Map.Entry<String, Object> stringObjectEntry : properties.entrySet()) {
            if (stringObjectEntry.getValue().getClass() == String.class) {
                propertiesQuery += stringObjectEntry.getKey() + " = :" + stringObjectEntry.getKey() + ", ";
            }
        }
        propertiesQuery += keyName + " = " + keyCond;

        String query = "UPDATE " + className + " " + propertiesQuery + " UPSERT WHERE " + keyName + " = " + keyCond;
//        OCommandRequest oCommandRequest= graph.command(new OCommandSQL(query).execute());
        int modified = graph.command(
                new OCommandSQL(query)).execute(params);
        return findVertices(className, keyName, keyCond).get(0);
    }




    public void removeNode(List<Vertex> nodes) {
        for (Vertex v : nodes) {
            String id = v.getId().toString();
            graph.removeVertex(v);
//            log.info("Vertex: " + id + " was removed!");
        }
    }

    //  In the case that you need to re-write this function//com.orientechnologies.orient.core.db.graph.OGraphDatabase rawGraph = graph.getRawGraph();
    public boolean edgeBetweenVertices(Vertex source, String destinationKey)
    {
        try {
            Set<String> result = source.getProperty("entity");
            if(result.contains(destinationKey))
            {return true;}
        }
        catch (Exception e)
        {
//            log.error("You do not define 'entity' in this vertex. As a result you cannot request this future.");
        }
        return false;
    }

    @Deprecated
    public Edge addEdgeNotDuplicated
            (Vertex sourceNode, Vertex distNode, String label, HashMap<String, Object> properties,String distID) {
        if(label==null)
        {
//            log.error("The label is null! Please check your edge label...");
        }
        if(edgeBetweenVertices(sourceNode,distID))
        {
            return null;
        }
        Edge edge = graph.addEdge(null, sourceNode, distNode, label);
        properties.entrySet().forEach(entry -> edge.setProperty(entry.getKey(), entry.getValue()));
//        log.info("The new edge was added " + edge.getLabel());
        edgeCount++;
        return edge;
    }

    public Edge addEdge(Vertex sourceNode, Vertex distNode, String label, HashMap<String, Object> properties) {
        if(label==null)
        {
//            log.error("The label is null! Please check your edge label...");
        }
        Edge edge = graph.addEdge(null, sourceNode, distNode, label);
        properties.entrySet().forEach(entry -> edge.setProperty(entry.getKey(), entry.getValue()));
//        log.info("The new edge was added " + edge.getLabel());
        edgeCount++;
        return edge;
    }

    //http://orientdb.com/docs/last/SQL-Create-Edge.html
    @Deprecated
    public void addEdge(String className, Vertex sourceNode, Vertex distNode) {
        String query = "CREATE EDGE " + className + " FROM " + sourceNode.getId() + " TO " + distNode.getId();
        //System.out.println(query);
        graph.command(
                new OCommandSQL(query)).execute();
//        log.info("The new edge was added between " + sourceNode.getId()+ " and "+distNode.getId());
    }

    public List<Vertex> findVertices(String className, String propertyName, Object propertyValus) {
        List<Vertex> result = new ArrayList<>();
        for (Vertex v : graph.getVertices(className + "." + propertyName, propertyValus)) {//("V.name", name)) {
//            log.info("Found vertex: " + v);
            result.add(v);
            break;//TODO: Note that! I broke the loop!
        }
        return result;
    }

    public List<ODocument> manualSql(String query) {
        List<ODocument> result = graph.getRawGraph().query(new OSQLSynchQuery("" + query));
        for (ODocument entries : result) {
//            log.info(entries.toString());
        }
        return result;
    }
}
