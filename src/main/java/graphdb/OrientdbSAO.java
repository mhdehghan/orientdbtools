package graphdb;

import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import org.apache.log4j.Logger;

public class OrientdbSAO {
//    public static final Logger log = Logger.getLogger("log");

    private String url;
    private OrientGraph graph;
    private OrientGraphNoTx graphNoTx;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public OrientGraph connectToDB() {
        graph = new OrientGraph(url,"admin","admin");
        return graph;
    }

    public OrientGraphNoTx connectToDB_NoTx() {
        graphNoTx = new OrientGraphNoTx(url);
        return graphNoTx;
    }

    public void shutdownGraph() {
        graph.shutdown();
    }

    public void commitGraph() {
        graph.commit();
    }

}
