package graphdb;

import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.apache.log4j.Logger;

public class DeleteVertexClass {

    //    public static final Logger log = Logger.getLogger("log");
    OrientdbDAO orientdbDAO;

    private void deleteVertex(String nodeType, String className, String deleteCount) {

        String query = "delete " + nodeType + " " + className + " limit " + deleteCount;
        orientdbDAO.getGraph().command(new OCommandSQL(query)).execute();
    }

    /**
     * @param nodeType                 Valid type: "vertex" or "edge"
     * @param maxDeleteNodeCount       Number of node that should be removed.
     * @param maxDeleteNodeCountPerItr Maximum node that should be removed in each iteration.
     * @param url                      Eg. Remote connection for localhost or ip.
     * @param databaseName             Database name that has the "className".
     * @param className                The class that you want to delete.
     * @return true if delete as much as maxDeleteNodeCount.
     */
    public boolean deleteNode(String nodeType, long maxDeleteNodeCount, long maxDeleteNodeCountPerItr, String url, String port, String databaseName, String className) {
        String urlAddr = String.format("remote:%s:%s/%s", url, port, databaseName);
        System.out.println(urlAddr);
        orientdbDAO = new OrientdbDAO(urlAddr);
        if (maxDeleteNodeCount < maxDeleteNodeCountPerItr) return false;
        long i = 0;
        while (true) {
            if (i + maxDeleteNodeCountPerItr <= maxDeleteNodeCount)
                i += maxDeleteNodeCountPerItr;
            else {
                maxDeleteNodeCountPerItr = maxDeleteNodeCount - i;
                i += maxDeleteNodeCountPerItr;
            }
            deleteVertex(nodeType, className, String.valueOf(maxDeleteNodeCountPerItr));
//            log.debug(i + "document(s) were removed!");
            System.out.println(i);
            if (i >= maxDeleteNodeCount) break;
        }
        return i == maxDeleteNodeCount;
    }

    public static void main(String[] args) {
        DeleteVertexClass deleteVertexClass = new DeleteVertexClass();
        boolean finish = deleteVertexClass.deleteNode("vertex", 11, 2, "localhost", "2424", "MyDB", "Customer");
        if(finish)
            System.out.println("Finished!");
        else
            System.out.println("An error occurred!");
    }
}
