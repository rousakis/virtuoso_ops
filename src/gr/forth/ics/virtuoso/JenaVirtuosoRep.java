package gr.forth.ics.virtuoso;



import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.*;

import com.hp.hpl.jena.rdf.model.RDFNode;
import java.io.BufferedReader;
import java.io.FileInputStream;

import java.io.InputStreamReader;
import java.util.List;
import virtuoso.jena.driver.*;

/**
 * This class contains a set of methods which are used to handle Virtuoso Triple
 * Store using Jena API calls.
 *
 * @author rousakis
 */
public class JenaVirtuosoRep {

    String conn_str;
    String username, password;
    VirtGraph graph;

    /**
     * Creates a new Virtuoso connection using a Jena RepositoryConnection.
     *
     * @param virt_instance The IP of the machine which hosts Virtuoso.
     * @param usr The username of the certified user.
     * @param pwd The password of the certified user.
     */
    public JenaVirtuosoRep(String virt_instance, String usr, String pwd) {
        conn_str = "jdbc:virtuoso://" + virt_instance + ":1111";
        username = usr;
        password = pwd;
        this.graph = new VirtGraph(conn_str, username, password);
    }

    public JenaVirtuosoRep(String graph, String virt_instance, String usr, String pwd) throws Exception {
        conn_str = "jdbc:virtuoso://" + virt_instance + ":1111";
        username = usr;
        password = pwd;
        this.graph = new VirtGraph(graph, conn_str, username, password);
    }

    /**
     * Sets the graph which will be considered.
     *
     * @param graph The graph which is set
     */
    public void setGraph(String graph) {
        this.graph = new VirtGraph(graph, conn_str, username, password);
    }

    /**
     * Returns the considered graph.
     *
     * @return
     */
    public VirtGraph getGraph() {
        return this.graph;
    }

    /**
     * Inserts a URI triple into the considered graph.
     *
     * @param s The subject of the triple.
     * @param p The predicate of the triple.
     * @param o The object of the triple which is a URI.
     */
    public void addTriple(Node s, Node p, Node o) {
        this.graph.add(new Triple(s, p, o));
    }

    /**
     * Inserts a Literal triple into the considered graph.
     *
     * @param s The subject of the triple.
     * @param p The predicate of the triple.
     * @param o The object of the triple which is a string literal.
     */
    public void addTriple(Node s, Node p, String o) {
        this.graph.add(new Triple(s, p, Node.createLiteral(o)));
    }

    /**
     * Executes a SPARQL select query given as parameter and print the results.
     *
     * @param query The SPARQL select query.
     */
    public void executeSPARQL(String query) {
        Query sparql = QueryFactory.create(query);
        VirtuosoQueryExecution vqe = VirtuosoQueryExecutionFactory.create(query, this.graph);
        ResultSet results = vqe.execSelect();
        while (results.hasNext()) {
            QuerySolution result = results.nextSolution();
//		    RDFNode graph = result.get("graph");
            RDFNode s = result.get("s");
            RDFNode p = result.get("p");
            RDFNode o = result.get("o");
            System.out.println(" { " + s + " " + p + " " + o + " . }");
        }
    }
}
