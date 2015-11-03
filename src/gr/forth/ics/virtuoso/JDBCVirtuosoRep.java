package gr.forth.ics.virtuoso;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * This class contains a set of methods which are used to handle Virtuoso Triple
 * Store over JDBC interface.
 *
 * @author rous
 */
public class JDBCVirtuosoRep {

    private Connection conn;
    private Statement statement;
    HashMap<String, String> namespaces;

    /**
     * Creates a new Virtuoso connection.
     *
     * @param virt_instance The IP of the machine which hosts Virtuoso.
     * @param port The port.
     * @param usr The username of the certified user.
     * @param pwd The password of the certified user.
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public JDBCVirtuosoRep(String virt_instance, int port, String usr, String pwd) throws ClassNotFoundException, SQLException {
        this.conn = null;
        String[] sa = new String[4];
        sa[0] = virt_instance;
        sa[1] = port + "";
        sa[2] = usr;
        sa[3] = pwd;
        Class.forName("virtuoso.jdbc4.Driver");
        conn = DriverManager.getConnection("jdbc:virtuoso://" + sa[0] + ":" + sa[1] + "/charset=UTF-8/log_enable=2", sa[2], sa[3]);
        statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        initNamespaces();
    }

    /**
     * Creates a new Virtuoso connection. The credentials are taken from a
     * properties file.
     *
     * @param prop The properties file
     * @throws ClassNotFoundException
     * @throws SQLException
     * @throws IOException
     */
    public JDBCVirtuosoRep(Properties prop) throws ClassNotFoundException, SQLException, IOException {
        this.conn = null;
        String[] sa = new String[4];
        sa[0] = prop.getProperty("Repository_IP");
        sa[1] = Integer.parseInt(prop.getProperty("Repository_Port")) + "";
        sa[2] = prop.getProperty("Repository_Username");
        sa[3] = prop.getProperty("Repository_Password");
        Class.forName("virtuoso.jdbc4.Driver");
        conn = DriverManager.getConnection("jdbc:virtuoso://" + sa[0] + ":" + sa[1] + "/charset=UTF-8/log_enable=2", sa[2], sa[3]);
        statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        initNamespaces();
    }

    private void initNamespaces() {
        namespaces = new HashMap<>();
        namespaces.put("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
        namespaces.put("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
    }

    /**
     * Executes an update query given as parameter.
     *
     * @param query The update query.
     * @param logging A boolean variable which denotes whether the update query
     * and its execution time will be printed or not.
     */
    public void executeUpdateQuery(String query, boolean logging) {
        try {
            long start = 0;
            if (logging) {
                System.out.println("QUERY: " + query);
                start = System.currentTimeMillis();
            }
            statement.executeQuery("log_enable(3,1)");
            if (query.startsWith("sparql")) {
                if (!query.contains("PREFIX")) {
                    String prefixes = "PREFIX diachron: <http://www.diachron-fp7.eu/resource/>\n"
                            + "PREFIX efo:<http://www.ebi.ac.uk/efo/>\n"
                            + "PREFIX co:<http://www.diachron-fp7.eu/changes/>\n";
                    query = query.replace("sparql", "sparql " + prefixes);
                }
            }
            statement.executeUpdate(query);
            if (logging) {
                System.out.println("Done in " + (System.currentTimeMillis() - start) + "ms");
            }
        } catch (SQLException ex) {
            System.out.println("Exception: " + ex.getMessage());
            System.out.println("During the update query: " + query);
        }
    }

    /**
     * Executes a SPARQL select query given as parameter.
     *
     * @param query The SPARQL select query.
     * @param logging A boolean variable which denotes whether the select query
     * and its execution time will be printed or not.
     * @return
     */
    public ResultSet executeSparqlQuery(String query, boolean logging) {
        try {
            ResultSet result;
            long start = 0;
            if (logging) {
                System.out.println("QUERY: " + query);
                start = System.currentTimeMillis();
            }
            statement.setFetchSize(1000000);
            StringBuilder sparql = new StringBuilder();
            sparql.append("PREFIX diachron:<http://www.diachron-fp7.eu/resource/>\n").
                    append("PREFIX efo:<http://www.ebi.ac.uk/efo/>\n").
                    append("PREFIX co:<http://www.diachron-fp7.eu/changes/>\n");
            result = statement.executeQuery("sparql " + sparql + query);
            if (logging) {
                System.out.println("Done in " + (System.currentTimeMillis() - start) + "ms");
            }
            return result;
        } catch (SQLException ex) {
            System.out.println("Exception: " + ex.getMessage());
            System.out.println("During the select query: " + query);
            return null;
        }
    }

    /**
     * Returns the statement instance of this JDBC connection.
     *
     * @return
     */
    public Statement getStatement() {
        return statement;
    }

    /**
     * Returns the connection instance of this JDBC connection.
     *
     * @return
     */
    public Connection getConnection() {
        return conn;
    }

    /**
     * Returns the number of the triples contained in the named graph given as
     * parameter.
     *
     * @param graph The named graph whose triples are counted.
     * @return The number of triples.
     */
    public long triplesNum(String graph) {
        try {
            String query = "SPARQL SELECT count(*) from <" + graph + "> where {?s ?p ?o}";
            ResultSet result = statement.executeQuery(query);
            ResultSetMetaData meta = result.getMetaData();
            int count = meta.getColumnCount();
            long triples = 0;
            while (result.next()) {
                for (int c = 1; c <= count; c++) {
                    triples = Long.parseLong(result.getString(c));
                }
            }
            return triples;
        } catch (SQLException ex) {
            System.out.println("Exception " + ex.getMessage() + "occured during the count of triples.");
            return 0;
        }
    }

    /**
     * Checks if the given graph contains any triples
     *
     * @param graph The named graph which will be examined.
     * @return True if the graph exists, false otherwise.
     */
    public boolean graphExists(String graph) {
        String query = "SELECT * from <" + graph + "> where {?s ?p ?o} limit 2";
        ResultSet result = executeSparqlQuery(query, false);
        try {
            if (result.next()) {
                return true;
            }
        } catch (SQLException ex) {
            System.out.println("Exception: " + ex.getMessage());
        }
        return false;
    }

    /**
     * Imports a single RDF/XML file into Virtuoso. The file must must belong
     * within the machine which hosts Virtuoso as it is a server side import.
     *
     * @param filename The full path of the file which contains the RDF/XML
     * data.
     * @param graph The graph which will receive the data.
     * @param logging A boolean variable which denotes whether the import
     * execution time will be printed or not.
     */
    public void importSingleRDFFile(String filename, String graph, boolean logging) {
        String query = "RDF_LOAD_RDFXML_MT(file_to_string_output('" + filename + "'), '', '" + graph + "')";
        executeUpdateQuery(query, logging);
    }

    /**
     * Imports a single N3 file into Virtuoso. The file must must belong within
     * the machine which hosts Virtuoso as it is a server side import.
     *
     * @param filename The full path of the file which contains the N3 data.
     * @param graph The graph which will receive the data.
     * @param logging A boolean variable which denotes whether the import
     * execution time will be printed or not.
     */
    public void importSingleN3File(String filename, String graph, boolean logging) {
        String query = "TTLP_MT(file_to_string_output('" + filename + "'), '', '" + graph + "')";
        executeUpdateQuery(query, logging);
    }

    /**
     * Clears the named graph given as parameter.
     *
     * @param graph The named graph to be cleared.
     * @param logging A boolean variable which denotes whether the clear
     * execution time will be printed or not.
     */
    public void clearGraph(String graph, boolean logging) {
        executeUpdateQuery("SPARQL CLEAR GRAPH <" + graph + ">", logging);
    }

    /**
     * Terminates the JDBC connection.
     */
    public void terminate() {

        try {
            if (!statement.isClosed()) {
                statement.close();
                conn.close();
            }
        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage() + " occured during the close of statement and connection.");
        }
    }

    public void clearRdfFilesToLoadList() throws Exception {
        String query = "delete from DB.DBA.load_list";
        System.out.println(query);
        executeUpdateQuery(query, false);
    }

    public void addRdfFilesToLoad(String folder, String format, String graph) throws Exception {
        String query = "ld_dir('" + folder + "', '" + format + "', '" + graph + "')";
        System.out.println(query);
        executeUpdateQuery(query, false);
    }

    public void processFilesToLoadQueue(boolean timer) throws Exception {
        executeUpdateQuery("set isolation='uncommitted'", timer);
        executeUpdateQuery("rdf_loader_run()", timer);
    }

    public void importRDFDataToVirtuoso(String repFolder, String format, String graph, boolean update, boolean logging) throws Exception {
        // importing the data into virtuoso
        if (!update) {
            clearGraph(graph, false);
        }
        clearRdfFilesToLoadList();
        addRdfFilesToLoad(repFolder, format, graph);
//        ops.executeSparqlQuery("select * from DB.DBA.load_list");
        processFilesToLoadQueue(logging);
        executeUpdateQuery("checkpoint", true);
    }

    /**
     * Copies the contents of a named graph into another.
     *
     * @param source The source named graph.
     * @param destination The destination named graph.
     */
    public void copyGraph(String source, String destination) {
        String query = "sparql "
                + "INSERT INTO <" + destination + "> {"
                + "?s ?p ?o "
                + "}\n"
                + "WHERE {"
                + "graph <" + source + "> { ?s ?p ?o }"
                + "}";
        executeUpdateQuery(query, false);
    }

    /**
     * Renames a named graph.
     *
     * @param oldName The old name of the named graph.
     * @param newName The new name of the named graph.
     */
    public void renameGraph(String oldName, String newName) {
        String query = "UPDATE DB.DBA.RDF_QUAD TABLE OPTION (index RDF_QUAD_GS) "
                + "SET g = iri_to_id ('" + newName + "') "
                + "WHERE g = iri_to_id ('" + oldName + "', 0)";
        executeUpdateQuery(query, true);
    }

    /**
     * Inserts a (URI) triple into a named graph.
     *
     * @param s The subject URI of the triple.
     * @param p The predicate URI of the triple.
     * @param o The object URI triple.
     * @param graph The named graph into which the triple will be inserted.
     */
    public void addTriple(String s, String p, String o, String graph) {
        String update = "INSERT INTO <" + graph + "> {\n"
                + "<" + s + "> <" + p + "> <" + o + ">.\n"
                + "}\n";
        executeUpdateQuery("sparql " + update, false);
    }

    /**
     * Inserts a (Literal) triple into a named graph.
     *
     * @param s The subject URI of the triple.
     * @param p The predicate URI of the triple.
     * @param o The string literal object of the triple.
     * @param graph The named graph into which the triple will be inserted.
     */
    public void addLitTriple(String s, String p, String o, String graph) {
        String update = "INSERT INTO <" + graph + "> {\n"
                + "<" + s + "> <" + p + "> \"" + o + "\".\n"
                + "}\n";
        executeUpdateQuery("sparql " + update, false);
    }

    /**
     * Inserts a list of triples within the given namedgraph.
     *
     * @param triples A list of {@link TripleString} instances which represents
     * the triples to be inserted.
     * @param graph The named graph into which the triple will be inserted.
     */
    public void addMultipleTriples(List<TripleString> triples, String graph) {
        StringBuilder update = new StringBuilder();
        update.append("INSERT INTO <" + graph + "> {\n");
        for (TripleString triple : triples) {
            update.append(triple.getTripleString() + ".\n");
        }
        update.append("}\n");
        executeUpdateQuery("sparql " + update.toString(), false);
    }

    public void dereifyDiachronData(String reifiedSrc, String dereifiedDst) {
        String query = "sparql insert into <" + dereifiedDst + "> {\n "
                + "?s ?p ?o. \n"
                + "} where { \n "
                + "graph <" + reifiedSrc + "> {\n"
                + "?record diachron:subject ?s ;\n"
                + "        diachron:hasRecordAttribute ?ratt.\n"
                + "?ratt diachron:predicate ?p ; \n"
                + "      diachron:object ?o.\n"
                + "}\n"
                + "}";
        executeUpdateQuery(query, false);
    }

    public void addSchemaClass(String className, String graph) {
        addTriple(className, namespaces.get("rdf") + "type", namespaces.get("rdf") + "Class", graph);
    }

    public void addSchemaProperty(String propertyName, String domain, String range, String graph) {
        List<TripleString> triples = new ArrayList<>();
        triples.add(new TripleString(propertyName, namespaces.get("rdf") + "type", namespaces.get("rdf") + "Property", Triple_Type.URI));
        triples.add(new TripleString(propertyName, namespaces.get("rdfs") + "domain", domain, Triple_Type.URI));
        triples.add(new TripleString(propertyName, namespaces.get("rdfs") + "range", range, Triple_Type.URI));
        addMultipleTriples(triples, graph);
    }

    public void addDatatypeProperty(String propertyName, String domain, String range, String graph) {
        List<TripleString> triples = new ArrayList<>();
        triples.add(new TripleString(propertyName, namespaces.get("rdf") + "type", namespaces.get("rdf") + "Property", Triple_Type.URI));
        triples.add(new TripleString(propertyName, namespaces.get("rdfs") + "domain", domain, Triple_Type.URI));
        triples.add(new TripleString(propertyName, namespaces.get("rdfs") + "range", range, Triple_Type.LITERAL));
        addMultipleTriples(triples, graph);
    }

}
