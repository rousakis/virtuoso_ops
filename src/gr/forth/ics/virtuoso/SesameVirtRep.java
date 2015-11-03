package gr.forth.ics.virtuoso;



/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Properties;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.Rio;

import virtuoso.sesame2.driver.VirtuosoRepository;

/**
 * This class contains a set of methods which are used to handle Virtuoso Triple
 * Store using Sesame API calls.
 *
 * @author rous
 */
public class SesameVirtRep {

    private Repository repository;
    private RepositoryConnection con;

    /**
     * Creates a new Virtuoso connection using a Sesame RepositoryConnection.
     *
     * @param virt_instance The IP of the machine which hosts Virtuoso.
     * @param port The port.
     * @param usr The username of the certified user.
     * @param pwd The password of the certified user.
     * @throws RepositoryException
     */
    public SesameVirtRep(String virt_instance, int port, String usr, String pwd) throws RepositoryException {
        repository = new VirtuosoRepository("jdbc:virtuoso://" + virt_instance + ":" + port + "/charset=UTF-8/log_enable=2", usr, pwd);
        con = repository.getConnection();
        con.setAutoCommit(false);
    }

    /**
     * Creates a new Virtuoso connection. The credentials are taken from a
     * properties file.
     *
     * @param prop The properties file instance
     * @throws RepositoryException
     */
    public SesameVirtRep(Properties prop) throws RepositoryException {
        String virt_instance = prop.getProperty("Repository_IP");
        String usr = prop.getProperty("Repository_Username");
        String pwd = prop.getProperty("Repository_Password");
        int port = Integer.parseInt(prop.getProperty("Repository_Port"));
        repository = new VirtuosoRepository("jdbc:virtuoso://" + virt_instance + ":" + port + "/charset=UTF-8/log_enable=2", usr, pwd);
        con = repository.getConnection();
        con.setAutoCommit(false);
    }

    /**
     * Returns the instance of the current RepositoryConnection.
     *
     * @return
     */
    public RepositoryConnection getCon() {
        return con;
    }

    /**
     * Terminates the RepositoryConnection connection.
     */
    public void terminate() {
        try {
            con.close();
            repository.shutDown();
        } catch (RepositoryException ex) {
            System.out.println("Exception: " + ex.getMessage() + " occured .");
        }
    }

    /**
     * Clears the named graph given as parameter.
     *
     * @param graph The named graph to be cleared.
     * @throws Exception
     */
    public void clearGraphContents(String graph) throws Exception {
        System.out.println("Deleting contents of: " + graph);
        con.clear(new URIImpl(graph));
    }

    /**
     * Executes a SPARQL select query given as parameter.
     *
     * @param sparql The SPARQL select query.
     * @return
     * @throws RepositoryException
     * @throws MalformedQueryException
     * @throws QueryEvaluationException
     */
    public TupleQueryResult queryExec(String sparql) throws RepositoryException, MalformedQueryException, QueryEvaluationException {
        TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, sparql);
        TupleQueryResult result = tupleQuery.evaluate();
        return result;
    }

    /**
     *
     * Returns the number of the triples contained in the named graph given as
     * parameter.
     *
     * @param graph The named graph whose triples are counted.
     * @return The number of triples.
     * @throws Exception
     */
    public long triplesNum(String graph) throws Exception {
        if (graph == null) {
            TupleQueryResult res = this.queryExec(""
                    + "select count(*)  "
                    + "where { ?s ?p ?o }");
            return Long.parseLong(res.next().getValue("callret-0").stringValue());
        } else {
            TupleQueryResult res = this.queryExec(""
                    + "select count(*)  "
                    + "from <" + graph.toString() + ">"
                    + "where { ?s ?p ?o }");
            return Long.parseLong(res.next().getValue("callret-0").stringValue());
        }
    }

    /**
     * Exports the contents of a named graph into a file in various formats.
     *
     * @param filename The filename in which the export data will be stored. The
     * filename can be either in the Virtuoso-host machine of not.
     * @param format The format of the exported data e.g., RDF/XML, N3,
     * N-Triples etc.
     * @param graphSource The named graph whose data will be exported.
     * @throws Exception
     */
    public void exportToFile(String filename, RDFFormat format, String graphSource) throws Exception {
        System.out.println("Exporting graph: " + graphSource.toString());
        RDFWriter writer = Rio.createWriter(format, new OutputStreamWriter(new FileOutputStream(new File(filename))));
        con.export(writer, new URIImpl(graphSource));
    }

    /**
     * Imports a file with RDF contents within Virtuoso and the named graph
     * given as parameter.
     *
     * @param filename The filename which contains the data to be imported.
     * @param format The format of the give data e.g., RDF/XML, N3, N-Triples
     * etc.
     * @param graphDest The named graph destination.
     * @throws Exception
     */
    public void importFile(String filename, RDFFormat format, String graphDest) throws Exception {
        System.out.println("Importing file: " + filename + " into graph: " + graphDest);
        con.add(new File(filename), graphDest, format, new URIImpl(graphDest));
//        con.commit();
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
        URI sub = repository.getValueFactory().createURI(s);
        URI pred = repository.getValueFactory().createURI(p);
        URI obj = repository.getValueFactory().createURI(o);
        URI g = repository.getValueFactory().createURI(graph);
        try {
            con.add(sub, pred, obj, g);
        } catch (RepositoryException ex) {
            System.out.println("Exception: " + ex.getMessage() + " occured .");
        }
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
        URI sub = repository.getValueFactory().createURI(s);
        URI pred = repository.getValueFactory().createURI(p);
        Literal obj = repository.getValueFactory().createLiteral(o);
        URI g = repository.getValueFactory().createURI(graph);
        try {
            con.add(sub, pred, obj, g);
        } catch (RepositoryException ex) {
            System.out.println("Exception: " + ex.getMessage() + " occured .");
        }
    }

    /**
     * Inserts a (Literal) triple into a named graph.
     *
     * @param s The subject URI of the triple.
     * @param p The predicate URI of the triple.
     * @param o The double literal object of the triple.
     * @param graph The named graph into which the triple will be inserted.
     */
    public void addLitTriple(String s, String p, double o, String graph) {
        URI sub = repository.getValueFactory().createURI(s);
        URI pred = repository.getValueFactory().createURI(p);
        Literal obj = repository.getValueFactory().createLiteral(o);
        URI g = repository.getValueFactory().createURI(graph);
        try {
            con.add(sub, pred, obj, g);
        } catch (RepositoryException ex) {
            System.out.println("Exception: " + ex.getMessage() + " occured .");
        }
    }

    public static void main(String[] args) throws Exception {
        Properties prop = new Properties();
        InputStream inputStream;
        try {
            inputStream = new FileInputStream("config.properties");
            prop.load(inputStream);
        } catch (IOException ex) {
            System.out.println("Exception: " + ex.getMessage() + " occured .");
            return;
        }
        SesameVirtRep sesame = new SesameVirtRep(prop);
//        sesame.exportToFile("version_2.43.rdf", RDFFormat.RDFXML, "http://original/efo/2.43");
//        sesame.exportToFile("version_2.44.rdf", RDFFormat.RDFXML, "http://original/efo/2.44");
//        sesame.exportToFile("version_2.47.rdf", RDFFormat.RDFXML, "http://original/efo/2.47");
//        sesame.exportToFile("version_2.48.rdf", RDFFormat.RDFXML, "http://original/efo/2.48");
//        sesame.exportToFile("version_2.49.rdf", RDFFormat.RDFXML, "http://original/efo/2.49");
//        sesame.exportToFile("version_2.50.rdf", RDFFormat.RDFXML, "http://original/efo/2.50");
//        sesame.exportToFile("input\\changes_ontology\\multidimensional\\ChangesOntologySchema.rdf", RDFFormat.RDFXML, "http://www.diachron-fp7.eu/changes/multidimensional/schema");
//        sesame.exportToFile("input\\changes_ontology\\multidimensional\\ChangesOntologySchema.nt", RDFFormat.NTRIPLES, "http://www.diachron-fp7.eu/changes/multidimensional/schema");
//        sesame.exportToFile("input\\changes_ontology\\multidimensional\\ChangesOntologySchema.n3", RDFFormat.N3, "http://www.diachron-fp7.eu/changes/multidimensional/schema");
//        sesame.exportToFile("input\\changes_ontology\\multidimensional\\ChangesOntologySchema.ttl", RDFFormat.TURTLE, "http://www.diachron-fp7.eu/changes/multidimensional/schema");
                
        sesame.exportToFile("input\\changes_ontology\\ontological\\datamarket-dataset-associations.nt", RDFFormat.NTRIPLES, "http://datamarket-dataset");
//        sesame.exportToFile("version_2.45.rdf", RDFFormat.RDFXML, "http://original/efo/2.45");
//        sesame.exportToFile("version_2.46.rdf", RDFFormat.RDFXML, "http://original/efo/2.46");
//        sesame.clearGraphContents("http://original/efo/2.45");
//        sesame.clearGraphContents("http://original/efo/2.46");
        sesame.terminate();
    }
}
