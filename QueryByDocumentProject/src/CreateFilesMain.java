// Tsogkas Evangelos 3150185

import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.util.Scanner;

/**
 * This class has its own main that needs to be ran once to create the files.
 *
 * Output:
 * 1. XML collection with "text" element (Path: output/new_xml_collection)
 * 2. JSON file for elastic search (Path: output/texts.json)
 *
 */
public class CreateFilesMain {

    private static String rcn, acronym, text, identifier;

    /* Parses the xml collection and calls the corresponding methods to create the new xml file with the "text" element
       and to write the json object to the json file.
    */
    public static void main (String args[]) throws ParserConfigurationException, SAXException, IOException, TransformerException {
        File directory = new File("output");
        if (!directory.exists()) {
            directory = new File("output/new_xml_collection");
            directory.mkdirs();

            System.out.print("Insert the path of the directory with the xml collection: ");
            Scanner scanner = new Scanner(System.in);
            String input = scanner.nextLine();
            System.out.println("");

            File dir = new File(input);
            File[] files = dir.listFiles();
            int count = 0;
            if (files!=null) {
                int numFiles = files.length;
                for (final File file : files) {
                    DocumentBuilder builder = DocumentBuilderFactory
                            .newInstance()
                            .newDocumentBuilder();
                    Document doc = builder.parse(file);

                    String objective, title;
                    NodeList list = doc.getElementsByTagName("project");
                    for (int i = 0; i < list.getLength(); i++) {
                        Node n = list.item(i);

                        if (n.getNodeType() == Node.ELEMENT_NODE) {
                            Element eElement = (Element) n;

                            rcn = eElement.getElementsByTagName("rcn").item(0).getTextContent();
                            acronym = eElement.getElementsByTagName("acronym").item(0).getTextContent();
                            objective = eElement.getElementsByTagName("objective").item(0).getTextContent();
                            title = eElement.getElementsByTagName("title").item(0).getTextContent();
                            identifier = eElement.getElementsByTagName("identifier").item(0).getTextContent();

                            text = title + "    " + objective;

                            writeJSON();
                            createXMLWithTextElement(file.getName());
                        }
                    }
                    //shows progress to console
                    count++;
                    String progress = "\rCompleted: " + count + "/" + numFiles;
                    System.out.write(progress.getBytes());
                }
            }
        }
        else
            System.out.println("Directory output already exists");
    }

    /* Creates a single xml file with the "text" element. */
    private static void createXMLWithTextElement(String fileName) throws ParserConfigurationException, TransformerException {
        DocumentBuilderFactory factory =
                DocumentBuilderFactory.newInstance();
        DocumentBuilder parser = factory.newDocumentBuilder();
        Document doc=parser.newDocument();

        Element root=doc.createElement("project");
        doc.appendChild(root);

        Element ercn=doc.createElement("rcn");
        ercn.setTextContent(rcn);
        root.appendChild(ercn);

        Element eacronym=doc.createElement("acronym");
        eacronym.setTextContent(acronym);
        root.appendChild(eacronym);

        Element etext=doc.createElement("text");
        etext.setTextContent(text);
        root.appendChild(etext);

        Element eidentifier=doc.createElement("identifier");
        eidentifier.setTextContent(identifier);
        root.appendChild(eidentifier);

        TransformerFactory transformerfactory=
                TransformerFactory.newInstance();
        Transformer transformer=
                transformerfactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty(OutputKeys.METHOD, "xml");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
        DOMSource source=new DOMSource(doc);

        File file = new File("output/new_xml_collection/"+fileName);
        StreamResult result=new StreamResult(file);
        transformer.transform(source, result);
    }

    /* Writes json objects to a json file. Uses rcn as index id. */
    private static void writeJSON() {
        JSONObject jindex = new JSONObject();
        JSONObject jproject = new JSONObject();

        jindex.put("index", new JSONObject().put("_id", rcn));
        jproject.put("acronym", acronym);
        jproject.put("text", text);
        jproject.put("identifier", identifier);

        try(FileWriter fw = new FileWriter("output/texts.json", true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter pw = new PrintWriter(bw))
        {
            pw.write(jindex.toString());
            pw.write("\n");
            pw.write(jproject.toString());
            pw.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

