// Tsogkas Evangelos 3150185

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * This class uses the {@link Client} to have access to elastic search functions.
 */
public class ElasticSearchMain {

    public static void main (String args[]) {
        File jsonFile = new File("output/texts.json");
        if (jsonFile.exists()) {
            Scanner scanner = new Scanner(System.in);
            System.out.print("Insert index name: ");
            String indexName = scanner.nextLine();
            Client client = new Client(indexName);
            boolean exists = client.indexExists();

            while (true) {
                System.out.println("Choose number: (Input '1' or '2' etc...)");
                System.out.println("1. Create index");
                System.out.println("2. Insert data");
                System.out.println("3. Query (phase 1)");
                System.out.println("4. Query (phase 2)");
                System.out.println("5. Delete index");
                System.out.println("6. Exit");
                String option = scanner.nextLine();

                if (option.equals("1")) {
                    if (!exists) client.createIndex();
                    else System.out.println("Index already exists...");
                }
                else if (option.equals("2")) {
                    if (exists) client.insertData();
                    else System.out.println("Index does not exist...");
                }
                else if (option.equals("3")) {
                    if (exists) {
                        System.out.println("Querying index...");
                        ArrayList<String> queries = readQueries();
                        String replies_file = "output/system_qrels.txt";
                        for (int i = 0; i < queries.size(); i++) {
                            ArrayList<float[]> replies = client.fullTextQuery(queries.get(i));
                            writeReplies(replies, i + 1, replies_file);
                        }
                        System.out.println("Saved replies to file '" + replies_file +"'...");
                    }
                    else System.out.println("Index does not exist...");
                }
                else if (option.equals("4")) {
                    if (exists) {
                        scanner = new Scanner(System.in);
                        System.out.println("Choose query type: ('1' or '2')");
                        System.out.println("1. Full text query (percentage of extracted phrases)");
                        System.out.println("2. MLT query");
                        String query_type = scanner.nextLine();

                        if (query_type.equals("1")) {
                            System.out.print("Insert the path of the directory with the extracted phrases files: ");
                            scanner = new Scanner(System.in);
                            String dir = scanner.nextLine();
                            if (new File(dir).exists()) {
                                for (float pct = 0.3f; pct <= 1f; pct += 0.3f) {
                                    ArrayList<String> queries = queriesFromExtractedPhrases(dir, pct);

                                    String replies_file = "output/system_qrels" + (int) (pct * 100) + "%.txt";
                                    for (int q = 0; q < queries.size(); q++) {
                                        ArrayList<float[]> replies = client.fullTextQuery(queries.get(q));
                                        writeReplies(replies, q + 1, replies_file);
                                    }
                                    System.out.println("Saved replies to file '" + replies_file + "'...");
                                }
                            } else System.out.println("Directory does not exist...");
                        }
                        else {
                            ArrayList<String> queries = readQueries();
                            String replies_file = "output/system_qrelsMLT.txt";
                            for (int i = 0; i < queries.size(); i++) {
                                ArrayList<float[]> replies = client.MLTQuery(queries.get(i));
                                writeReplies(replies, i + 1, replies_file);
                            }
                            System.out.println("Saved replies to file '" + replies_file +"'...");
                        }
                    }
                    else System.out.println("Index does not exist...");
                }
                else if (option.equals("5")) {
                    if (exists) client.deleteIndex();
                    else System.out.println("Index does not exist...");
                }
                else {
                    client.close();
                    break;
                }
                exists = client.indexExists();
            }
        }
        else
            System.out.println("File 'output/texts.json' not found. Please run CreateFilesMain to create it");
    }

    /* Creates the queries from the files with the extracted phrases according to the given percentage of phrases. */
    private static ArrayList<String> queriesFromExtractedPhrases(String directory, float percentage) {
        String[] files = {"193378.txt", "213164.txt", "204146.txt", "214253.txt", "212490.txt",
            "210133.txt", "213097.txt", "193715.txt", "197346.txt", "199879.txt"};
        ArrayList<String> queries = new ArrayList<>(10);

        for (String file : files) {
            ArrayList<String> phrases = new ArrayList<>();
            String readLine;
            try {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new DataInputStream(new FileInputStream(directory+"/"+file))));
                while((readLine = bufferedReader.readLine()) != null) {
                    phrases.add(readLine);
                }
                //create query
                StringBuilder query = new StringBuilder();
                for (int i=0; i<phrases.size()*percentage; i++) {
                    query.append(phrases.get(i));
                    query.append(" ");
                }
                queries.add(query.toString());
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        return queries;
    }

    /* Reads the file 'input/testingQueries.txt' with the queries. */
    private static ArrayList<String> readQueries() {
        ArrayList<String> queries = new ArrayList<>(10);
        String readLine;
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new DataInputStream(new FileInputStream("input/testingQueries.txt"))));

            while((readLine = bufferedReader.readLine()) != null) {
                queries.add(readLine.substring(4).replaceAll("[+-=&|><!(){}^\"~*?:\\[\\]\\\\/]", " "));
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return queries;
    }

    /* Writes query replies. */
    private static void writeReplies(ArrayList<float[]> replies, int queryId, String file) {
        try(FileWriter fw = new FileWriter(file, true);
            BufferedWriter bw = new BufferedWriter(fw);
            PrintWriter pw = new PrintWriter(bw))
        {
            for (float[] doc : replies) {
                pw.write("Q"+ String.format("%02d", queryId) + "\t0\t" + (int) doc[0] + "\t0\t" + doc[1] + "\tfullTextQuery\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

