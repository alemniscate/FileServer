package client;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.stream.Collectors;

public class Main { 
    static Scanner scanner;
    static String address = "127.0.0.1";
    static int port = 23456;
    static DataInputStream input;
    static DataOutputStream output;

    static String dataFolder = "src/client/data";
    //static String dataFolder = "client/data";
    
    public static void main(String[] args) {
        System.out.println("Client started!");
        System.out.println("Enter action (1 - get a file, 2 - save a file, 3 - delete a file): ");
        scanner = new Scanner(System.in);
        String command = scanner.nextLine();
        try {
            Socket socket = new Socket(InetAddress.getByName(address), port);
            input = new DataInputStream(socket.getInputStream());
            output = new DataOutputStream(socket.getOutputStream());
            int menuno = 0;
            if (!command.equals("exit")) {
                menuno = Integer.parseInt(command);
            }
            switch (menuno) {
                case 1:
                    get();
                    break;
                case 2:
                    save();
                    break;
                case 3:
                    delete();
                    break;
                case 0:
                    exit();
                    break;
            }     
            socket.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void get() {
        System.out.println("Do you want to get the file by name or by id (1 - name, 2 - id):");
        String number = scanner.nextLine();
        String sendmsg = "";
        if (number.equals("2")) {
            System.out.println("Enter id: ");
            String id = scanner.nextLine();
            sendmsg = "get GET BY_ID " + id;
        } else {
            System.out.println("Enter filename: ");
            String fileName = scanner.nextLine();
            if ("".equals(fileName)) {
                send("cancel", false);
                return;
            }
            sendmsg = "get GET BY_NAME " + fileName;
        }
        String response = send(sendmsg, false);
        if (response.startsWith("200")) {
            System.out.println("The file was downloaded! Specify a name for it: ");
            String outputFileName = scanner.nextLine();
            if (pipeBinary(outputFileName)) {
                System.out.println("File saved on the hard drive!");
            }
        }
    }

    static void save() {
        System.out.println("Enter filename: ");
        String inputFileName = scanner.nextLine();
        String path = dataFolder + "/" + inputFileName;
        if (!ReadText.isExist(path)) {
            System.out.println("file not found");
            send("cancel", false);
            return;
        }
        System.out.println("Enter name of the file to be saved on server:");
        String outputFileName = scanner.nextLine();
        try {
            String sendmsg = "put " + outputFileName;
            String response = send(sendmsg, true);
            if (response.startsWith("200")) {
                byte[] data = ReadBinary.read(path);
                output.writeInt(data.length);
                output.write(data);  
                System.out.println("The request was sent.");
                response  = getResponse();
                if (response.startsWith("200")) {
                    String id = response.substring(4);
                    System.out.println("Response says that file is saved! ID = " + id);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static void delete() {
        System.out.println("Do you want to delete the file by name or by id (1 - name, 2 - id):");
        String number = scanner.nextLine();
        String sendmsg = "";
        if (number.equals("2")) {
            System.out.println("Enter id: ");
            String id = scanner.nextLine();
            sendmsg = "delete GET BY_ID " + id;
        } else {
            System.out.println("Enter filename: ");
            String fileName = scanner.nextLine();
            sendmsg = "delete GET BY_NAME " + fileName;
        }
        String response = send(sendmsg, false);
        if (response.startsWith("200")) {
            System.out.println("The response says that this file was deleted successfully!");
        }
    }

    static void exit() {
        String sendmsg = "exit";
        send(sendmsg, false);
        try {
            Socket socket = new Socket(InetAddress.getByName(address), port);
            input = new DataInputStream(socket.getInputStream());
            output = new DataOutputStream(socket.getOutputStream());
            output.writeUTF(sendmsg);
            input.readUTF();
        } catch (Exception e) {
        }
    }

    static String getResponse() {
        try {
            String rcvmsg = input.readUTF();
//            System.out.println("Received: " + rcvmsg);
            if (rcvmsg.equals("403")) {
                System.out.println("The response says that creating the file was forbidden!");
            } else if (rcvmsg.equals("404")) {
                System.out.println("The response says that the file was not found!");
            }
            return rcvmsg;
        } catch (Exception e) {
            e.printStackTrace();
            return "400";
        }
    }

    static String send(String sendmsg, boolean continueFlag) {
        try {
//            System.out.println("Sent: " + sendmsg);
            output.writeUTF(sendmsg);
            if (!continueFlag) {
                System.out.println("The request was sent.");
            }
            String rcvmsg = input.readUTF();
//            System.out.println("Received: " + rcvmsg);
            if (rcvmsg.equals("403")) {
                System.out.println("The response says that creating the file was forbidden!");
            } else if (rcvmsg.equals("404")) {
                System.out.println("The response says that this file is not found!");
            }
            return rcvmsg;
        } catch (Exception e) {
            e.printStackTrace();
            return "400";
        }
    }

    static boolean pipeBinary(String fileName) {
        String path = dataFolder + "/" + fileName;
        try {
            int length = input.readInt();
            byte[] bytes = new byte[length];
            input.readFully(bytes, 0, length);
            return WriteBinary.write(path, bytes);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
}

class ReadBinary {
 
    static byte[] read(String fileName) { 
 
        byte[] bytes;
        try (
            InputStream inputStream = new FileInputStream(fileName);
        ) {
            long fileSize = new File(fileName).length();
            bytes = new byte[(int) fileSize];
            inputStream.read(bytes);
        } catch (IOException ex) {
            ex.printStackTrace();
            bytes = new byte[1];
        }
        return bytes;
    }
}

class WriteBinary {
 
    static boolean write(String fileName, byte[] bytes) { 
 
        try (
            OutputStream outputStream = new FileOutputStream(fileName);
        ) {
            outputStream.write(bytes);
            return true;
         } catch (IOException ex) {
            ex.printStackTrace();
            return false;
        }
    }
}

class ReadText {

    static boolean isExist(String fileName) {
        File file = new File(fileName);
        if (file.exists()) {
            return true;
        } else {
            return false;
        }
    }

    static String getAbsolutePath(String fileName) {
        File file = new File(fileName);
        return file.getAbsolutePath();
    }

    static String readAllWithoutEol(String fileName) {
        String text = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(fileName));   
            text =  br.lines().collect(Collectors.joining());        
            br.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return text;
    }

    static List<String> readLines(String fileName) {
        List<String> lines = new ArrayList<>();
        try {
            BufferedReader br = new BufferedReader(new FileReader(fileName));   
            lines =  br.lines().collect(Collectors.toList());        
            br.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return lines;
    }

    static String readAll(String fileName) {
        char[] cbuf = new char[4096];
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new FileReader(fileName));           
            while (true) {
                int length = br.read(cbuf, 0, cbuf.length);
                if (length != -1) {
                    sb.append(cbuf, 0, length);
                }
                if (length < cbuf.length) {
                    break;
                }
            }
            br.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return sb.toString();
    }

    static String readAll(String fileName, String encoding) {
        char[] cbuf = new char[4096];
        StringBuilder sb = new StringBuilder();
        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), encoding));
            while (true) {
                int length = br.read(cbuf, 0, cbuf.length);
                if (length != -1) {
                    sb.append(cbuf, 0, length);
                }
                if (length < cbuf.length) {
                    break;
                }
            }
            br.close();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
        return sb.toString();
    }
}

class WriteText {

    static boolean writeAll(String fileName, String text) {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(fileName));
            bw.write(text, 0, text.length());
            bw.close();
            return true;
        } catch (IOException e) {
            System.out.println(e.getMessage());
            return false;
        }
    }

    static boolean writeAll(String fileName, String text, String encoding) {
        try {
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), encoding));
            bw.write(text, 0, text.length());
            bw.close();
            return true;
        } catch (IOException e) {
            System.out.println(e.getMessage());
            return false;
        }
    }
}