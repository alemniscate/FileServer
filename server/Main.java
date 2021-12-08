package server;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main { 
    static String address = "127.0.0.1";
    static int port = 23456;
    static DataInputStream input;
    static DataOutputStream output;
   
    static String dataFolder = "src/server/data";
    static String idsFileName = "src/server/ids";
    //static String dataFolder = "server/data";
    //static String idsFileName = "server/ids";
    static int threadCount = 10;

    static Ids ids;
    static StoreIds storeIds;

    public static void main(String[] args) {
        ids = new Ids();
        if (ReadText.isExist(idsFileName)) {
            try {
                storeIds = (StoreIds) SerializationUtils.deserialize(idsFileName);
                storeIds.toIds(ids);
            } catch (IOException | ClassNotFoundException e) {
                System.out.println(e.getMessage());
            }
        } else {
            storeIds = new StoreIds();
        }

        ExecutorService pool = Executors.newFixedThreadPool(threadCount);

        System.out.println("Server started!");
        System.out.println(ReadText.getAbsolutePath(dataFolder));
        String address = "127.0.0.1";
        int port = 23456;
        try (ServerSocket server = new ServerSocket(port, 50, InetAddress.getByName(address));) {
            for (;!ServerTask.exitFlag;) {
            //while (!ServerTask.exitFlag) {
//                System.out.println("submit start " + i);
                pool.submit(new ServerTask(server.accept(), dataFolder, ids, storeIds, idsFileName));
//                System.out.println("submit end " + i);
            }           
            pool.shutdown();
        } catch (Exception e) {
            pool.shutdown();
        }
//        System.out.println("server Main end");
    }   
}

class ServerTask implements Runnable {

    static boolean exitFlag = false;
    Socket socket;
    String dataFolder;
    Ids ids;
    DataInputStream input;
    DataOutputStream output;
    StoreIds storeIds;
    String idsFileName;

    ServerTask(Socket socket, String dataFolder, Ids ids, StoreIds storeIds, String idsFileName) {
        this.socket = socket;
        this.dataFolder = dataFolder;
        this.ids = ids;
        this.storeIds = storeIds;
        this.idsFileName = idsFileName;
//        System.out.println("new thread =" + Thread.currentThread().getId());
    }

    public void run() {
        try {
//            System.out.println("run thread =" + Thread.currentThread().getId());
            input = new DataInputStream(socket.getInputStream());
            output  = new DataOutputStream(socket.getOutputStream());
            String rcvmsg = input.readUTF();
            String[] strs = rcvmsg.split("\\s+");
            String command = strs[0];

            String response;  
            switch (command) {
                case "get":
                    response = get(rcvmsg.substring(4));
                    send(response);
                    break;
                case "put":
                    put(rcvmsg);
                    break;
                case "delete":
                    response = delete(rcvmsg.substring(7));
                    send(response);
                    break;
                case "exit":
                    response = exit();
                    send(response);
                    exitFlag = true;
                    break;
                case "cancel":
                    send("200");
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    String exit() {
        storeIds.toStore(ids.getIdTable());
        try {
            SerializationUtils.serialize(storeIds, idsFileName);
//            System.out.println("Done! Id data Saved to the file.");
        } catch (IOException err) {
            System.out.println(err.getMessage());
        }
        return "200";
    }

    String get(String param) {
        String fileName = "";
        if (param.startsWith("GET BY_ID")) {
            String id = param.substring(10);
            fileName = ids.getFileName(id);
            if ("".equals(fileName)) {
                return "404";
            }
        } else {
            fileName = param.substring(12);  // GET BY_NAME
        }
        String path = dataFolder + "/" + fileName;
        if (!ReadText.isExist(path)) {
            return "404";
        }
        byte[] bytes = ReadBinary.read(path);
        String sendmsg = "200";
        send(sendmsg);
        try {
            output.writeInt(bytes.length);
            output.write(bytes);
        } catch (Exception e) {
            e.printStackTrace();
            return "403";
        } 
        return "200";
    }

    String pipeBinary(IdFileName idFileName) {
        String fileName = idFileName.getFileName();
        String id = idFileName.getId();
        String path = dataFolder + "/" + fileName;
        try {
            int length = input.readInt();
            byte[] bytes = new byte[length];
            input.readFully(bytes, 0, length);
            if (WriteBinary.write(path, bytes)) {
                return "200 " + id;
            } else {
                return "403";
            }
        } catch (Exception e) {
            e.printStackTrace();
            return "403";
        }
    }

    void put(String rcvmsg) {
        String fileName = "";
        if (rcvmsg.length() > 4) {
            fileName = rcvmsg.substring(4);
        }
        IdFileName idFileName = ids.storeNewEntry(fileName);
        if (idFileName == null) {
            send("403");
            return;
        }
        send("200");
        String response = pipeBinary(idFileName);
        send(response);
        if (!response.startsWith("200")) {
            ids.remove(idFileName);         // rewind
        }
    }
    
    String delete(String param) {
        String fileName = "";
        if (param.startsWith("GET BY_ID")) {
            String id = param.substring(10);
            fileName = ids.getFileName(id);
            if ("".equals(fileName)) {
                return "404";
            }
        } else {
            fileName = param.substring(12);  // GET BY_NAME
        }
        if (!ids.deleteByFileName(fileName)) {
            return "404";
        }
        String path = dataFolder + "/" + fileName;
        if (!ReadText.isExist(path)) {
            return "404";
        }
        File file = new File(path);
        file.delete();
        return "200";
    }

    void send(String sendmsg) {
        try {
//            System.out.println("Sent: " + sendmsg);
            output.writeUTF(sendmsg);
//            System.out.println("The request was sent.");
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}

class IdFileName {
    String id;
    String fileName;

    IdFileName(String id, String fileName) {
        this.id = id;
        this.fileName = fileName;
    }

    String getId() {
        return id;
    }

    String getFileName() {
        return fileName;
    }

    void setId(String id) {
        this.id = id;
    }

    void setFileName(String fileName) {
        this.fileName = fileName;
    }
}

class Ids {

    Random rand;
    List<IdFileName> idTable;

    Ids() {
        rand = new Random();
        idTable = new ArrayList<>();
    }

    boolean remove(IdFileName idFileName) {
        String id = idFileName.getId();
        for (int i = 0; i < idTable.size(); i++) {
            IdFileName entry = idTable.get(i);
            if (id.equals(entry.getId())) {
                idTable.remove(i);
                return true;
            }
        }
        return false;
    }

    IdFileName storeNewEntry(String fileName) {
        String newFileName = fileName;
        String strId = "";
        if ("".equals(fileName)) {
            while (true) {
                int intId = rand.nextInt(100000);
                strId = "" + intId;
                if (checkEntry(strId, strId)) {
                    break;
                }
            }
            newFileName = strId;
        } else {
            if (!checkFileName(fileName)) {
                return null;
            }
            while (true) {
                int intId = rand.nextInt(100000);
                strId = "" + intId;
                if (checkId(strId)) {
                    break;
                }
            } 
        }

        IdFileName newEntry = new IdFileName(strId, newFileName);
        idTable.add(newEntry);
        return newEntry;
    }

    boolean checkEntry(String id, String fileName) {
        for (int i = 0; i < idTable.size(); i++) {
            IdFileName idFileName = idTable.get(i);
            if (id.equals(idFileName.getId())) {
                return false;
            }
            if (fileName.equals(idFileName.getFileName())) {
                return false;
            }
        }
        return true;
    }

    boolean checkFileName(String fileName) {
        for (int i = 0; i < idTable.size(); i++) {
            IdFileName idFileName = idTable.get(i);
            if (fileName.equals(idFileName.getFileName())) {
                return false;
            }
        }
        return true;
    }

    boolean checkId(String id) {
        for (int i = 0; i < idTable.size(); i++) {
            IdFileName idFileName = idTable.get(i);
            if (id.equals(idFileName.getId())) {
                return false;
            }
        }
        return true;
    }

    String getId(String fileName) {
        int intId = rand.nextInt(100000);
        String strId = "" + intId;
        this.idTable.add(new IdFileName(strId, fileName));
        return strId;
    }

    String getFileName(String id) {
        for (int i = 0; i < idTable.size(); i++) {
            IdFileName idFileName = idTable.get(i);
            if (idFileName.getId().equals(id)) {
                return idFileName.getFileName();
            }
        }
        return "";
    }

    List<IdFileName> getIdTable() {
        return idTable;
    }

    void setIdTable(List<IdFileName> idTable) {
        this.idTable = idTable;
    }

    boolean deleteById(String id) {
        for (int i = 0; i < idTable.size(); i++) {
            IdFileName idFileName = idTable.get(i);
            if (idFileName.getId().equals(id)) {
                idTable.remove(i);
                return true;
            }
        }
        return false;
    }

    boolean deleteByFileName(String fileName) {
        for (int i = 0; i < idTable.size(); i++) {
            IdFileName idFileName = idTable.get(i);
            if (idFileName.getFileName().equals(fileName)) {
                idTable.remove(i);
                return true;
            }
        }
        return false;
    }

}

class StoreIds implements Serializable {
    private static final long serialVersionUID = 1L;

    String[] idTableStore;

    StoreIds() {}

    void toStore(List<IdFileName> idTable) {
        idTableStore = new String[idTable.size()];
        for (int i = 0; i < idTable.size(); i++) {
            IdFileName idFileName = idTable.get(i); 
            idTableStore[i] = idFileName.getId() + " " + idFileName.getFileName(); 
        }
    }

    void toIds(Ids ids) {
        List<IdFileName> idTable = new ArrayList<>();
        for (int i = 0; i < idTableStore.length; i++) {
            String idTableItem = idTableStore[i];
            int index =  idTableItem.indexOf(" ");
            String id = idTableItem.substring(0, index);
            String fileName = idTableItem.substring(index + 1);
            idTable.add(new IdFileName(id, fileName));
        }
        ids.setIdTable(idTable);
    }
}

class SerializationUtils {
    static void serialize(Object obj, String fileName) throws IOException {
        FileOutputStream fos = new FileOutputStream(fileName);
        BufferedOutputStream bos = new BufferedOutputStream(fos);
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(obj);
        oos.close();
    }

    static Object deserialize(String fileName) throws IOException, ClassNotFoundException {
        FileInputStream fis = new FileInputStream(fileName);
        BufferedInputStream bis = new BufferedInputStream(fis);
        ObjectInputStream ois = new ObjectInputStream(bis);
        Object obj = ois.readObject();
        ois.close();
        return obj;
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