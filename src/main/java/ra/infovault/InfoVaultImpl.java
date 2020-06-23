package ra.infovault;

import ra.common.InfoVault;
import ra.common.Status;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

public final class InfoVaultImpl implements InfoVault {

    private String location;
    private String name;
    private Status status = Status.Stopped;
    private Logger LOG = Logger.getLogger(InfoVaultImpl.class.getName());

    private File dbDir;

    public InfoVaultImpl() {}

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public boolean teardown() {

        return true;
    }

    public void save(String label, String key, byte[] content, boolean autoCreate) throws FileNotFoundException {
        LOG.info("Saving content...");
        File path = null;
        if(label != null) {
            path = new File(dbDir, label);
            if(!path.exists()) {
                if(!autoCreate)
                    throw new FileNotFoundException("Label doesn't exist and autoCreate = false");
                else {
                    path.mkdirs();
                    path.setWritable(true);
                }
            }
        }
        File file = null;
        if(path == null)
            file = new File(dbDir, key);
        else
            file = new File(path, key);
        file.setWritable(true);

        if(!file.exists() && autoCreate) {
            try {
                if(!file.createNewFile()) {
                    LOG.warning("Unable to create new file.");
                    return;
                }
            } catch (IOException e) {
                LOG.warning(e.getLocalizedMessage());
                return;
            }
        }
        byte[] buffer = new byte[8 * 1024];
        ByteArrayInputStream in = new ByteArrayInputStream(content);
        FileOutputStream out = new FileOutputStream(file);
        try {
            int b;
            while ((b = in.read(buffer)) != -1) {
                out.write(buffer, 0, b);
            }
            LOG.info("Content saved.");
        } catch (IOException ex) {
            LOG.warning(ex.getLocalizedMessage());
        } finally {
            try {
                out.close();
                in.close();
            } catch (IOException e) {
                LOG.warning(e.getLocalizedMessage());
            }
        }
    }

    public Boolean delete(String label, String key) {
        LOG.info("Deleting content for label: "+label+" and key: "+key);
        File path = new File(dbDir, label);
        if(!path.exists())
            return true;
        File file = new File(path, key);
        if(!file.exists())
            return true;
        return file.delete();
    }

    public byte[] load(String label, String key) throws FileNotFoundException {
        LOG.info("Loading content for label: "+label+" and key: "+key);
        File path = null;
        if(label != null) {
            path = new File(dbDir, label);
            if(!path.exists()) {
                throw new FileNotFoundException("Label doesn't exist");
            }
        }

        File file = null;
        if(path == null)
            file = new File(dbDir, key);
        else
            file = new File(path, key);

        return loadFile(file);
    }

    @Override
    public List<byte[]> loadRange(String label, int start, int numberEntries) {
        LOG.info("Loading range of content for label: "+label);
        List<byte[]> contentList = new ArrayList<>();
        File path = null;
        if(label != null) {
            path = new File(dbDir, label);
            if(path.exists()) {
                // first get a list of file names and sort them alphabetically
                File[] children = path.listFiles();
                Map<String, File> fileMap = new HashMap<>();
                List<String> names = new ArrayList<>();
                for(File f : children) {
                    names.add(f.getName());
                    fileMap.put(f.getName(), f);
                }
                Collections.sort(names);
                int cursor = 1;
                int end = start + numberEntries;
                File f;
                for(String name : names) {
                    if(cursor>=start) {
                        f = fileMap.get(name);
                        try {
                            contentList.add(loadFile(f));
                        } catch (FileNotFoundException e) {
                            LOG.warning("File not found: " + f.getAbsolutePath());
                        }
                    }
                    cursor++;
                    if(cursor > end)
                        break;
                }
            }
        }
        return contentList;
    }

    @Override
    public List<byte[]> loadAll(String label) {
        LOG.info("Loading all content for label: "+label);
        List<byte[]> contentList = new ArrayList<>();
        File path = null;
        if(label != null) {
            path = new File(dbDir, label);
            if(path.exists()) {
                File[] children = path.listFiles();
                for(File f : children) {
                    try {
                        contentList.add(loadFile(f));
                    } catch (FileNotFoundException e) {
                        LOG.warning("File not found: "+f.getAbsolutePath());
                    }
                }
            }
        }
        return contentList;
    }

    private byte[] loadFile(File file) throws FileNotFoundException {
        byte[] buffer = new byte[8 * 1024];
        FileInputStream in = new FileInputStream(file);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            int b;
            while((b = in.read(buffer)) != -1) {
                out.write(buffer, 0, b);
            }
            LOG.info("Content loaded.");
        } catch (IOException ex) {
            LOG.warning(ex.getLocalizedMessage());
            return null;
        } finally {
            try {
                out.close();
                in.close();
            } catch (IOException ex) {
                LOG.warning(ex.getLocalizedMessage());
            }
        }
        return out.toByteArray();
    }

    @Override
    public boolean init(Properties properties) {
        if(location==null) {
            LOG.warning("Location must be provided.");
            return false;
        }
        if(name==null) {
            LOG.warning("Name must be provided.");
            return false;
        }
        File baseDir = new File(location);
        if (!baseDir.exists() && !baseDir.mkdir()) {
            LOG.warning("Unable to build InfoVaultService directory at: " + baseDir.getAbsolutePath());
            return false;
        }
        baseDir.setWritable(true);
        dbDir = new File(baseDir, name);
        if(!dbDir.exists() && !dbDir.mkdir()) {
            LOG.warning("Unable to create dbFile at: "+location+"/"+name);
            return false;
        } else {
            dbDir.setWritable(true);
        }
        return true;
    }

    @Override
    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public String getLocation() {
        return location;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

//    public static void main(String[] args) {
//        DID did = new DID();
//        did.setAlias("Alice");
//        did.setIdentityHash(HashUtil.generateHash(did.getAlias()));
//
//        LocalFSInfoVaultDB s = new LocalFSInfoVaultDB();
//        s.dbDir = new File("dbDir");
//        if(!s.dbDir.exists()) {
//            if (!s.dbDir.mkdir()) {
//                System.out.println("Unable to make dbDir.");
//                return;
//            }
//        }
//
//        SaveDIDDAO saveDIDDAO = new SaveDIDDAO(s, did, true);
//        saveDIDDAO.execute();
//
//        DID did2 = new DID();
//        did2.setAlias("Alice");
//
//        LoadDIDDAO loadDIDDAO = new LoadDIDDAO(s, did2);
//        loadDIDDAO.execute();
//        DID didLoaded = loadDIDDAO.getLoadedDID();
//
//        System.out.println("did1.hash: "+did.getIdentityHash());
//        System.out.println("did2.hash: "+didLoaded.getIdentityHash());
//    }
//    public static void main(String[] args) {
//        LocalFSInfoVaultDB db = new LocalFSInfoVaultDB();
//        db.setLocation("/home/objectorange/Projects/1m5/1m5/src/main/resources/");
//        db.setName("contacts");
//        db.init(null);
//        try {
//            DID didA = new DID();
//            didA.setUsername("Alice");
//            didA.setStatus(DID.Status.ACTIVE);
//            didA.getPublicKey().setAddress("1234567890");
//            didA.getAttributes().put("birthday", "1991-12-21");
//            db.save(DID.class.getName(), didA.getUsername(), JSONPretty.toPretty(JSONParser.toString(didA.toMap()), 4).getBytes(), true);
//
//            DID didB = new DID();
//            didB.setUsername("Bob");
//            didB.setStatus(DID.Status.ACTIVE);
//            didB.getPublicKey().setAddress("1234567890");
//            didB.getAttributes().put("birthday", "1992-12-21");
//            db.save(DID.class.getName(), didB.getUsername(), JSONPretty.toPretty(JSONParser.toString(didB.toMap()), 4).getBytes(), true);
//
//            DID didC = new DID();
//            didC.setUsername("Charlie");
//            didC.setStatus(DID.Status.ACTIVE);
//            didC.getPublicKey().setAddress("1234567890");
//            didC.getAttributes().put("birthday", "1993-12-21");
//            db.save(DID.class.getName(), didC.getUsername(), JSONPretty.toPretty(JSONParser.toString(didC.toMap()), 4).getBytes(), true);
//
//            List<byte[]> bytes = db.loadRange(DID.class.getName(), 1, 10);
//            for(byte[] b : bytes) {
//                System.out.print(new String(b));
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//    }
}
