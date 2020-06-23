package ra.infovault;

import ra.common.*;
import ra.common.content.Content;
import ra.common.route.Route;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.logging.Logger;

/**
 * Asynchronous access to the file system as a Service (handleDocument).
 * It only supports DocumentMessage within the supplied Envelope to save as a Content in the Envelope
 * through DLC.getContent():Content.
 * Content is saved as a JSON document as-is with the naming scheme of:
 * Access to an instance of LocalFSInfoVaultDB (InfoVaultDB) is provided in each Service too (by BaseService)
 * for synchronous access.
 * Developer's choice to which to use on a per-case basis by Services extending BaseService.
 * Clients always use this service as they do not have direct access to InfoVaultDB.
 * Consider using this service for heavier higher-latency work by Services extending BaseService vs using their
 * synchronous access instance in BaseService.
 *
 * InfoVault
 * Maintain thread-safe.
 * Use directly synchronously.
 * InfoVaultDB instances are singleton by type when instantiated through InfoVaultService.getInstance(String className).
 * Multiple types can be instantiated in parallel, e.g. LocalFSInfoVaultDB and Neo4jDB
 * Pass in class name (including package) to get an instance of it.
 * Make sure your class implements the InfoVault interface.
 *
 */
public final class InfoVaultService extends BaseService {

    private static final Logger LOG = Logger.getLogger(InfoVaultService.class.getName());

    public static final String OPERATION_SAVE = "SAVE";
    public static final String OPERATION_LOAD = "LOAD";

    public InfoVaultService(MessageProducer producer, ServiceStatusListener serviceStatusListener) {
        super(producer, serviceStatusListener);
    }

    @Override
    public void handleDocument(Envelope e) {
        Route r = e.getRoute();
        switch(r.getOperation()) {
            case OPERATION_SAVE: {
                Object obj = DLC.getContent(e);
                if(obj==null) {
                    LOG.warning("Content of List<Content> required.");
                    return;
                }
                if(obj instanceof List) {
                    List list = (List)DLC.getContent(e);
                    for(Object ob : list) {
                        if(ob instanceof Content) {
                            save((Content)ob);
                        } else {
                            LOG.warning("Only Content objects supported within a List.");
                        }
                    }
                } else if(obj instanceof Content) {

                } else {
                    LOG.warning("Only Content or List<Content> supported.");
                    return;
                }
                break;
            }
            case OPERATION_LOAD: {
                Content content = (Content)DLC.getContent(e);

            }
            default: deadLetter(e);
        }
    }

    private void save(Content content) {
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
    public boolean start(Properties properties) {
        super.start(properties);
        LOG.info("Starting...");
        updateStatus(ServiceStatus.STARTING);

        updateStatus(ServiceStatus.RUNNING);
        LOG.info("Started.");
        return true;
    }

    @Override
    public boolean shutdown() {
        super.shutdown();
        LOG.info("Shutting down...");
        updateStatus(ServiceStatus.SHUTTING_DOWN);

        updateStatus(ServiceStatus.SHUTDOWN);
        LOG.info("Shutdown.");
        return true;
    }

    @Override
    public boolean gracefulShutdown() {
        return shutdown();
    }

}
