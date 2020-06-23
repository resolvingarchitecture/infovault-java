package ra.infovault;

import ra.common.*;
import ra.common.content.Content;
import ra.common.route.Route;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * Asynchronous access to the file system as a Service (handleDocument).
 * It only supports DocumentMessage within the supplied Envelope to save as an InfoVault in the Envelope
 * through DLC.getContent():InfoVault.
 * Content is saved as a JSON document as-is.
 */
public final class InfoVaultService extends BaseService {

    private static final Logger LOG = Logger.getLogger(InfoVaultService.class.getName());

    public static final String OPERATION_SAVE = "SAVE";
    public static final String OPERATION_LOAD = "LOAD";
    public static final String OPERATION_DELETE = "DELETE";

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
                        if(ob instanceof InfoVault) {
                            InfoVault infoVault = (InfoVault)ob;
                            if(infoVault.location==null) {
                                e.getMessage().addErrorMessage("InfoVault.location is required.");
                                return;
                            }
                            if(infoVault.content==null) {
                                e.getMessage().addErrorMessage("InfoVault.content is required.");
                                return;
                            }
                            try {
                                save((InfoVault)ob);
                            } catch (FileNotFoundException fileNotFoundException) {
                                e.getMessage().addErrorMessage(fileNotFoundException.getLocalizedMessage());
                                return;
                            }
                        } else {
                            LOG.warning("Only Content objects supported within a List.");
                        }
                    }
                } else if(obj instanceof InfoVault) {
                    InfoVault infoVault = (InfoVault)obj;
                    if(infoVault.location==null) {
                        e.getMessage().addErrorMessage("InfoVault.location is required.");
                        return;
                    }
                    if(infoVault.content==null) {
                        e.getMessage().addErrorMessage("InfoVault.content is required.");
                        return;
                    }
                    try {
                        save((InfoVault)obj);
                    } catch (FileNotFoundException fileNotFoundException) {
                        e.getMessage().addErrorMessage(fileNotFoundException.getLocalizedMessage());
                        return;
                    }
                } else {
                    LOG.warning("Only InfoVault or List<InfoVault> supported.");
                    return;
                }
                break;
            }
            case OPERATION_LOAD: {
                Object obj = DLC.getContent(e);
                if(obj instanceof List) {

                } else if(obj instanceof InfoVault) {

                }
                break;
            }
            case OPERATION_DELETE: {
                Object obj = DLC.getContent(e);
                if(obj instanceof InfoVault) {

                }
                break;
            }
            default: deadLetter(e);
        }
    }

    private boolean save(InfoVault infoVault) throws FileNotFoundException {
        LOG.info("Saving content...");
        File path = new File(infoVault.location);
        if(!path.exists()) {
            if(!infoVault.autoCreate)
                throw new FileNotFoundException("InfoVault.location doesn't exist and autoCreate = false");
            if(!path.mkdirs() ||!path.setWritable(true))
                throw new FileNotFoundException("Unable to create or set writable InfoVault.location: "+infoVault.location);
        }
        File file = new File(path, infoVault.content.getName());
        try {
            if(!file.exists() && !file.createNewFile() || !file.setWritable(true)) {
                throw new FileNotFoundException("Unable to create file or set writable: "+infoVault.content.getName());
            }
        } catch (IOException e) {
            throw new FileNotFoundException("Unable to create file: "+infoVault.content.getName());
        }

        byte[] buffer = new byte[8 * 1024];
        ByteArrayInputStream in = new ByteArrayInputStream(infoVault.content.toJSON().getBytes());
        FileOutputStream out = new FileOutputStream(file);
        try {
            int b;
            while ((b = in.read(buffer)) != -1) {
                out.write(buffer, 0, b);
            }
            LOG.info("Content saved.");
        } catch (IOException ex) {
            throw new FileNotFoundException("Unable to persist content: "+file.getAbsolutePath());
        } finally {
            try {
                out.close();
                in.close();
            } catch (IOException e) {
                LOG.warning(e.getLocalizedMessage());
            }
        }
        return true;
    }

    public Boolean delete(InfoVault infoVault) {
        LOG.info("Deleting content for label: "+label+" and key: "+key);
        File path = new File(dbDir, label);
        if(!path.exists())
            return true;
        File file = new File(path, key);
        if(!file.exists())
            return true;
        return file.delete();
    }

    public void load(InfoVault infoVault) throws FileNotFoundException {
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

        loadFile(file);
    }

    @Override
    public List<InfoVault> loadRange(String location, int start, int numberEntries) {
        LOG.info("Loading range of content in: "+location);
        List<InfoVault> contentList = new ArrayList<>();
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
    public List<InfoVault> loadAll(String location) {
        LOG.info("Loading all content for location: "+location);
        List<InfoVault> contentList = new ArrayList<>();
        File path = null;
        if(location != null) {
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
