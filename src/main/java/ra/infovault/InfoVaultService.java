package ra.infovault;

import ra.common.*;
import ra.common.content.Content;
import ra.common.route.Route;
import ra.util.JSONParser;
import ra.util.SystemSettings;

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

    private File internalStorage;
    private File externalStorage;

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
                    e.getMessage().addErrorMessage("InfoVault required.");
                    return;
                }
                if(obj instanceof List) {
                    List list = (List)DLC.getContent(e);
                    for(Object ob : list) {
                        if(ob instanceof InfoVault) {
                            InfoVault infoVault = (InfoVault)ob;
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
                            e.getMessage().addErrorMessage("Only InfoVault objects supported within a List.");
                            return;
                        }
                    }
                } else if(obj instanceof InfoVault) {
                    InfoVault infoVault = (InfoVault)obj;
                    if(infoVault.content==null) {
                        e.getMessage().addErrorMessage("InfoVault.content is required.");
                        return;
                    }
                    try {
                        save((InfoVault)obj);
                    } catch (Exception ex) {
                        e.getMessage().addErrorMessage(ex.getLocalizedMessage());
                        return;
                    }
                } else {
                    e.getMessage().addErrorMessage("Only InfoVault or List<InfoVault> supported.");
                    return;
                }
                break;
            }
            case OPERATION_LOAD: {
                Object obj = DLC.getContent(e);
                if(obj==null) {
                    e.getMessage().addErrorMessage("InfoVault required.");
                    return;
                }
                if(!(obj instanceof InfoVault)) {
                    e.getMessage().addErrorMessage("Only InfoVault objects supported.");
                    return;
                }
                InfoVault infoVault = (InfoVault)obj;
                if(infoVault.location==null) {
                    e.getMessage().addErrorMessage("InfoVault.location required.");
                    return;
                }
                if(infoVault.content==null) {
                    e.getMessage().addErrorMessage("InfoVault.content required.");
                    return;
                }
                try {
                    load(infoVault);
                } catch (FileNotFoundException fileNotFoundException) {
                    e.getMessage().addErrorMessage(fileNotFoundException.getLocalizedMessage());
                }
                break;
            }
            case OPERATION_DELETE: {
                Object obj = DLC.getContent(e);
                if(obj==null) {
                    e.getMessage().addErrorMessage("InfoVault required.");
                    return;
                }
                if(!(obj instanceof InfoVault)) {
                    e.getMessage().addErrorMessage("Only InfoVault objects supported.");
                    return;
                }
                InfoVault infoVault = (InfoVault)obj;
                try {
                    if(!delete(infoVault))
                        e.getMessage().addErrorMessage("Problem deleting InfoVault: "+infoVault.location+"/"+infoVault.content.getName());
                } catch (FileNotFoundException fileNotFoundException) {
                    e.getMessage().addErrorMessage(fileNotFoundException.getLocalizedMessage());
                }
                break;
            }
            default: deadLetter(e);
        }
    }

    private boolean save(InfoVault infoVault) throws ExternalStorageNotAvailable, FileCreationFailedException, FileNotWriteableException, IOException {
        LOG.info("Saving content...");
        File path;
        if(infoVault.storeExternal) {
            if(externalStorage==null)
                throw new ExternalStorageNotAvailable("External Storage Not Available");
            path = externalStorage;
        } else {
            path = internalStorage;
        }
        File file = new File(path, infoVault.content.getId());
        try {
            if(!file.exists() && !file.createNewFile()) {
                throw new FileCreationFailedException("Unable to create file: "+infoVault.content.getId()+" in directory: "+path.getAbsolutePath());
            }
        } catch (IOException e) {
            throw new FileCreationFailedException(e.getLocalizedMessage());
        }
        if(!file.setWritable(true)) {
            throw new FileNotWriteableException("Unable to set file to writeable: "+infoVault.content.getId()+" in directory: "+path.getAbsolutePath());
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

    public Boolean delete(InfoVault infoVault) throws FileNotFoundException {
        LOG.info("Deleting InfoVault at: "+infoVault.location+"/"+infoVault.content.getName());
        File file = new File(infoVault.location, infoVault.content.getName());
        if(!file.exists())
            throw new FileNotFoundException("File not found to delete: "+infoVault.location+"/"+infoVault.content.getName());
        if(!file.delete()) {
            throw new FileNotFoundException("Deletion of file failed: "+infoVault.location+"/"+infoVault.content.getName());
        }
        return true;
    }

    public void load(InfoVault infoVault) throws FileNotFoundException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        LOG.info("Loading InfoVault at: "+infoVault.location+"/"+infoVault.content.getId());
        File path = new File(infoVault.location);
        if(!path.exists()) {
            throw new FileNotFoundException("InfoVault.location doesn't exist: "+infoVault.location);
        }
        File file = new File(path, infoVault.content.getId());
        if(!file.exists()) {
            throw new FileNotFoundException("InfoVault Content doesn't exist: "+infoVault.location+"/"+infoVault.content.getId());
        }
        byte[] bytes = loadFile(file);
        Content content = Content.newInstance((Map<String,Object>)JSONParser.parse(new String(bytes)));
        infoVault.content = content;
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

    @Override
    public boolean start(Properties properties) {
        super.start(properties);
        LOG.info("Starting...");
        updateStatus(ServiceStatus.STARTING);
        // Setup Internal Storage


        // Setup External Storage if set
        if(properties.getProperty("ra.infovault.storage.external")!=null) {
            String externalStoragePath = properties.getProperty("ra.infovault.storage.external");
            File temp = new File(externalStoragePath);
            if(temp.exists()) {
                externalStorage = temp;
            } else {
                LOG.warning(externalStoragePath+" does not exist.");
            }
        }

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
