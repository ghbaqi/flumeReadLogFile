package com.hupu.hermes.flume;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.hupu.hermes.flume.util.MysqlBaseDao;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.client.avro.ReliableEventReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.gson.stream.JsonReader;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HermesReliableTaildirEventReader implements ReliableEventReader {
    private static final Logger logger = LoggerFactory.getLogger(HermesReliableTaildirEventReader.class);
    /**
     * 缓存的所有fileGroup 配置
     */
    private final List<HermesTaildirMatcher> taildirCache;
    private final Table<String, String, String> headerTable;

    /**
     * 当前文件
     */
    private HermesTailFile currentFile = null;
    /**
     * 所有需要读取的文件inode --> HermesTailFile，会轮循更新
     */
    private Map<Long, HermesTailFile> tailFiles = Maps.newHashMap();
    private long updateTime;
    private boolean addByteOffset;
    private boolean cachePatternMatching;
    private boolean committed = true;
    private final boolean annotateFileName;
    private final String fileNameHeader;
    private String host;
    private MysqlBaseDao mysqlBaseDao;
    private String positionFilePath;

    /**
     * 创建可靠的TailDir 阅读器，监听配置的文件夹
     */
    private HermesReliableTaildirEventReader(Map<String, String> filePaths, Table<String, String, String> headerTable,
                                             String positionFilePath, boolean skipToEnd, boolean addByteOffset, boolean cachePatternMatching,
                                             boolean annotateFileName, String fileNameHeader, String host, MysqlBaseDao mysqlBaseDao) throws IOException {
        // 必要的检查
        Preconditions.checkNotNull(filePaths);
        Preconditions.checkNotNull(positionFilePath);

        if (logger.isDebugEnabled()) {
            logger.debug("Initializing {} with directory={}",
                    new Object[]{HermesReliableTaildirEventReader.class.getSimpleName(), filePaths});
        }

        List<HermesTaildirMatcher> taildirCache = Lists.newArrayList();
        for (Map.Entry<String, String> e : filePaths.entrySet()) {
            taildirCache.add(new HermesTaildirMatcher(e.getKey(), e.getValue(), cachePatternMatching));
        }
        logger.info("taildirCache: " + taildirCache.toString());
        logger.info("headerTable: " + headerTable.toString());

        this.taildirCache = taildirCache;
        this.headerTable = headerTable;
        this.addByteOffset = addByteOffset;
        this.cachePatternMatching = cachePatternMatching;
        this.annotateFileName = annotateFileName;
        this.fileNameHeader = fileNameHeader;

        this.host = host;
        this.mysqlBaseDao = mysqlBaseDao;
        this.positionFilePath = positionFilePath;
        // 根据fileGroup配置获取 需要读取的文件
        updateTailFiles(skipToEnd);

        logger.info("Updating position from position file: " + positionFilePath);

        // 载入Position文件（offset记录）
        loadPositionFile(positionFilePath);
    }

    public Position getRemoteOffset(String path) {
        String uniqueFlag = DigestUtils.md5Hex(host + ":" + path);
        String querySQL = "SELECT * FROM hermes_flume_position WHERE unique_flag=?";
        return mysqlBaseDao.query(querySQL, uniqueFlag);
    }


    /**
     * 加载配置文件，如果文件存在就从上次的文件开始
     */
    public void loadPositionFile(String filePath) {
        Long inode, pos, lineNumber;
        String path;
        FileReader fr = null;
        JsonReader jr = null;
        try {
            fr = new FileReader(filePath);
            jr = new JsonReader(fr);
            jr.beginArray();
            while (jr.hasNext()) {
                inode = null;
                pos = null;
                path = null;
                lineNumber = null;
                jr.beginObject();
                while (jr.hasNext()) {
                    switch (jr.nextName()) {
                        case "inode":
                            inode = jr.nextLong();
                            break;
                        case "pos":
                            pos = jr.nextLong();
                            break;
                        case "file":
                            path = jr.nextString();
                            break;
                        case "lineNumber":
                            lineNumber = jr.nextLong();
                            break;
                    }
                }
                jr.endObject();

                for (Object v : Arrays.asList(inode, pos, path, lineNumber)) {
                    Preconditions.checkNotNull(v, "Detected missing value in position file. " + "inode: " + inode
                            + ", pos: " + pos + ", path: " + path + ",lineNumber:" + lineNumber);
                }
                HermesTailFile tf = tailFiles.get(inode);
                if (tf != null) {
                    Position remoteOffset = getRemoteOffset(tf.getPath());
                    if (remoteOffset != null) {
                        if (remoteOffset.getPos() > pos || remoteOffset.getLine_number() > lineNumber) {
                            logger.info("Pos数据小于远程Offset,自动降级[" + path + "] lineNumber:(" + lineNumber + ")-->(" + remoteOffset.getLine_number() + ")" +
                                    ",pos:(" + pos + ")-->(" + remoteOffset.getPos() + ")");
                            pos = remoteOffset.getPos();
                            lineNumber = remoteOffset.getLine_number();
                        }
                    }
                    if (tf.updatePos(path, inode, pos, lineNumber)) {
                        tailFiles.put(inode, tf);
                    }
                } else {
                    logger.info("Missing file: " + path + ", inode: " + inode + ", pos: " + pos);
                }
            }
            jr.endArray();
        } catch (FileNotFoundException e) {
            logger.info("File not found: " + filePath + ", not updating position");
        } catch (IOException e) {
            logger.error("Failed loading positionFile: " + filePath, e);
        } finally {
            try {
                if (fr != null) {
                    fr.close();
                }
                if (jr != null) {
                    jr.close();
                }
            } catch (IOException e) {
                logger.error("Error: " + e.getMessage(), e);
            }
        }
    }

    public Map<Long, HermesTailFile> getTailFiles() {
        return tailFiles;
    }

    public void setCurrentFile(HermesTailFile currentFile) {
        this.currentFile = currentFile;
    }

    @Override
    public Event readEvent() throws IOException {
        List<Event> events = readEvents(1);
        if (events.isEmpty()) {
            return null;
        }
        return events.get(0);
    }

    @Override
    public List<Event> readEvents(int numEvents) throws IOException {
        return readEvents(numEvents, false);
    }

    @VisibleForTesting
    public List<Event> readEvents(HermesTailFile tf, int numEvents) throws IOException {
        setCurrentFile(tf);
        return readEvents(numEvents, true);
    }

    /**
     * 读取 numEvents 条数据
     *
     * @param numEvents
     * @param backoffWithoutNL
     * @return
     * @throws IOException
     */
    public List<Event> readEvents(int numEvents, boolean backoffWithoutNL) throws IOException {
        if (!committed) {
            if (currentFile == null) {
                throw new IllegalStateException("current file does not exist. " + currentFile.getPath());
            }
            logger.info("Last read was never committed - resetting position");
            long lastPos = currentFile.getPos();
            currentFile.updateFilePos(lastPos);
            currentFile.setLineNumber(currentFile.getLineNumber());
        }
        // 读取一个batch
        List<Event> events = currentFile.readEvents(numEvents, backoffWithoutNL, addByteOffset);
        if (events.isEmpty()) {
            return events;
        }
        // 每个Event添加Header
        Map<String, String> headers = currentFile.getHeaders();
        if (annotateFileName || (headers != null && !headers.isEmpty())) {
            for (Event event : events) {
                if (headers != null && !headers.isEmpty()) {
                    event.getHeaders().putAll(headers);
                }
                if (annotateFileName) {
                    event.getHeaders().put(fileNameHeader, currentFile.getPath());
                }
            }
        }
        committed = false;
        return events;
    }

    @Override
    public void close() throws IOException {
        for (HermesTailFile tf : tailFiles.values()) {
            if (tf.getRaf() != null) {
                tf.getRaf().close();
            }
        }
    }

    /**
     * Commit the last lines which were read.
     */
    @Override
    public void commit() throws IOException {
        if (!committed && currentFile != null) {
            long pos = currentFile.getLineReadPos();
            long ln = currentFile.getLn();
            currentFile.setLineNumber(ln);
            currentFile.setPos(pos);
            currentFile.setLastUpdated(updateTime);
            committed = true;
        }
    }

    /**
     * 更新tailFiles文件列表
     *
     * @param skipToEnd
     * @return
     * @throws IOException
     */
    public List<Long> updateTailFiles(boolean skipToEnd) throws IOException {
        updateTime = System.currentTimeMillis();

        File positionFile = new File(positionFilePath);

        List<Long> updatedInodes = Lists.newArrayList();

        for (HermesTaildirMatcher taildir : taildirCache) {
            // 获取每个组对应的header
            Map<String, String> headers = headerTable.row(taildir.getFileGroup());
            for (File f : taildir.getMatchingFiles()) {
                long inode;
                try {
                    inode = getInode(f);
                } catch (NoSuchFileException e) {
                    logger.info("File has been deleted in the meantime: " + e.getMessage());
                    continue;
                }
                HermesTailFile tf = tailFiles.get(inode);
                // 没有读取的 源数据文件
                if (tf == null || !tf.getPath().equals(f.getAbsolutePath())) {
                    String path = f.getAbsolutePath();
                    long startPos = skipToEnd ? f.length() : 0;
                    long lineNumber = 0;
                    if (!positionFile.exists()) {
                        //position文件被删除 降级
                        Position remoteOffset = getRemoteOffset(path);
                        if (remoteOffset != null) {
                            startPos = remoteOffset.getPos();
                            lineNumber = remoteOffset.getLine_number();
                            logger.info("Position文件未找到,自动降级[" + path + "] lineNumber:(0)-->(" + remoteOffset.getLine_number() + ")" +
                                    ",pos:(0)-->(" + remoteOffset.getPos() + ")");
                        }
                    }
                    tf = openFile(f, headers, inode, startPos, lineNumber, host);
                } else {
                    // 读取过的 源文件
                    // 文件发生了变化 且 没有读取到尾部
                    boolean updated = tf.getLastUpdated() < f.lastModified() || tf.getPos() != f.length();
                    if (updated) {
                        // 从上次读取的文件打开
                        if (tf.getRaf() == null) {
                            tf = openFile(f, headers, inode, tf.getPos(), tf.getLineNumber(), host);
                        }
                        // 覆盖写文件才可能出现的情况
                        if (f.length() < tf.getPos()) {
                            logger.info("Pos " + tf.getPos() + " is larger than file size! "
                                    + "Stop read file: " + tf.getPath() + ", inode: " + inode);
                            updated = false;
                            //tf.updatePos(tf.getPath(), inode, 0, 0);
                        }
                    }
                    tf.setNeedTail(updated);
                }
                tailFiles.put(inode, tf);
                updatedInodes.add(inode);
            }
        }
        return updatedInodes;
    }

    public List<Long> updateTailFiles() throws IOException {
        return updateTailFiles(false);
    }

    private long getInode(File file) throws IOException {
        long inode = (long) Files.getAttribute(file.toPath(), "unix:ino");
        return inode;
    }

    private HermesTailFile openFile(File file, Map<String, String> headers, long inode, long pos, long lineNumber,
                                    String host) {
        try {
            logger.info("Opening file: " + file + ", inode: " + inode + ", pos: " + pos + ",lineNumer: " + lineNumber);
            return new HermesTailFile(file, headers, inode, pos, lineNumber, host);
        } catch (IOException e) {
            throw new FlumeException("Failed opening file: " + file, e);
        }
    }

    /**
     * HermesReliableTaildirEventReader Builder 构造器
     */
    public static class Builder {
        private Map<String, String> filePaths;
        private Table<String, String, String> headerTable;
        private String positionFilePath;
        private boolean skipToEnd;
        private boolean addByteOffset;
        private boolean cachePatternMatching;
        private Boolean annotateFileName = HermesTaildirSourceConfigurationConstants.DEFAULT_FILE_HEADER;
        private String fileNameHeader = HermesTaildirSourceConfigurationConstants.DEFAULT_FILENAME_HEADER_KEY;
        private String host;
        private MysqlBaseDao mysqlBaseDao;

        public HermesReliableTaildirEventReader.Builder filePaths(Map<String, String> filePaths) {
            this.filePaths = filePaths;
            return this;
        }

        public HermesReliableTaildirEventReader.Builder headerTable(Table<String, String, String> headerTable) {
            this.headerTable = headerTable;
            return this;
        }

        public HermesReliableTaildirEventReader.Builder positionFilePath(String positionFilePath) {
            this.positionFilePath = positionFilePath;
            return this;
        }

        public HermesReliableTaildirEventReader.Builder skipToEnd(boolean skipToEnd) {
            this.skipToEnd = skipToEnd;
            return this;
        }

        public HermesReliableTaildirEventReader.Builder addByteOffset(boolean addByteOffset) {
            this.addByteOffset = addByteOffset;
            return this;
        }

        public HermesReliableTaildirEventReader.Builder cachePatternMatching(boolean cachePatternMatching) {
            this.cachePatternMatching = cachePatternMatching;
            return this;
        }

        public HermesReliableTaildirEventReader.Builder annotateFileName(boolean annotateFileName) {
            this.annotateFileName = annotateFileName;
            return this;
        }

        public HermesReliableTaildirEventReader.Builder fileNameHeader(String fileNameHeader) {
            this.fileNameHeader = fileNameHeader;
            return this;
        }

        public HermesReliableTaildirEventReader.Builder host(String host) {
            this.host = host;
            return this;
        }

        public HermesReliableTaildirEventReader.Builder mysqlBaseDao(MysqlBaseDao mysqlBaseDao) {
            this.mysqlBaseDao = mysqlBaseDao;
            return this;
        }

        public HermesReliableTaildirEventReader build() throws IOException {
            return new HermesReliableTaildirEventReader(filePaths, headerTable, positionFilePath, skipToEnd,
                    addByteOffset, cachePatternMatching, annotateFileName, fileNameHeader, host, mysqlBaseDao);
        }
    }

}
