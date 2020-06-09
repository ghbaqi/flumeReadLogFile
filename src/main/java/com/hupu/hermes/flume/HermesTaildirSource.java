package com.hupu.hermes.flume;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.hupu.hermes.flume.biz.ServerBizTblMeta;
import com.hupu.hermes.flume.util.MysqlBaseDao;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.PollableSourceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import static com.hupu.hermes.flume.HermesTaildirSourceConfigurationConstants.*;

/**
 * 基于https://github.com/apache/flume/tree/trunk/flume-ng-sources/flume-taildir-source 修改而来
 */
public class HermesTaildirSource extends AbstractSource implements PollableSource, Configurable, BatchSizeSupported {
    private static final Logger logger = LoggerFactory.getLogger(HermesTaildirSource.class);
    private static final Logger errorContentLogger = LoggerFactory.getLogger("errorContent");

    private Map<String, String> filePaths;
    private Table<String, String, String> headerTable;
    private int batchSize;
    private String positionFilePath;
    private boolean skipToEnd;
    private boolean byteOffsetHeader;

    private SourceCounter sourceCounter;
    private HermesReliableTaildirEventReader reader;
    private ScheduledExecutorService idleFileChecker;
    private ScheduledExecutorService positionWriter;
    private int retryInterval = 1000;
    private int maxRetryInterval = 5000;
    private int idleTimeout;
    private int checkIdleInterval = 5000;
    private int writePosInitDelay = 5000;
    private int writePosInterval;
    private boolean cachePatternMatching;

    private List<Long> existingInodes = new CopyOnWriteArrayList<Long>();
    private List<Long> idleInodes = new CopyOnWriteArrayList<Long>();
    private Long backoffSleepIncrement;
    private Long maxBackOffSleepInterval;
    private boolean fileHeader;
    private String fileHeaderKey;
    private Long maxBatchCount;
    private String host;
    private MysqlBaseDao mysqlBaseDao;

    private HashMap<String, List<ServerBizTblMeta>> bizMap = new HashMap();

    @Override
    public synchronized void start() {
        logger.info("{} HermesTaildirSource source starting with directory: {}", getName(), filePaths);
        try {
            reader = new HermesReliableTaildirEventReader.Builder().filePaths(filePaths).headerTable(headerTable)
                    .positionFilePath(positionFilePath).skipToEnd(skipToEnd).addByteOffset(byteOffsetHeader)
                    .cachePatternMatching(cachePatternMatching).annotateFileName(fileHeader).fileNameHeader(fileHeaderKey)
                    .host(host).mysqlBaseDao(mysqlBaseDao).build();
        } catch (IOException e) {
            throw new FlumeException("Error instantiating HermesReliableTaildirEventReader", e);
        }
        // 创建空闲文件检查器(另启动线程) 5s间隔检查
        idleFileChecker = Executors
                .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("idleFileChecker").build());
        idleFileChecker.scheduleWithFixedDelay(new HermesTaildirSource.IdleFileCheckerRunnable(), idleTimeout,
                checkIdleInterval, TimeUnit.MILLISECONDS);
        // 创建保存文件offset的线程 5s间隔触发一次保存
        positionWriter = Executors
                .newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("positionWriter").build());
        positionWriter.scheduleWithFixedDelay(new HermesTaildirSource.PositionWriterRunnable(), writePosInitDelay,
                writePosInterval, TimeUnit.MILLISECONDS);

        super.start();
        logger.info("HermesTaildirSource started");
        sourceCounter.start();
    }

    @Override
    public synchronized void stop() {
        try {
            super.stop();
            ExecutorService[] services = {idleFileChecker, positionWriter};
            for (ExecutorService service : services) {
                service.shutdown();
                if (!service.awaitTermination(1, TimeUnit.SECONDS)) {
                    service.shutdownNow();
                }
            }
            // write the last position
            writePosition();
            reader.close();
        } catch (InterruptedException e) {
            logger.info("Interrupted while awaiting termination", e);
        } catch (IOException e) {
            logger.info("Failed: " + e.getMessage(), e);
        }
        sourceCounter.stop();
        logger.info("HermesTaildir source {} stopped. Metrics: {}", getName(), sourceCounter);
    }

    @Override
    public String toString() {
        return String.format(
                "HermesTaildir source: { positionFile: %s, skipToEnd: %s, "
                        + "byteOffsetHeader: %s, idleTimeout: %s, writePosInterval: %s }",
                positionFilePath, skipToEnd, byteOffsetHeader, idleTimeout, writePosInterval);
    }

    @Override
    public synchronized void configure(Context context) {
        String fileGroups = context.getString(FILE_GROUPS);
        Preconditions.checkState(fileGroups != null, "Missing param: " + FILE_GROUPS);

        filePaths = selectByKeys(context.getSubProperties(FILE_GROUPS_PREFIX), fileGroups.split("\\s+"));
        Preconditions.checkState(!filePaths.isEmpty(),
                "Mapping for tailing files is empty or invalid: '" + FILE_GROUPS_PREFIX + "'");

        String homePath = System.getProperty("user.home").replace('\\', '/');
        positionFilePath = context.getString(POSITION_FILE, homePath + DEFAULT_POSITION_FILE);
        Path positionFile = Paths.get(positionFilePath);
        try {
            Files.createDirectories(positionFile.getParent());
        } catch (IOException e) {
            throw new FlumeException("Error creating positionFile parent directories", e);
        }
        headerTable = getTable(context, HEADERS_PREFIX);
        batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        skipToEnd = context.getBoolean(SKIP_TO_END, DEFAULT_SKIP_TO_END);
        byteOffsetHeader = context.getBoolean(BYTE_OFFSET_HEADER, DEFAULT_BYTE_OFFSET_HEADER);
        idleTimeout = context.getInteger(IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT);
        writePosInterval = context.getInteger(WRITE_POS_INTERVAL, DEFAULT_WRITE_POS_INTERVAL);
        cachePatternMatching = context.getBoolean(CACHE_PATTERN_MATCHING, DEFAULT_CACHE_PATTERN_MATCHING);

        backoffSleepIncrement = context.getLong(PollableSourceConstants.BACKOFF_SLEEP_INCREMENT,
                PollableSourceConstants.DEFAULT_BACKOFF_SLEEP_INCREMENT);
        maxBackOffSleepInterval = context.getLong(PollableSourceConstants.MAX_BACKOFF_SLEEP,
                PollableSourceConstants.DEFAULT_MAX_BACKOFF_SLEEP);
        fileHeader = context.getBoolean(FILENAME_HEADER, DEFAULT_FILE_HEADER);
        fileHeaderKey = context.getString(FILENAME_HEADER_KEY, DEFAULT_FILENAME_HEADER_KEY);
        maxBatchCount = context.getLong(MAX_BATCH_COUNT, DEFAULT_MAX_BATCH_COUNT);
        if (maxBatchCount <= 0) {
            maxBatchCount = DEFAULT_MAX_BATCH_COUNT;
            logger.warn("Invalid maxBatchCount specified, initializing source " + "default maxBatchCount of {}",
                    maxBatchCount);
        }

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }

        host = context.getString(AGENT_HOST, DEFAULT_AGENT_HOST);
        if (host == null) {
            logger.error("Error host is null");
            System.exit(-1);
        }

        mysqlBaseDao = new MysqlBaseDao(context.getString(MYSQL_URL), context.getString(MYSQL_USERNAME),
                context.getString(MYSQL_PASSWORD));
    }

    @Override
    public long getBatchSize() {
        return batchSize;
    }

    /**
     * 为每一个fileGroups 指定配置的规则
     *
     * @param map
     * @param keys
     * @return
     */
    private Map<String, String> selectByKeys(Map<String, String> map, String[] keys) {
        Map<String, String> result = Maps.newHashMap();
        for (String key : keys) {
            if (map.containsKey(key)) {
                result.put(key, map.get(key));
            }
        }
        return result;
    }

    /**
     * 解析Header 配置
     *
     * @param context
     * @param prefix
     * @return
     */
    private Table<String, String, String> getTable(Context context, String prefix) {
        Table<String, String, String> table = HashBasedTable.create();
        for (Map.Entry<String, String> e : context.getSubProperties(prefix).entrySet()) {
            String[] parts = e.getKey().split("\\.", 2);
            table.put(parts[0], parts[1], e.getValue());
        }
        return table;
    }

    @VisibleForTesting
    protected SourceCounter getSourceCounter() {
        return sourceCounter;
    }

    /**
     * 轮询处理数据文件
     *
     * @return
     */
    @Override
    public Status process() {
        Status status = Status.BACKOFF;
        try {
            existingInodes.clear();
            // 获取需要处理的文件
            existingInodes.addAll(reader.updateTailFiles());
            // 如果配置过个filegroup 这里是顺序读取的
            for (long inode : existingInodes) {
                HermesTailFile tf = reader.getTailFiles().get(inode);
                if (tf.needTail()) {
                    boolean hasMoreLines = tailFileProcess(tf, true);
                    if (hasMoreLines) {
                        status = Status.READY;
                    }
                }
            }
            closeTailFiles();
        } catch (Throwable t) {
            logger.error("Unable to tail files", t);
            sourceCounter.incrementEventReadFail();
            status = Status.BACKOFF;
        }
        return status;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return backoffSleepIncrement;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return maxBackOffSleepInterval;
    }

    /**
     * TailFile
     *
     * @param tf
     * @param backoffWithoutNL
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    private boolean tailFileProcess(HermesTailFile tf, boolean backoffWithoutNL)
            throws IOException, InterruptedException {
        long batchCount = 0;
        while (true) {
            reader.setCurrentFile(tf);
            List<Event> events = reader.readEvents(batchSize, backoffWithoutNL);
            if (events.isEmpty()) {
                return false;
            }
            sourceCounter.addToEventReceivedCount(events.size());
            sourceCounter.incrementAppendBatchReceivedCount();

            events = explode(tf, events);
            try {
                if (!events.isEmpty()) {
                    getChannelProcessor().processEventBatch(events);
                }
                reader.commit();
            } catch (ChannelException ex) {
                logger.error("", ex);
                logger.warn("The channel is full or unexpected failure. " + "The source will try again after "
                        + retryInterval + " ms");
                sourceCounter.incrementChannelWriteFail();
                TimeUnit.MILLISECONDS.sleep(retryInterval);
                retryInterval = retryInterval << 1;
                retryInterval = Math.min(retryInterval, maxRetryInterval);
                continue;
            }
            retryInterval = 1000;
            sourceCounter.addToEventAcceptedCount(events.size());
            sourceCounter.incrementAppendBatchAcceptedCount();
            if (events.size() < batchSize) {
                logger.debug("The events taken from " + tf.getPath() + " is less than " + batchSize);
                return false;
            }
            if (++batchCount >= maxBatchCount) {
                logger.debug("The batches read from the same file is larger than " + maxBatchCount);
                return true;
            }
        }
    }

    private void closeTailFiles() throws IOException, InterruptedException {
        for (long inode : idleInodes) {
            HermesTailFile tf = reader.getTailFiles().get(inode);
            if (tf.getRaf() != null) {
                // when file has not closed yet
                tailFileProcess(tf, false);
                tf.close();
                logger.info("Closed file: " + tf.getPath() + ", inode: " + inode + ", pos: " + tf.getPos());
            }
        }
        idleInodes.clear();
    }

    /**
     * 空闲文件检查线程
     */
    private class IdleFileCheckerRunnable implements Runnable {
        @Override
        public void run() {
            try {
                long now = System.currentTimeMillis();
                // 超过idleTimeout 2分钟就确定为闲置文件，可以从tailFiles中剔除
                for (HermesTailFile tf : reader.getTailFiles().values()) {
                    if (tf.getLastUpdated() + idleTimeout < now && tf.getRaf() != null) {
                        idleInodes.add(tf.getInode());
                    }
                }
            } catch (Throwable t) {
                logger.error("Uncaught exception in IdleFileChecker thread", t);
                sourceCounter.incrementGenericProcessingFail();
            }
        }
    }

    /**
     * Position 文件维护线程
     */
    private class PositionWriterRunnable implements Runnable {
        @Override
        public void run() {
            writePosition();
        }
    }

    private void writePosition() {
        File file = new File(positionFilePath);
        FileWriter writer = null;
        try {
            writer = new FileWriter(file);
            if (!existingInodes.isEmpty()) {
                String json = toPosInfoJson();
                writer.write(json);
            }
        } catch (Throwable t) {
            logger.error("Failed writing positionFile", t);
            sourceCounter.incrementGenericProcessingFail();
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
            } catch (IOException e) {
                logger.error("Error: " + e.getMessage(), e);
                sourceCounter.incrementGenericProcessingFail();
            }
        }
    }

    private String toPosInfoJson() {
        @SuppressWarnings("rawtypes")
        List<Map> posInfos = Lists.newArrayList();
        for (Long inode : existingInodes) {
            HermesTailFile tf = reader.getTailFiles().get(inode);
            // 同步至mysql
            try {
                savePostionToMysql(tf, inode);
            } catch (Exception e) {
                logger.error("sync origin offset fail", e);
            }
            posInfos.add(ImmutableMap.of("inode", inode, "pos", tf.getPos(), "file", tf.getPath(), "lineNumber",
                    tf.getLineNumber()));

        }
        return new Gson().toJson(posInfos);
    }

    /**
     * POST: Base64解码 --> Gzip解压 --> 裂变成多个Event
     * GET:  Base64解码 --> 变化成一个Event
     *
     * @param events
     * @return
     */
    private List<Event> explode(HermesTailFile tf, List<Event> events) {
        return events.parallelStream().flatMap(e -> {
            ArrayList<Event> re = Lists.newArrayList();
            InputStream input = null;
            Reader reader = null;
            String content = null;
            try {
                if (e.getBody() != null && e.getBody().length > 0) {
                    Gson jsonDeserializer = new Gson();
                    content = new String(e.getBody()).replace("\\", "\\\\");
                    Request request = jsonDeserializer.fromJson(content, Request.class);

                    String body = request.getBody();
                    String requestMethod = request.getRequest_method();
                    String requetMethod = request.getRequet_method();
                    if (StringUtils.isNotBlank(body)) {

                        input = new ByteArrayInputStream(Base64.decodeBase64(body));

                        // 埋点数据
                        if (StringUtils.isEmpty(request.getPartner()) || StringUtils.isEmpty(request.getTbl())) {

                            if (StringUtils.endsWithIgnoreCase(requestMethod, "GET") ||
                                    StringUtils.endsWithIgnoreCase(requetMethod, "GET")) {
                                //GET 请求
                                reader = new InputStreamReader(input);
                                JsonElement jsonElement = jsonDeserializer.fromJson(reader, JsonElement.class);
                                Event event = jsonObjectConvertEvent(jsonDeserializer, jsonElement.getAsJsonObject(), request, e);
                                if (event != null) {
                                    re.add(event);
                                }
                            } else {
                                //POST 请求
                                reader = new InputStreamReader(new GZIPInputStream(input));
                                JsonArray jsonElements = jsonDeserializer.fromJson(reader, JsonArray.class);

                                for (JsonElement element : jsonElements) {
                                    JsonObject record = element.getAsJsonObject();
                                    Event event = jsonObjectConvertEvent(jsonDeserializer, record, request, e);
                                    if (event != null) {
                                        re.add(event);
                                    }
                                }
                            }
                            // 业务方 数据
                        } else {
                            reader = new InputStreamReader(input);
                            JsonArray jsonElements = jsonDeserializer.fromJson(reader, JsonArray.class);
                            for (JsonElement element : jsonElements) {
                                JsonObject record = element.getAsJsonObject();
                                Event event = jsonObjectConvertEvent2(jsonDeserializer, record, request, e);
                                if (event != null) {
                                    re.add(event);
                                }
                            }


                        }


                    } else {
                        logger.warn("request body is null");
                    }
                }
            } catch (Exception e1) {
                logger.error("read record failed:", e1);
                errorContentLogger.error("[" + e1.getMessage() + "]==>" + content);
            } finally {
                IOUtils.closeQuietly(reader);
                IOUtils.closeQuietly(input);
            }
            return re.stream();
        }).filter(Objects::nonNull).collect(Collectors.toList());
    }

    /**
     * jsonObject 转成Event
     *
     * @param jsonDeserializer
     * @param record
     * @param request
     * @param e
     * @return
     */
    private Event jsonObjectConvertEvent(Gson jsonDeserializer, JsonObject record, Request request, Event e) {
        if (record != null) {
            record.addProperty("ip", request.getIp());
            record.addProperty("server_time",
                    Long.parseLong(StringUtils.replaceOnce(request.getServer_time(), ".", "")));
            record.addProperty("request_method", request.getRequest_method());
            record.addProperty("ua", request.getUa());
            Map<String, String> headers = e.getHeaders();

            // business  业务方信息 , 埋点信息
//            headers.put("business", "userTrack");

            JsonElement headerElement = jsonDeserializer.toJsonTree(headers);
            record.add("meta", headerElement);
            String content = jsonDeserializer.toJson(record);
            if (content.getBytes().length > MAX_MSG_BYTES) {
                if (content.getBytes().length <= IGNORE_MSG_BYTES) {
                    logger.warn("[msg body bytes:{} greater than {}]", content.getBytes().length, MAX_MSG_BYTES);
                    errorContentLogger.error("[msg body bytes:{} greater than {}]=>{}", content.getBytes().length, MAX_MSG_BYTES, content);
                } else {
                    logger.warn("[ignore msg body bytes:{} greater than {}]", content.getBytes().length, IGNORE_MSG_BYTES);
                }
            } else {
                return EventBuilder.withBody(content.getBytes());
            }
        }
        return null;
    }


    private Event jsonObjectConvertEvent2(Gson jsonDeserializer, JsonObject record, Request request, Event e) {
        if (record != null) {
            record.addProperty("ip", request.getIp());
            record.addProperty("server_time",
                    Long.parseLong(StringUtils.replaceOnce(request.getServer_time(), ".", "")));
            record.addProperty("request_method", request.getRequest_method());
            record.addProperty("ua", request.getUa());
//            e.getHeaders().put("", "");
            record.addProperty("partner", request.getPartner());
            record.addProperty("tbl", request.getTbl());

            Map<String, String> headers = e.getHeaders();

            // business  业务方信息
            headers.put("business", request.getPartner());
            JsonElement headerElement = jsonDeserializer.toJsonTree(headers);
            record.add("meta", headerElement);

            String content = jsonDeserializer.toJson(record);
            if (content.getBytes().length > MAX_MSG_BYTES) {
                if (content.getBytes().length <= IGNORE_MSG_BYTES) {
                    logger.warn("[msg body bytes:{} greater than {}]", content.getBytes().length, MAX_MSG_BYTES);
                    errorContentLogger.error("[msg body bytes:{} greater than {}]=>{}", content.getBytes().length, MAX_MSG_BYTES, content);
                } else {
                    logger.warn("[ignore msg body bytes:{} greater than {}]", content.getBytes().length, IGNORE_MSG_BYTES);
                }
            } else {
                return EventBuilder.withBody(content.getBytes());
            }
        }
        return null;
    }

    /**
     * 保护Offset现场，异常时候降级处理
     */
    private void savePostionToMysql(HermesTailFile tf, long inode) {
        String path = tf.getPath();
        long pos = tf.getPos();
        long lineNumber = tf.getLineNumber();
        String uniqueFlag = DigestUtils.md5Hex(host + ":" + path);
        String isExistSQL = "SELECT * FROM hermes_flume_position WHERE unique_flag=?";
        String updateSQL = "UPDATE hermes_flume_position SET host=?,path=?,inode=?,pos=?,line_number=?,gmt_modified=? WHERE unique_flag=?";
        String insertSQL = "INSERT INTO hermes_flume_position(host,path,inode,pos,line_number,unique_flag,gmt_created) VALUES(?,?,?,?,?,?,?)";
        Position position = mysqlBaseDao.query(isExistSQL, uniqueFlag);
        if (position != null) {
            if (position.getPos() != pos && position.getPos() < pos && pos != 0) {
                mysqlBaseDao.update(updateSQL, host, path, inode, pos, lineNumber, new Date(), uniqueFlag);
            }
        } else {
            mysqlBaseDao.save(insertSQL, host, path, inode, pos, lineNumber, uniqueFlag, new Date());
        }
    }
}
