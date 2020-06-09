package com.hupu.hermes.flume;

import com.google.common.collect.Lists;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;

import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.BYTE_OFFSET_HEADER_KEY;

public class HermesTailFile {
    private static final Logger logger = LoggerFactory.getLogger(HermesTailFile.class);

    private static final byte BYTE_NL = (byte) 10;
    private static final byte BYTE_CR = (byte) 13;

    private static final int BUFFER_SIZE = 8192;
    private static final int NEED_READING = -1;

    private RandomAccessFile raf;
    private final String path;
    private final long inode;
    private long pos;
    private long lastUpdated;
    private boolean needTail;
    private final Map<String, String> headers;
    private byte[] buffer;
    private byte[] oldBuffer;
    private int bufferPos;
    private long lineReadPos;
    private long lineNumber;
    private long ln;
    private String host;

    public HermesTailFile(File file, Map<String, String> headers, long inode, long pos, long lineNumber, String host)
            throws IOException {
        this.raf = new RandomAccessFile(file, "r");
        if (pos > 0) {
            raf.seek(pos);
            lineReadPos = pos;
            ln = lineNumber;
        }
        this.path = file.getAbsolutePath();
        this.inode = inode;
        this.pos = pos;
        this.lastUpdated = 0L;
        this.needTail = true;
        this.headers = headers;
        this.oldBuffer = new byte[0];
        this.bufferPos = NEED_READING;
        this.lineNumber = lineNumber;
        this.host = host;
    }

    public RandomAccessFile getRaf() {
        return raf;
    }

    public String getPath() {
        return path;
    }

    public long getInode() {
        return inode;
    }

    public long getPos() {
        return pos;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public boolean needTail() {
        return needTail;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public long getLineReadPos() {
        return lineReadPos;
    }

    public void setPos(long pos) {
        this.pos = pos;
    }

    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    public void setNeedTail(boolean needTail) {
        this.needTail = needTail;
    }

    public long getLineNumber() {
        return lineNumber;
    }

    public void setLineNumber(long lineNumber) {
        this.lineNumber = lineNumber;
    }

    public void setLineReadPos(long lineReadPos) {
        this.lineReadPos = lineReadPos;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public long getLn() {
        return ln;
    }

    public void setLn(long ln) {
        this.ln = ln;
    }

    public boolean updatePos(String path, long inode, long pos, long lineNumber) throws IOException {
        if (this.inode == inode && this.path.equals(path)) {
            setPos(pos);
            setLineNumber(lineNumber);
            ln = lineNumber;
            updateFilePos(pos);
            logger.info("Updated position, file: " + path + ", inode: " + inode + ", pos: " + pos + ",ln: " + lineNumber);
            return true;
        }
        return false;
    }

    public void updateFilePos(long pos) throws IOException {
        raf.seek(pos);
        lineReadPos = pos;
        bufferPos = NEED_READING;
        oldBuffer = new byte[0];
    }

    public List<Event> readEvents(int numEvents, boolean backoffWithoutNL, boolean addByteOffset) throws IOException {
        List<Event> events = Lists.newLinkedList();
        for (int i = 0; i < numEvents; i++) {
            Event event = readEvent(backoffWithoutNL, addByteOffset);
            if (event == null) {
                break;
            }
            this.ln++;
            Map<String, String> headers = event.getHeaders();
            headers.put("line_number", String.valueOf(getLn()));
            headers.put("pos", String.valueOf(getLineReadPos()));
            headers.put("file_name", this.path);
            headers.put("host", host);
            events.add(event);
        }
        return events;
    }

    /**
     * 读取一行数据
     *
     * @param backoffWithoutNL
     * @param addByteOffset
     * @return
     * @throws IOException
     */
    private Event readEvent(boolean backoffWithoutNL, boolean addByteOffset) throws IOException {
        Long posTmp = getLineReadPos();
        HermesTailFile.LineResult line = readLine();
        if (line == null) {
            return null;
        }
        if (backoffWithoutNL && !line.lineSepInclude) {
            logger.info("Backing off in file without newline: " + path + ", inode: " + inode + ", pos: "
                    + raf.getFilePointer());
            updateFilePos(posTmp);
            return null;
        }
        Event event = EventBuilder.withBody(line.line);
        if (addByteOffset == true) {
            event.getHeaders().put(BYTE_OFFSET_HEADER_KEY, posTmp.toString());
        }
        return event;
    }

    private void readFile() throws IOException {
        if ((raf.length() - raf.getFilePointer()) < BUFFER_SIZE) {
            buffer = new byte[(int) (raf.length() - raf.getFilePointer())];
        } else {
            buffer = new byte[BUFFER_SIZE];
        }
        raf.read(buffer, 0, buffer.length);
        bufferPos = 0;
    }

    private byte[] concatByteArrays(byte[] a, int startIdxA, int lenA, byte[] b, int startIdxB, int lenB) {
        byte[] c = new byte[lenA + lenB];
        System.arraycopy(a, startIdxA, c, 0, lenA);
        System.arraycopy(b, startIdxB, c, lenA, lenB);
        return c;
    }

    public HermesTailFile.LineResult readLine() throws IOException {
        HermesTailFile.LineResult lineResult = null;
        while (true) {
            if (bufferPos == NEED_READING) {
                if (raf.getFilePointer() < raf.length()) {
                    readFile();
                } else {
                    if (oldBuffer.length > 0) {
                        lineResult = new HermesTailFile.LineResult(false, oldBuffer);
                        oldBuffer = new byte[0];
                        setLineReadPos(lineReadPos + lineResult.line.length);
                    }
                    break;
                }
            }
            for (int i = bufferPos; i < buffer.length; i++) {
                if (buffer[i] == BYTE_NL) {
                    int oldLen = oldBuffer.length;
                    // Don't copy last byte(NEW_LINE)
                    int lineLen = i - bufferPos;
                    // For windows, check for CR
                    if (i > 0 && buffer[i - 1] == BYTE_CR) {
                        lineLen -= 1;
                    } else if (oldBuffer.length > 0 && oldBuffer[oldBuffer.length - 1] == BYTE_CR) {
                        oldLen -= 1;
                    }
                    lineResult = new HermesTailFile.LineResult(true,
                            concatByteArrays(oldBuffer, 0, oldLen, buffer, bufferPos, lineLen));
                    setLineReadPos(lineReadPos + (oldBuffer.length + (i - bufferPos + 1)));
                    oldBuffer = new byte[0];
                    if (i + 1 < buffer.length) {
                        bufferPos = i + 1;
                    } else {
                        bufferPos = NEED_READING;
                    }
                    break;
                }
            }
            if (lineResult != null) {
                break;
            }
            // NEW_LINE not showed up at the end of the buffer
            oldBuffer = concatByteArrays(oldBuffer, 0, oldBuffer.length, buffer, bufferPos, buffer.length - bufferPos);
            bufferPos = NEED_READING;
        }
        return lineResult;
    }

    public void close() {
        try {
            raf.close();
            raf = null;
            long now = System.currentTimeMillis();
            setLastUpdated(now);
        } catch (IOException e) {
            logger.error("Failed closing file: " + path + ", inode: " + inode, e);
        }
    }

    private class LineResult {
        final boolean lineSepInclude;
        final byte[] line;

        public LineResult(boolean lineSepInclude, byte[] line) {
            super();
            this.lineSepInclude = lineSepInclude;
            this.line = line;
        }
    }
}
