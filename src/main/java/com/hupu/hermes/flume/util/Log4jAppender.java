package com.hupu.hermes.flume.util;

import org.apache.log4j.Priority;
import org.apache.log4j.RollingFileAppender;

public class Log4jAppender extends RollingFileAppender {
    @Override
    public boolean isAsSevereAsThreshold(Priority priority) {
        Priority threshold = this.getThreshold();
        if (threshold != null) {
            return threshold.equals(priority);
        } else {
            return super.isAsSevereAsThreshold(priority);
        }
    }
}
