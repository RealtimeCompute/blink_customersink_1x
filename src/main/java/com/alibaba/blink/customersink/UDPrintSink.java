package com.alibaba.blink.customersink;

import com.alibaba.blink.streaming.connector.custom.api.CustomSinkBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class UDPrintSink extends CustomSinkBase {
    private static Logger LOG = LoggerFactory.getLogger(UDPrintSink.class);

    public void open(int taskNumber, int numTasks) throws IOException {
        LOG.info(String.format("Open Method Called: taskNumber %d numTasks %d", taskNumber, numTasks));
        String[] filedNames = rowTypeInfo.getFieldNames();
        TypeInformation[] typeInformations = rowTypeInfo.getFieldTypes();
        LOG.info(String.format("Open Method Called: filedNames %d typeInformations %d", filedNames.length,
                typeInformations.length));
        int i = 0;
        for(TypeInformation typeInformation:typeInformations) {
            Class clazz = typeInformation.getTypeClass();
            LOG.info("Open Method Clazz:" + clazz);
            if (byte[].class.equals(clazz)) {
                LOG.info(filedNames[i] + ":" + clazz);
            } else if (String.class.equals(clazz)) {
                LOG.info(filedNames[i] + ":" +clazz);
            } else if (Byte.class.equals(clazz)) {
                LOG.info(filedNames[i] + ":" + clazz);
            } else if (Short.class.equals(clazz)) {
                LOG.info(filedNames[i] + ":" + clazz);
            } else if (Integer.class.equals(clazz)) {
                LOG.info(filedNames[i] + ":" + clazz);
            } else if (Long.class.equals(clazz)) {
                LOG.info(filedNames[i] + ":" + clazz);
            } else if (Float.class.equals(clazz)) {
                LOG.info(filedNames[i] + ":" + clazz);
            } else if (Double.class.equals(clazz)) {
                LOG.info(filedNames[i] + ":" + clazz);
            } else if (Boolean.class.equals(clazz)) {
                LOG.info(filedNames[i] + ":" + clazz);
            } else if (Timestamp.class.equals(clazz)) {
                LOG.info(filedNames[i] + ":" + clazz);
            } else if (Date.class.equals(clazz)) {
                LOG.info(filedNames[i] + ":" + clazz);
            } else if (Time.class.equals(clazz)) {
                LOG.info(filedNames[i] + ":" + clazz);
            } else if (BigDecimal.class.equals(clazz)) {
                LOG.info(filedNames[i] + ":" + clazz);
            } else if (BigInteger.class.equals(clazz)) {
                LOG.info(filedNames[i] + ":" + clazz);
            } else {
                LOG.info("Unsupported: " + clazz);
            }
            i ++;
        }

    }

    public void close() throws IOException {
        LOG.info(String.format("Close Method Called"));
    }

    public void writeAddRecord(Row row) throws IOException {
        LOG.info("Write: " + row.toString());
    }

    public void writeDeleteRecord(Row row) throws IOException {
        LOG.info("Delete: " + row.toString());
    }

    public void sync() throws IOException {
        //没有做攒批写入，空置该方法
    }

    public String getName() {
        return "UDPrintSink";
    }
}
