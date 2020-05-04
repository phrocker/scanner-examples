package org.poma.scanning.visibility;


import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.poma.scanning.ScannerDelegate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class VisibilityHelper {
    private static final Logger logger = LoggerFactory.getLogger(VisibilityHelper.class);

    public VisibilityHelper() {
    }

    public static Scanner createScanner(Connector connector, String tableName, Collection<Authorizations> authorizations) throws TableNotFoundException {
        if (authorizations != null && !authorizations.isEmpty()) {
            Iterator<Authorizations> iter = AuthorizationsMinimizer.minimize(authorizations).iterator();
            Scanner scanner = connector.createScanner(tableName, (Authorizations)iter.next());
            addVisibilityFilters(iter, scanner);
            return scanner;
        } else {
            throw new IllegalArgumentException("Authorizations must not be empty.");
        }
    }

    public static BatchScanner createBatchScanner(Connector connector, String tableName, Collection<Authorizations> authorizations, int numQueryThreads) throws TableNotFoundException {
        if (authorizations != null && !authorizations.isEmpty()) {
            Iterator<Authorizations> iter = AuthorizationsMinimizer.minimize(authorizations).iterator();
            BatchScanner batchScanner = connector.createBatchScanner(tableName, (Authorizations)iter.next(), numQueryThreads);
            addVisibilityFilters(iter, batchScanner);
            return batchScanner;
        } else {
            throw new IllegalArgumentException("Authorizations must not be empty.");
        }
    }

    public static BatchDeleter createBatchDeleter(Connector connector, String tableName, Collection<Authorizations> authorizations, int numQueryThreads, long maxMemory, long maxLatency, int maxWriteThreads) throws TableNotFoundException {
        if (authorizations != null && !authorizations.isEmpty()) {
            Iterator<Authorizations> iter = AuthorizationsMinimizer.minimize(authorizations).iterator();
            BatchWriterConfig bwCfg = (new BatchWriterConfig()).setMaxLatency(maxLatency, TimeUnit.MILLISECONDS).setMaxMemory(maxMemory).setMaxWriteThreads(maxWriteThreads);
            BatchDeleter batchDeleter = connector.createBatchDeleter(tableName, (Authorizations)iter.next(), numQueryThreads, bwCfg);
            addVisibilityFilters(iter, batchDeleter);
            return batchDeleter;
        } else {
            throw new IllegalArgumentException("Authorizations must not be empty.");
        }
    }

    protected static void addVisibilityFilters(Iterator<Authorizations> iter, ScannerBase scanner) {
        for(int priority = 10; iter.hasNext(); ++priority) {
            IteratorSetting cfg = new IteratorSetting(priority, OptionVIsibilityIterator.class);
            cfg.setName("visibilityFilter" + priority);
            cfg.addOption("authorizations", ((Authorizations)iter.next()).toString());
            if (scanner instanceof ScannerDelegate) {
                ((ScannerDelegate)scanner).addSystemScanIterator(cfg);
            } else {
                logger.warn("Adding system visibility filter to non-wrapped scanner {}.", scanner.getClass(), new Exception());
                scanner.addScanIterator(cfg);
            }
        }

    }
}
