package org.poma.scanning;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.data.thrift.IterInfo;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ScannerDelegate implements ScannerBase {
    private static final Logger log = LoggerFactory.getLogger(ScannerDelegate.class);
    private static final String SYSTEM_ITERATOR_NAME_PREFIX = "sys_";
    protected final ScannerBase delegate;

    public ScannerDelegate(ScannerBase delegate) {
        this.delegate = delegate;
    }

    public void addScanIterator(IteratorSetting cfg) {
        if (cfg.getName().startsWith("sys_")) {
            throw new IllegalArgumentException("Non-system iterators' names cannot start with sys_");
        } else {
            this.delegate.addScanIterator(cfg);
        }
    }

    public void addSystemScanIterator(IteratorSetting cfg) {
        if (!cfg.getName().startsWith("sys_")) {
            cfg.setName("sys_" + cfg.getName());
        }

        this.delegate.addScanIterator(cfg);
    }

    public void removeScanIterator(String iteratorName) {
        if (iteratorName.startsWith("sys_")) {
            throw new IllegalArgumentException("DATAWAVE system iterator " + iteratorName + " cannot be removed");
        } else {
            this.delegate.removeScanIterator(iteratorName);
        }
    }

    public void removeSystemScanIterator(String iteratorName) {
        if (!iteratorName.startsWith("sys_")) {
            iteratorName = "sys_" + iteratorName;
        }

        this.delegate.removeScanIterator(iteratorName);
    }

    public void updateScanIteratorOption(String iteratorName, String key, String value) {
        if (iteratorName.startsWith("sys_")) {
            throw new IllegalArgumentException("DATAWAVE system iterator " + iteratorName + " cannot be updated");
        } else {
            this.delegate.updateScanIteratorOption(iteratorName, key, value);
        }
    }

    public void updateSystemScanIteratorOption(String iteratorName, String key, String value) {
        if (!iteratorName.startsWith("sys_")) {
            iteratorName = "sys_" + iteratorName;
        }

        this.delegate.updateScanIteratorOption(iteratorName, key, value);
    }

    public void fetchColumnFamily(Text col) {
        this.delegate.fetchColumnFamily(col);
    }

    public void fetchColumn(Text colFam, Text colQual) {
        this.delegate.fetchColumn(colFam, colQual);
    }

    public void fetchColumn(IteratorSetting.Column column) {
        this.delegate.fetchColumn(column);
    }

    public void clearColumns() {
        this.delegate.clearColumns();
    }

    public void clearScanIterators() {
        if (this.delegate instanceof ScannerOptions) {
            ScannerDelegate.ScannerOptionsHelper opts = new ScannerDelegate.ScannerOptionsHelper((ScannerOptions)this.delegate);
            Iterator var2 = opts.getIterators().iterator();

            while(var2.hasNext()) {
                IteratorSetting iteratorSetting = (IteratorSetting)var2.next();
                if (!iteratorSetting.getName().startsWith("sys_")) {
                    this.delegate.removeScanIterator(iteratorSetting.getName());
                }
            }

        } else {
            throw new UnsupportedOperationException("Cannot clear scan iterators on a non-ScannerOptions class! (" + this.delegate.getClass() + ")");
        }
    }

    public void clearSystemScanIterators() {
        this.delegate.clearScanIterators();
    }

    public Iterator<Map.Entry<Key, Value>> iterator() {
        return this.delegate.iterator();
    }

    public void setTimeout(long timeOut, TimeUnit timeUnit) {
        this.delegate.setTimeout(timeOut, timeUnit);
    }

    public long getTimeout(TimeUnit timeUnit) {
        return this.delegate.getTimeout(timeUnit);
    }

    public void close() {
        this.delegate.close();
    }

    public Authorizations getAuthorizations() {
        return this.delegate.getAuthorizations();
    }

    public void setSamplerConfiguration(SamplerConfiguration samplerConfiguration) {
        this.delegate.setSamplerConfiguration(samplerConfiguration);
    }

    public SamplerConfiguration getSamplerConfiguration() {
        return this.delegate.getSamplerConfiguration();
    }

    public void clearSamplerConfiguration() {
        this.delegate.clearSamplerConfiguration();
    }

    public void setBatchTimeout(long l, TimeUnit timeUnit) {
        this.delegate.setBatchTimeout(l, timeUnit);
    }

    public long getBatchTimeout(TimeUnit timeUnit) {
        return this.delegate.getBatchTimeout(timeUnit);
    }

    public void setClassLoaderContext(String s) {
        this.delegate.setClassLoaderContext(s);
    }

    public void clearClassLoaderContext() {
        this.delegate.clearClassLoaderContext();
    }

    public String getClassLoaderContext() {
        return this.delegate.getClassLoaderContext();
    }

    public void setContext(String context) {
        this.delegate.setClassLoaderContext(context);
    }

    public void clearContext() {
        this.delegate.clearClassLoaderContext();
    }

    public String getContext() {
        return this.delegate.getClassLoaderContext();
    }

    private static class ScannerOptionsHelper extends ScannerOptions {
        public ScannerOptionsHelper(ScannerOptions other) {
            super(other);
        }

        public Collection<IteratorSetting> getIterators() {
            Collection<IteratorSetting> settings = Lists.newArrayList();
            Iterator var2 = this.serverSideIteratorList.iterator();

            while(var2.hasNext()) {
                IterInfo iter = (IterInfo)var2.next();
                IteratorSetting setting = new IteratorSetting(iter.getPriority(), iter.getIterName(), iter.getClassName());
                setting.addOptions((Map)this.serverSideIteratorOptions.get(iter.getIterName()));
                settings.add(setting);
            }

            return settings;
        }
    }
}

