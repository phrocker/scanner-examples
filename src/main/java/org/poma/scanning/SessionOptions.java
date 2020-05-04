package org.poma.scanning;

import com.google.common.collect.Lists;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.impl.ScannerOptions;
import org.apache.accumulo.core.data.thrift.IterInfo;

import java.util.Collection;

/**
 * Extension to allow an open constructor
 *
 * Justification: constructor
 */
public class SessionOptions extends ScannerOptions {

   private String accumuloPassword;

    public SessionOptions() {
        super();
    }

    public SessionOptions(SessionOptions other) {
        super(other);
        this.accumuloPassword = other.accumuloPassword;
    }


    public void setAccumuloPassword(final String accumuloPassword) {
        this.accumuloPassword = accumuloPassword;
    }

    public String getAccumuloPassword() {
        return accumuloPassword;
    }

    public Collection<IteratorSetting> getIterators() {

        Collection<IteratorSetting> settings = Lists.newArrayList();
        for (IterInfo iter : serverSideIteratorList) {
            IteratorSetting setting = new IteratorSetting(iter.getPriority(), iter.getIterName(), iter.getClassName());
            setting.addOptions(serverSideIteratorOptions.get(iter.getIterName()));
            settings.add(setting);
        }
        return settings;
    }
}