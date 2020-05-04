package org.poma.scanning.visibility;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;
import org.apache.accumulo.core.iterators.system.VisibilityFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

public class OptionVIsibilityIterator  extends WrappingIterator implements OptionDescriber  {
    public static final String AUTHORIZATIONS_OPT = "authorizations";
    private SortedKeyValueIterator<Key, Value> delegate;
    private static final Logger log = Logger.getLogger(OptionVIsibilityIterator.class);

    public OptionVIsibilityIterator() {
    }

    public OptionVIsibilityIterator(SortedKeyValueIterator<Key, Value> iterator, Authorizations authorizations, byte[] defaultVisibility) {
        this.setSource(iterator);
    }

    public OptionVIsibilityIterator(OptionVIsibilityIterator other, IteratorEnvironment env) {
        this.delegate = other.delegate.deepCopy(env);
        this.setSource(this.delegate);
    }

    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env) throws IOException {
        Authorizations auths = Authorizations.EMPTY;
        if (options.containsKey("authorizations")) {
            auths = new Authorizations(((String)options.get("authorizations")).split(","));
        }

        log.debug("Using authorizations: " + auths);
        this.delegate = VisibilityFilter.wrap(source, auths, new byte[0]);
        super.init(this.delegate, options, env);
    }

    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        return new OptionVIsibilityIterator(this, env);
    }

    public IteratorOptions describeOptions() {
        IteratorOptions opts = new IteratorOptions(this.getClass().getSimpleName(), "Filters keys based to return only those whose visibility tests positive against the supplied authorizations", (Map)null, (List)null);
        opts.addNamedOption("authorizations", "Comma delimited list of scan authorizations");
        return opts;
    }

    public boolean validateOptions(Map<String, String> options) {
        boolean valid = false;
        String auths = (String)options.get("authorizations");
        if (auths != null) {
            try {
                new Authorizations(auths.split(","));
                valid = true;
            } catch (Exception var5) {
            }
        }

        return valid;
    }
}
