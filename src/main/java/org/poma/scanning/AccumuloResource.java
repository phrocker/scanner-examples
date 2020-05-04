package org.poma.scanning;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

/**
 * Purpose: Basic Iterable resource. Contains the connector from which we will create the scanners.
 *
 * Justification: This class should contain all resources that will be used to identify a given scanner session. The batchScanner can be returned as a
 * BatchScanner resource. ScannerResource is an immutable resource. While we could make them mutable, their purpose is to maintain history of scan sessions so
 * that can do runtime analysis, if desired.
 *
 * Design: Is closeable for obvious reasons, is an iterable so that instead of exposing the specific underlying accumulo resource, we return an iterator. While
 * this isn't entirely necessary, it does allow us to better inject test code.
 *
 */
public class AccumuloResource implements Closeable, Iterable<Entry<Key,Value>> {

    /**
     * Our connector.
     */
    private Connector connector;

    public AccumuloResource(final Connector cxn) {
        Preconditions.checkNotNull(cxn);

        connector = cxn;
    }

    public AccumuloResource(final AccumuloResource other) {
        // deep copy
    }

    protected Connector getConnector() {
        return connector;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
        // nothing to close.
    }

    protected void init(final String tableName, final Set<Authorizations> auths, Collection<Range> currentRange) throws TableNotFoundException {
        // do nothing.
    }

    /**
     * Sets the option on this currently running resource.
     *
     * @param options
     * @return
     */
    public AccumuloResource setOptions(SessionOptions options) {

        return this;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<Entry<Key,Value>> iterator() {
        return Iterators.emptyIterator();
    }

    public static final class ResourceFactory {

        /**
         * Initializes a resource after it was delegated.
         *
         * @param clazz
         * @param baseResource
         * @param tableName
         * @param auths
         * @param currentRange
         * @return
         * @throws TableNotFoundException
         */
        public static <T> AccumuloResource initializeResource(Class<T> clazz, AccumuloResource baseResource, final String tableName,
                                                              final Set<Authorizations> auths, Range currentRange) throws TableNotFoundException {
            return initializeResource(clazz, baseResource, tableName, auths, Collections.singleton(currentRange));
        }

        public static <T> AccumuloResource initializeResource(Class<T> clazz, AccumuloResource baseResource, final String tableName,
                                                              final Set<Authorizations> auths, Collection<Range> currentRange) throws TableNotFoundException {

            AccumuloResource newResource = null;
            try {
                newResource = (AccumuloResource) clazz.getConstructor(AccumuloResource.class).newInstance(baseResource);
                newResource.init(tableName, auths, currentRange);
            } catch (IllegalArgumentException | NoSuchMethodException | InvocationTargetException | IllegalAccessException | InstantiationException
                    | SecurityException e) {
                throw new RuntimeException(e);
            }

            return newResource;
        }

    }

}
