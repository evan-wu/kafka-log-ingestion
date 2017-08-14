package com.tazhi.log.ingestion.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link ThreadFactory} that sets names to the threads created by this factory. Threads created by this factory
 * will take names in the form of the string <code>namePrefix + " thread-" + threadNum</code> where <tt>threadNum</tt> is the 
 * count of threads created by this type of factory.
 * 
 * @author <a href="mailto:asanoujam@terracottatech.com">Abhishek Sanoujam</a>
 * 
 */
public class NamedThreadFactory implements ThreadFactory {

    private static AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    /**
     * Constructor accepting the prefix of the threads that will be created by this {@link ThreadFactory}
     * 
     * @param namePrefix
     *            Prefix for names of threads
     */
    public NamedThreadFactory(String namePrefix) {
        this.namePrefix = namePrefix;
    }

    /**
     * Returns a new thread using a name as specified by this factory {@inheritDoc}
     */
    public Thread newThread(Runnable runnable) {
        return new Thread(runnable, namePrefix + " thread-" + threadNumber.getAndIncrement());
    }

}
