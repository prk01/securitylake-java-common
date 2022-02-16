package org.securitylake.common.util;

/**
 * Reference wrapper. Mutable and not threadsafe.
 *
 * @param <T>
 */
public class Reference<T>
{
    private T t;

    /**
     * Constructor
     *
     * @param t
     */
    public Reference(T t)
    {
        this.t = t;
    }

    /**
     * Set the new value
     *
     * @param t
     */
    public void setT(T t)
    {
        this.t = t;
    }

    /**
     * get the new value
     *
     * @return
     */
    public T getT()
    {
        return t;
    }

    @Override
    public String toString()
    {
        return "Reference {" +
                "t=" + t +
                '}';
    }
}

