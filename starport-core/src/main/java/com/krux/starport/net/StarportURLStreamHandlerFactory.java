package com.krux.starport.net;

import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;

public class StarportURLStreamHandlerFactory implements URLStreamHandlerFactory {

    /**
     * Registers StarportURLStreamHandlerFactory as the application's URLStreamHandlerFactory.
     * Note that it uses URL.setURLStreamHandlerFactory() to register.
     *
     * @return true if StarportURLStreamHandlerFactory is set properly;
     *         false if the URLStreamHandlerFactory has already been set in the application's JVM.
     */
    public static synchronized boolean register()
            throws NoSuchFieldException, IllegalAccessException {
        URLStreamHandlerFactory urlStreamHandlerFactory = getUrlStreamHandlerFactory();

        // Ensure that URL.setURLStreamHandlerFactory() is only called once within a JVM
        // See https://docs.oracle.com/javase/7/docs/api/index.html?java/net/URL.html
        if (urlStreamHandlerFactory != null)
            return false;

        URL.setURLStreamHandlerFactory(new StarportURLStreamHandlerFactory());
        return true;
    }

    @Override
    public URLStreamHandler createURLStreamHandler(String protocol) {
        if ("s3".equals(protocol)) {
            return new sun.net.www.protocol.s3.Handler();
        }
        if("ssm".equals(protocol)) {
            return new sun.net.www.protocol.ssm.Handler();
        }

        return null;
    }

    private static synchronized URLStreamHandlerFactory getUrlStreamHandlerFactory()
            throws NoSuchFieldException,IllegalAccessException {
        Field factoryField = URL.class.getDeclaredField("factory");
        factoryField.setAccessible(true);
        return (URLStreamHandlerFactory) factoryField.get(null);
    }
}
