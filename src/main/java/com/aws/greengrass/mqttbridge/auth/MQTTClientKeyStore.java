/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.auth;

import com.aws.greengrass.certificatemanager.CertificateManager;
import com.aws.greengrass.certificatemanager.certificate.CertificateRequestGenerator;
import com.aws.greengrass.certificatemanager.certificate.CsrProcessingException;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import lombok.AccessLevel;
import lombok.Getter;
import org.bouncycastle.operator.OperatorCreationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.inject.Inject;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

public class MQTTClientKeyStore {
    private static final Logger LOGGER = LogManager.getLogger(MQTTClientKeyStore.class);
    static final char[] DEFAULT_KEYSTORE_PASSWORD = "".toCharArray();
    private static final String DEFAULT_CN = "aws-greengrass-mqttbridge";
    static final String KEY_ALIAS = "aws-greengrass-mqttbridge";
    private static final String RSA_KEY_INSTANCE = "RSA";
    private static final int RSA_KEY_LENGTH = 2048;

    @Getter(AccessLevel.PACKAGE)
    private KeyStore keyStore;

    private KeyPair keyPair;

    private final CertificateManager certificateManager;

    private final List<UpdateListener> updateListeners = new CopyOnWriteArrayList<>();

    @FunctionalInterface
    public interface UpdateListener {
        void onCAUpdate();
    }

    /**
     * Constructor for MQTTClient KeyStore.
     *
     * @param certificateManager certificate manager for subscribing to cert updates
     */
    @Inject
    public MQTTClientKeyStore(CertificateManager certificateManager) {
        this.certificateManager = certificateManager;
    }

    /**
     * Initialize keypair and keystore and subscribe to cert updates.
     *
     * @throws CsrProcessingException if unable to subscribe with csr
     * @throws KeyStoreException      if unable to generate keypair or load keystore
     * @throws CsrGeneratingException if unable to generate csr
     */
    public void init() throws CsrProcessingException, KeyStoreException, CsrGeneratingException {
        try {
            keyPair = newRSAKeyPair();
        } catch (NoSuchAlgorithmException e) {
            throw new KeyStoreException("unable to generate keypair for key store", e);
        }

        keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try {
            keyStore.load(null, DEFAULT_KEYSTORE_PASSWORD);
        } catch (IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new KeyStoreException("Unable to load keystore", e);
        }

        String csr;
        try {
            //client cert doesn't require SANs
            csr = CertificateRequestGenerator.createCSR(keyPair, DEFAULT_CN, null,  null);
        } catch (IOException | OperatorCreationException e) {
            throw new CsrGeneratingException("Unable to generate CSR from keypair", e);
        }
        certificateManager.subscribeToClientCertificateUpdates(csr, this::updateCert);
    }

    private KeyPair newRSAKeyPair() throws NoSuchAlgorithmException {
        KeyPairGenerator kpg = KeyPairGenerator.getInstance(RSA_KEY_INSTANCE);
        kpg.initialize(RSA_KEY_LENGTH);
        return kpg.generateKeyPair();
    }

    private void updateCert(X509Certificate... certChain) {
        try {
            LOGGER.atDebug().log("Storing new client certificate to be used on next connect attempt");
            keyStore.setKeyEntry(KEY_ALIAS, keyPair.getPrivate(), DEFAULT_KEYSTORE_PASSWORD, certChain);
        } catch (KeyStoreException e) {
            LOGGER.atError("Unable to store generated cert", e);
        }
    }

    /**
     * Update CA in keystore.
     *
     * @param caCerts CA to trust MQTT broker
     * @throws IOException          if unable to read cert pem
     * @throws CertificateException if unable to generate cert from pem
     * @throws KeyStoreException    if unable to store cert in keystore
     */
    public void updateCA(List<String> caCerts) throws IOException, CertificateException, KeyStoreException {
        //Delete existing CAs
        Enumeration<String> entries = keyStore.aliases();
        while (entries.hasMoreElements()) {
            String alias = entries.nextElement();
            if (keyStore.isCertificateEntry(alias)) {
                keyStore.deleteEntry(alias);
            }
        }

        for (int i = 0; i < caCerts.size(); i++) {
            X509Certificate caCert = pemToX509Certificate(caCerts.get(i));
            keyStore.setCertificateEntry("CA" + i, caCert);
        }

        updateListeners.forEach(UpdateListener::onCAUpdate); //notify MQTTClient
    }

    private X509Certificate pemToX509Certificate(String certPem) throws IOException, CertificateException {
        byte[] certBytes = certPem.getBytes(StandardCharsets.UTF_8);
        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
        X509Certificate cert;
        try (InputStream certStream = new ByteArrayInputStream(certBytes)) {
            cert = (X509Certificate) certFactory.generateCertificate(certStream);
        }
        return cert;
    }

    /**
     * Add listener to listen to KeyStore updates.
     * @param listener listener method
     */
    public synchronized void listenToCAUpdates(UpdateListener listener) {
        updateListeners.add(listener);
    }

    /**
     * Gets SSL Socket Factory from Key Store.
     *
     * @return SSLSocketFactory
     * @throws KeyStoreException if unable to create Socket Factory
     */
    public SSLSocketFactory getSSLSocketFactory() throws KeyStoreException {
        try {
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keyStore, DEFAULT_KEYSTORE_PASSWORD);

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(keyStore);

            SSLContext sc = SSLContext.getInstance("TLS");
            sc.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            return sc.getSocketFactory();
        } catch (NoSuchAlgorithmException | UnrecoverableKeyException | KeyManagementException e) {
            throw new KeyStoreException("Unable to create SocketFactory from KeyStore", e);
        }
    }
}
