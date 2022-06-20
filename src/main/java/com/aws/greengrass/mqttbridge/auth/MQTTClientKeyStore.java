/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqttbridge.auth;

import com.aws.greengrass.device.ClientDevicesAuthServiceApi;
import com.aws.greengrass.device.api.CertificateUpdateEvent;
import com.aws.greengrass.device.api.GetCertificateRequest;
import com.aws.greengrass.device.api.GetCertificateRequestOptions;
import com.aws.greengrass.device.exception.CertificateGenerationException;
import com.aws.greengrass.logging.api.Logger;
import com.aws.greengrass.logging.impl.LogManager;
import com.aws.greengrass.mqttbridge.MQTTBridge;
import lombok.AccessLevel;
import lombok.Getter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

public class MQTTClientKeyStore {
    private static final Logger LOGGER = LogManager.getLogger(MQTTClientKeyStore.class);
    static final char[] DEFAULT_KEYSTORE_PASSWORD = "".toCharArray();
    static final String KEY_ALIAS = "aws-greengrass-mqttbridge";

    @Getter(AccessLevel.PACKAGE)
    private KeyStore keyStore;
    private final ClientDevicesAuthServiceApi clientDevicesAuthServiceApi;
    private final List<UpdateListener> updateListeners = new CopyOnWriteArrayList<>();
    private final GetCertificateRequest clientCertificateRequest;

    @FunctionalInterface
    public interface UpdateListener {
        void onCAUpdate();
    }

    /**
     * Constructor for MQTTClient KeyStore.
     *
     * @param clientDevicesAuthServiceApi client devices auth api for subscribing to cert updates
     */
    @Inject
    public MQTTClientKeyStore(ClientDevicesAuthServiceApi clientDevicesAuthServiceApi) {
        GetCertificateRequestOptions options = new GetCertificateRequestOptions();
        options.setCertificateType(GetCertificateRequestOptions.CertificateType.CLIENT);
        this.clientCertificateRequest = new GetCertificateRequest(MQTTBridge.SERVICE_NAME, options, this::updateCert);
        this.clientDevicesAuthServiceApi = clientDevicesAuthServiceApi;
    }

    /**
     * Initialize keypair and keystore and subscribe to cert updates.
     *
     * @throws KeyStoreException              if unable to generate keypair or load keystore
     * @throws CertificateGenerationException if unable to request a client certificate
     */
    public void init() throws KeyStoreException, CertificateGenerationException {
        keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try {
            keyStore.load(null, DEFAULT_KEYSTORE_PASSWORD);
        } catch (IOException | NoSuchAlgorithmException | CertificateException e) {
            throw new KeyStoreException("Unable to load keystore", e);
        }

        clientDevicesAuthServiceApi.subscribeToCertificateUpdates(clientCertificateRequest);
    }

    /**
     * Shutdown client key store.
     */
    public void shutdown() {
        clientDevicesAuthServiceApi.unsubscribeFromCertificateUpdates(clientCertificateRequest);
    }

    private void updateCert(CertificateUpdateEvent certificateUpdate) {
        try {
            LOGGER.atDebug().log("Storing new client certificate to be used on next connect attempt");
            X509Certificate[] certChain = Stream.concat(
                            Stream.of(certificateUpdate.getCertificate()),
                            Arrays.stream(certificateUpdate.getCaCertificates()))
                    .toArray(X509Certificate[]::new);
            keyStore.setKeyEntry(
                    KEY_ALIAS, certificateUpdate.getKeyPair().getPrivate(), DEFAULT_KEYSTORE_PASSWORD, certChain);
        } catch (KeyStoreException e) {
            LOGGER.atError().log("Unable to store generated cert", e);
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
