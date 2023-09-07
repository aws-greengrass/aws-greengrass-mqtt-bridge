/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.auth;

import com.aws.greengrass.clientdevices.auth.api.ClientDevicesAuthServiceApi;
import com.aws.greengrass.clientdevices.auth.api.CertificateUpdateEvent;
import com.aws.greengrass.clientdevices.auth.api.GetCertificateRequest;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
public class MQTTClientKeyStoreTest {
    private static final String CERTIFICATE = "-----BEGIN CERTIFICATE-----\r\n"
            + "MIICujCCAaICCQCQcEEQmGoJqjANBgkqhkiG9w0BAQUFADAfMR0wGwYDVQQDDBRt\r\n"
            + "b3F1ZXR0ZS5lY2xpcHNlLm9yZzAeFw0yMDA3MjExODA2MzdaFw0yMTA3MTYxODA2\r\n"
            + "MzdaMB8xHTAbBgNVBAMMFG1vcXVldHRlLmVjbGlwc2Uub3JnMIIBIjANBgkqhkiG\r\n"
            + "9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzA6+mCCxb4wcuMYo+U7n50MFfNiDoeXRdYgE\r\n"
            + "JRSqk/1Op8K5IzeMGddRhQKi4f2nojRtHak7wCj1QzJrkQRs+N8FOIMT5h0qFOzP\r\n"
            + "EJ4N39hGY+lnjsRl892zE52TzvPyvogChhth99U3KKf3d/bWjiX28edcY6/cxpKp\r\n"
            + "UN7aY4eNkdsteeWGN3l99laxDzG+uWIntwtw0S2TgyPvYVDl/oDL80uFxXApP6R5\r\n"
            + "/zBE3uGcHq+koqUP+dLiKJd5C+e2IojBxY8ACp3g47Memy66cyHBEm4OYRsejHsA\r\n"
            + "jCqhQy8UMu7u+ldg61mNSgvbU9ex0RVCsoWYn/mHQrKBVfsTYwIDAQABMA0GCSqG\r\n"
            + "SIb3DQEBBQUAA4IBAQCypHyuR3RHPUOC4bqzP//UY9RPgpwY9pw01+LCGdToTMeZ\r\n"
            + "TyakWfmC8eT+TqmRtLHHNd5pVdSYQj9F9RrOGh5WgzvG+4s13OMwdzV11kTQxc3v\r\n"
            + "r8HlEMLWbS9UIabezCBfUJfcZMr/9OY4N4S0n23Ed4hJ1xsfGnqb6bIw0JDAKZ8V\r\n"
            + "e41Mh3o4lPB0MWM9Tuq701/ZPyEDTnFsZdEwlKQ04ZfIfo5xA26eIDJvrf3ZeYuz\r\n"
            + "AgTh4Slc4H6sBg9OYPcvgVbrbO8gK1fl7B1YbtZsjut/8tYZ+OLkDkTXYS+AwFXl\r\n"
            + "220FJlnogGSU9xvjdQCXzt6p+kC4cKpQBTqshgcA\r\n"
            + "-----END CERTIFICATE-----\r\n";
    private static final byte[] BEGIN_CERT = "-----BEGIN CERTIFICATE-----\r\n".getBytes(StandardCharsets.UTF_8);
    private static final byte[] END_CERT = "\r\n-----END CERTIFICATE-----\r\n".getBytes(StandardCharsets.UTF_8);

    @Mock
    private ClientDevicesAuthServiceApi mockServiceApi;
    private KeyPair keyPair;

    @BeforeEach
    void beforeEach() throws NoSuchAlgorithmException {
        assumeTrue(System.getProperty("java.version").startsWith("1.8"));
        KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
        kpg.initialize(2048);
        keyPair = kpg.generateKeyPair();
    }

    @Test
    void GIVEN_MQTTClientKeyStore_WHEN_initialized_THEN_keyAndCertGenerated() throws Exception {
        MQTTClientKeyStore mqttClientKeyStore = new MQTTClientKeyStore(mockServiceApi);
        mqttClientKeyStore.init();

        ArgumentCaptor<GetCertificateRequest> cbArgumentCaptor = ArgumentCaptor.forClass(GetCertificateRequest.class);
        verify(mockServiceApi, times(1))
                .subscribeToCertificateUpdates(cbArgumentCaptor.capture());
        GetCertificateRequest getCertificateRequest = cbArgumentCaptor.getValue();

        KeyStore keyStore = mqttClientKeyStore.getKeyStore();
        assertThat(keyStore.size(), is(0));

        X509Certificate certificate = pemToX509Certificate(CERTIFICATE);
        CertificateUpdateEvent certificateUpdate =
                new CertificateUpdateEvent(keyPair, certificate, new X509Certificate[]{certificate});
        getCertificateRequest.getCertificateUpdateConsumer().accept(certificateUpdate);
        assertThat(keyStore.size(), is(1));

        PrivateKey privateKey = (PrivateKey) keyStore.getKey(MQTTClientKeyStore.KEY_ALIAS, MQTTClientKeyStore.DEFAULT_KEYSTORE_PASSWORD);
        assertThat(privateKey.getAlgorithm(), is("RSA"));

        assertThat(keyStore.getCertificateChain(MQTTClientKeyStore.KEY_ALIAS).length, is(2));
        verifyStoredCertificate((X509Certificate) keyStore.getCertificateChain(MQTTClientKeyStore.KEY_ALIAS)[0]);
        verifyStoredCertificate((X509Certificate) keyStore.getCertificateChain(MQTTClientKeyStore.KEY_ALIAS)[1]);
    }

    private void verifyStoredCertificate(X509Certificate cert) throws CertificateEncodingException, IOException {
        byte[] certBytes = encodeToBase64Pem(cert.getEncoded(), BEGIN_CERT, END_CERT);
        String certPem = new String(certBytes, StandardCharsets.UTF_8);
        assertThat(certPem, is(CERTIFICATE));
    }

    @Test
    void GIVEN_MQTTClientKeyStore_WHEN_called_updateCA_THEN_CA_stored() throws Exception {
        MQTTClientKeyStore mqttClientKeyStore = new MQTTClientKeyStore(mockServiceApi);
        mqttClientKeyStore.init();
        CountDownLatch updateLatch = new CountDownLatch(1);
        mqttClientKeyStore.listenToCAUpdates(updateLatch::countDown);

        KeyStore keyStore = mqttClientKeyStore.getKeyStore();
        assertThat(keyStore.size(), is(0));

        mqttClientKeyStore.updateCA(Collections.singletonList(CERTIFICATE));
        assertThat(updateLatch.await(100, TimeUnit.MILLISECONDS), is(true));
        assertThat(keyStore.size(), is(1));

        X509Certificate caCert = (X509Certificate) keyStore.getCertificate("CA0");
        byte[] caCertBytes = encodeToBase64Pem(caCert.getEncoded(), BEGIN_CERT, END_CERT);
        String caCertPem = new String(caCertBytes, StandardCharsets.UTF_8);
        assertThat(caCertPem, is(CERTIFICATE));
    }

    @Test
    void GIVEN_MQTTClientKeyStore_WHEN_getSSLSocketFactory_THEN_returns_SSLSocketFactory() throws Exception {
        MQTTClientKeyStore mqttClientKeyStore = new MQTTClientKeyStore(mockServiceApi);
        mqttClientKeyStore.init();
        CountDownLatch updateLatch = new CountDownLatch(1);
        mqttClientKeyStore.listenToCAUpdates(updateLatch::countDown);

        ArgumentCaptor<GetCertificateRequest> cbArgumentCaptor = ArgumentCaptor.forClass(GetCertificateRequest.class);
        verify(mockServiceApi, times(1))
                .subscribeToCertificateUpdates(cbArgumentCaptor.capture());
        GetCertificateRequest certificateRequest = cbArgumentCaptor.getValue();

        KeyStore keyStore = mqttClientKeyStore.getKeyStore();
        assertThat(keyStore.size(), is(0));

        X509Certificate certificate = pemToX509Certificate(CERTIFICATE);
        CertificateUpdateEvent certificateUpdate =
                new CertificateUpdateEvent(keyPair, certificate, new X509Certificate[]{certificate});
        certificateRequest.getCertificateUpdateConsumer().accept(certificateUpdate);
        mqttClientKeyStore.updateCA(Collections.singletonList(CERTIFICATE));
        assertThat(updateLatch.await(100, TimeUnit.MILLISECONDS), is(true));
        assertThat(keyStore.size(), is(2));

        SocketFactory socketFactory = mqttClientKeyStore.getSSLSocketFactory();
        assertThat(socketFactory, is(instanceOf(SSLSocketFactory.class)));
    }

    private byte[] encodeToBase64Pem(byte[] content, byte[] header, byte[] footer) throws IOException {
        byte[] encodedBytes = Base64.getMimeEncoder(64, "\r\n".getBytes(StandardCharsets.UTF_8)).encode(content);
        try (ByteArrayOutputStream contentStream = new ByteArrayOutputStream()) {
            contentStream.write(header);
            contentStream.write(encodedBytes);
            contentStream.write(footer);
            encodedBytes = contentStream.toByteArray();
        }
        return encodedBytes;
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
}
