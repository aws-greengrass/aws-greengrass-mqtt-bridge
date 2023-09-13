/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package com.aws.greengrass.mqtt.bridge.auth;

import com.aws.greengrass.clientdevices.auth.api.CertificateUpdateEvent;
import com.aws.greengrass.clientdevices.auth.api.ClientDevicesAuthServiceApi;
import com.aws.greengrass.clientdevices.auth.api.GetCertificateRequest;
import com.aws.greengrass.clientdevices.auth.certificate.CertificateHelper;
import com.aws.greengrass.clientdevices.auth.certificate.CertificateStore;
import com.aws.greengrass.testcommons.testutilities.GGExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith({MockitoExtension.class, GGExtension.class})
class MQTTClientKeyStoreTest {

    String caCertPem;
    X509Certificate caCert;
    KeyPair caKeyPair;

    X509Certificate clientCert;
    KeyPair clientKeyPair;

    @Mock
    ClientDevicesAuthServiceApi mockServiceApi;

    @BeforeEach
    void setUp() throws Exception {
        caKeyPair = CertificateStore.newRSAKeyPair(2048);
        caCert = CertificateHelper.createCACertificate(
                caKeyPair,
                Date.from(Instant.now()),
                Date.from(Instant.now().plusSeconds(100)),
                "CA"
        );
        caCertPem = CertificateHelper.toPem(caCert);

        clientKeyPair = CertificateStore.newRSAKeyPair(2048);
        clientCert = CertificateHelper.issueClientCertificate(
                caCert,
                caKeyPair.getPrivate(),
                CertificateHelper.getX500Name("client"),
                clientKeyPair.getPublic(),
                Date.from(Instant.now()),
                Date.from(Instant.now().plusSeconds(100)));
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

        CertificateUpdateEvent certificateUpdate =
                new CertificateUpdateEvent(caKeyPair, clientCert, new X509Certificate[]{caCert});
        getCertificateRequest.getCertificateUpdateConsumer().accept(certificateUpdate);
        assertThat(keyStore.size(), is(1));

        PrivateKey privateKey = (PrivateKey) keyStore.getKey(MQTTClientKeyStore.KEY_ALIAS, MQTTClientKeyStore.DEFAULT_KEYSTORE_PASSWORD);
        assertThat(privateKey.getAlgorithm(), is("RSA"));

        assertThat(keyStore.getCertificateChain(MQTTClientKeyStore.KEY_ALIAS).length, is(2));
        assertEquals(clientCert, keyStore.getCertificateChain(MQTTClientKeyStore.KEY_ALIAS)[0]);
        assertEquals(caCert, keyStore.getCertificateChain(MQTTClientKeyStore.KEY_ALIAS)[1]);
    }

    @Test
    void GIVEN_MQTTClientKeyStore_WHEN_called_updateCA_THEN_CA_stored() throws Exception {
        MQTTClientKeyStore mqttClientKeyStore = new MQTTClientKeyStore(mockServiceApi);
        mqttClientKeyStore.init();
        CountDownLatch updateLatch = new CountDownLatch(1);
        mqttClientKeyStore.listenToUpdates(updateLatch::countDown);

        KeyStore keyStore = mqttClientKeyStore.getKeyStore();
        assertThat(keyStore.size(), is(0));

        mqttClientKeyStore.updateCA(Collections.singletonList(caCertPem));
        assertThat(updateLatch.await(100, TimeUnit.MILLISECONDS), is(true));
        assertThat(keyStore.size(), is(1));

        assertEquals(caCert, keyStore.getCertificate("CA0"));
    }

    @Test
    void GIVEN_MQTTClientKeyStore_WHEN_getSSLSocketFactory_THEN_returns_SSLSocketFactory() throws Exception {
        MQTTClientKeyStore mqttClientKeyStore = new MQTTClientKeyStore(mockServiceApi);
        mqttClientKeyStore.init();
        CountDownLatch updateLatch = new CountDownLatch(1);
        mqttClientKeyStore.listenToUpdates(updateLatch::countDown);

        ArgumentCaptor<GetCertificateRequest> cbArgumentCaptor = ArgumentCaptor.forClass(GetCertificateRequest.class);
        verify(mockServiceApi, times(1))
                .subscribeToCertificateUpdates(cbArgumentCaptor.capture());
        GetCertificateRequest certificateRequest = cbArgumentCaptor.getValue();

        KeyStore keyStore = mqttClientKeyStore.getKeyStore();
        assertThat(keyStore.size(), is(0));

        CertificateUpdateEvent certificateUpdate =
                new CertificateUpdateEvent(caKeyPair, clientCert, new X509Certificate[]{caCert});
        certificateRequest.getCertificateUpdateConsumer().accept(certificateUpdate);
        mqttClientKeyStore.updateCA(Collections.singletonList(caCertPem));
        assertThat(updateLatch.await(100, TimeUnit.MILLISECONDS), is(true));
        assertThat(keyStore.size(), is(2));

        SocketFactory socketFactory = mqttClientKeyStore.getSSLSocketFactory();
        assertThat(socketFactory, is(instanceOf(SSLSocketFactory.class)));
    }
}
