/*
 * Tencent is pleased to support the open source community by making BK-CI 蓝鲸持续集成平台 available.
 *
 * Copyright (C) 2022 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-CI 蓝鲸持续集成平台 is licensed under the MIT license.
 *
 * A copy of the MIT License is included in this file.
 *
 *
 * Terms of the MIT License:
 * ---------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of
 * the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.tencent.bkrepo.analyst.dispatcher

import com.tencent.bkrepo.analyst.dispatcher.dsl.metadata
import com.tencent.bkrepo.analyst.dispatcher.dsl.v1Secret
import com.tencent.bkrepo.common.analysis.pojo.scanner.Scanner
import com.tencent.bkrepo.common.analysis.pojo.scanner.standard.StandardScanner
import com.tencent.bkrepo.common.api.constant.CharPool
import com.tencent.bkrepo.common.api.constant.StringPool
import com.tencent.bkrepo.common.api.util.toJsonString
import io.kubernetes.client.openapi.apis.CoreV1Api
import io.kubernetes.client.openapi.models.V1Secret
import org.springframework.util.Base64Utils

object SecretUtils {
    fun createSecret(scanner: Scanner, namespace: String, coreV1Api: CoreV1Api): String? {
        require(scanner is StandardScanner)
        val username = scanner.username
        val password = scanner.password
        if (username.isNullOrBlank() || password.isNullOrBlank()) {
            return null
        }

        val privateRegistry = scanner.image.substringBefore(CharPool.SLASH, StringPool.EMPTY)
        val secretMetadataName = generateSecretName(scanner.image)
        val passed = checkSecret(
            secretMetadataName,
            namespace,
            username,
            password,
            privateRegistry,
            coreV1Api
        )
        return passed?.let {
            doCreateSecret(
                secretMetadataName,
                namespace,
                username,
                password,
                privateRegistry,
                coreV1Api,
                it
            )
        }
    }

    private fun doCreateSecret(
        metadataName: String,
        namespaceOfAnalyst: String,
        username: String,
        password: String,
        privateRegistry: String,
        coreV1Api: CoreV1Api,
        doCreate: Boolean = true
    ): String? {
        val secret = v1Secret {
            apiVersion = KubernetesStringPool.API_VERSION.value
            kind = KubernetesStringPool.SECRET_KIND.value
            metadata {
                name = metadataName
                namespace = namespaceOfAnalyst
            }
            data = createSecretDataOfPrivateRegistry(username, password, privateRegistry)
            type = KubernetesStringPool.DOCKER_SECRET_YAML_TYPE.value
        }
        val newSecret: V1Secret = if (doCreate) {
            coreV1Api.createNamespacedSecret(namespaceOfAnalyst, secret, null, null, null)
        } else {
            coreV1Api.replaceNamespacedSecret(metadataName, namespaceOfAnalyst, secret, null, null, null)
        }
        return newSecret.metadata?.name
    }

    private fun checkSecret(
        metadataName: String,
        namespace: String,
        username: String,
        password: String,
        privateRegistry: String,
        coreV1Api: CoreV1Api
    ): Boolean? {
        val secretList = coreV1Api.listNamespacedSecret(
            namespace,
            null,
            null,
            null,
            "metadata.name=$metadataName",
            null,
            null,
            null,
            null,
            null,
            null
        ).items
        if (secretList.size != 1) {
            return null
        }

        val secretData = secretList[0].data
        val localSecretData = createSecretDataOfPrivateRegistry(username, password, privateRegistry)
        if (secretData != localSecretData) {
            return false
        }
        return true
    }

    private fun generateSecretName(input: String): String {
        return input.replace(Regex("[^a-z\\d.-]+"), "-")
            .replace(Regex("^-|-$"), "")
            .replace(Regex("\\.{2,}"), ".")
            .take(253)
    }

    private fun createSecretDataOfPrivateRegistry(
        username: String,
        password: String,
        privateRegistry: String
    ): Map<String, ByteArray> {
        val auths = mapOf(
            privateRegistry to mapOf(
                "auth" to Base64Utils.encodeToString("$username:$password".toByteArray())
            )
        )
        val dockerCredentialsJson = mapOf("auths" to auths).toJsonString()
        return mapOf(KubernetesStringPool.DOCKER_SECRET_TYPE.value to dockerCredentialsJson.toByteArray())
    }
}
