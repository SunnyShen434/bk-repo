package com.tencent.bkrepo.analyst.dispatcher.dsl

import io.kubernetes.client.openapi.models.V1ObjectMeta
import io.kubernetes.client.openapi.models.V1Secret

/**
 * 创建Secret并配置
 */
fun v1Secret(configuration: V1Secret.() -> Unit): V1Secret {
    return V1Secret().apply(configuration)
}

/**
 * 配置Secret元数据
 */
fun V1Secret.metadata(configuration: V1ObjectMeta.() -> Unit) {
    metadata?.apply(configuration) ?: V1ObjectMeta().apply(configuration).also { metadata = it }
}
