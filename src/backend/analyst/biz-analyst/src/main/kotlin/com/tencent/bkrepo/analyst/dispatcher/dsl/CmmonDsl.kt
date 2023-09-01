package com.tencent.bkrepo.analyst.dispatcher.dsl

import io.kubernetes.client.openapi.models.V1LocalObjectReference

/**
 * 创建V1LocalObjectReference并配置
 */
fun v1LocalObjectReference(configuration: V1LocalObjectReference.() -> Unit): V1LocalObjectReference {
    return V1LocalObjectReference().apply(configuration)
}
