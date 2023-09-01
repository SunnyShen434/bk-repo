package com.tencent.bkrepo.analyst.dispatcher

enum class KubernetesStringPool(val value: String) {
    /**
     * General
     */
    API_VERSION("v1"),

    /**
     * Job
     */
    JOB_RESTART_POLICY_NEVER("Never"),

    /**
     * Secret
     */
    SECRET_KIND("Secret"),
    DOCKER_SECRET_TYPE(".dockerconfigjson"),
    DOCKER_SECRET_YAML_TYPE("kubernetes.io/dockerconfigjson")
}
