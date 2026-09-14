pub(crate) fn is_default_otlp_resource_label(name: &str) -> bool {
    matches!(
        name,
        "service.name"
            | "service.namespace"
            | "service.instance.id"
            | "deployment.environment"
            | "deployment.environment.name"
            | "cloud.region"
            | "cloud.availability_zone"
            | "k8s.cluster.name"
            | "k8s.namespace.name"
            | "k8s.pod.name"
            | "k8s.container.name"
            | "container.name"
            | "k8s.replicaset.name"
            | "k8s.deployment.name"
            | "k8s.statefulset.name"
            | "k8s.daemonset.name"
            | "k8s.cronjob.name"
            | "k8s.job.name"
    )
}
