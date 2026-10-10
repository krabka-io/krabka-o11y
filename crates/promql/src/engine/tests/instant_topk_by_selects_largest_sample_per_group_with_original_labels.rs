use super::*;

#[tokio::test]
pub(crate) async fn instant_topk_by_selects_largest_sample_per_group_with_original_labels() {
    let engine = job_memory_bytes_engine(&[
        JobInstanceSample {
            job: "api",
            instance: "a",
            value: 1.0,
        },
        JobInstanceSample {
            job: "api",
            instance: "b",
            value: 3.0,
        },
        JobInstanceSample {
            job: "worker",
            instance: "c",
            value: 5.0,
        },
        JobInstanceSample {
            job: "worker",
            instance: "d",
            value: 2.0,
        },
    ]);
    let samples = instant_vector(&engine, "topk by (job) (1, memory_bytes)", 10_000).await;
    check!(samples.len() == 2);
    check!(has_job_memory_bytes_sample(
        &samples,
        &JobInstanceSample {
            job: "api",
            instance: "b",
            value: 3.0,
        },
    ));
    check!(has_job_memory_bytes_sample(
        &samples,
        &JobInstanceSample {
            job: "worker",
            instance: "c",
            value: 5.0,
        },
    ));
}
