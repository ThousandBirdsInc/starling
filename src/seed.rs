//! Environment seed: the session metadata and cluster info that aren't derived
//! from the Starlingfile. Resources come from the engine instead.

use crate::api::v1alpha1::*;

fn meta(name: &str) -> ObjectMeta {
    ObjectMeta {
        name: name.to_string(),
        uid: uuid::Uuid::new_v4().to_string(),
        ..Default::default()
    }
}

pub fn env_seed(start_time: &str) -> (UISession, Vec<Cluster>) {
    let session = UISession {
        metadata: Some(meta("Starling")),
        spec: Some(UISessionSpec {}),
        status: Some(UISessionStatus {
            needs_analytics_nudge: Some(false),
            running_tilt_build: Some(TiltBuild {
                version: Some("0.1.0-starling".to_string()),
                date: Some("2026-05-22".to_string()),
                dev: Some(true),
                ..Default::default()
            }),
            suggested_tilt_version: Some("0.1.0-starling".to_string()),
            version_settings: Some(VersionSettings {
                check_updates: Some(true),
            }),
            tilt_start_time: Some(start_time.to_string()),
            tiltfile_key: Some("Starlingfile".to_string()),
            ..Default::default()
        }),
    };

    let clusters = vec![Cluster {
        metadata: Some(meta("default")),
        spec: Some(ClusterSpec {
            connection: Some(ClusterConnection {
                docker: Some(DockerClusterConnection {
                    host: Some("unix:///var/run/docker.sock".to_string()),
                }),
                kubernetes: None,
            }),
            default_registry: None,
        }),
        status: Some(ClusterStatus {
            arch: Some(std::env::consts::ARCH.to_string()),
            connected_at: Some(start_time.to_string()),
            ..Default::default()
        }),
    }];

    (session, clusters)
}
